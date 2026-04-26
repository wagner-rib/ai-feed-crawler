"""
Article processor:
  1. Fetches full article HTML from source
  2. Extracts clean readable content (readability-lxml)
  3. Sanitizes and fixes image/link URLs
  4. Calls Claude API only for tags + card excerpt
"""

import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse

import bleach
import requests
from bs4 import BeautifulSoup
from readability import Document

from crawler import DB_PATH, get_db

log = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}
FETCH_TIMEOUT = 20
JINA_BASE = "https://r.jina.ai/"

# Domains with soft paywalls or JS-rendered content — always use Jina directly
JINA_PREFERRED_DOMAINS = {
    "technologyreview.com",
    "wired.com",
    "wsj.com",
    "ft.com",
    "bloomberg.com",
    "economist.com",
    "nytimes.com",
    "theatlantic.com",
    "newyorker.com",
    "businessinsider.com",
    "blogs.nvidia.com",
    "zdnet.com",
    "cnet.com",
    "techcrunch.com",
    "arstechnica.com",
    "alignmentforum.org",
    "lesswrong.com",
    "simonwillison.net",
}

def _prefers_jina(url: str) -> bool:
    from urllib.parse import urlparse
    netloc = urlparse(url).netloc.lstrip("www.")
    return any(netloc == d or netloc.endswith("." + d) for d in JINA_PREFERRED_DOMAINS)

# Tags we allow in the stored HTML
ALLOWED_TAGS = [
    "p", "br", "hr",
    "h1", "h2", "h3", "h4", "h5", "h6",
    "ul", "ol", "li",
    "strong", "b", "em", "i", "u", "s", "mark",
    "blockquote", "q", "cite",
    "pre", "code", "kbd", "samp",
    "a", "img",
    "video", "source",
    "figure", "figcaption",
    "table", "thead", "tbody", "tfoot", "tr", "th", "td", "caption",
    "div", "section", "article", "aside",
    "span", "small", "sub", "sup",
    "dl", "dt", "dd",
]
ALLOWED_ATTRS = {
    "a":      ["href", "title", "rel", "target"],
    "img":    ["src", "alt", "title", "width", "height", "loading"],
    "video":  ["autoplay", "muted", "loop", "playsinline", "controls", "width", "height", "style"],
    "source": ["src", "type"],
    "*":      ["class", "id"],
}

SYSTEM_PROMPT = (
    "You are a concise AI assistant helping tag and excerpt news articles. "
    "Respond only with valid JSON — no markdown, no extra text."
)


# ---------------------------------------------------------------------------
# DB migration
# ---------------------------------------------------------------------------

def init_db_v2():
    with get_db() as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(articles)").fetchall()}
        migrations = [
            ("full_text",    "ALTER TABLE articles ADD COLUMN full_text    TEXT"),
            ("content_html", "ALTER TABLE articles ADD COLUMN content_html TEXT"),
            ("ai_digest",    "ALTER TABLE articles ADD COLUMN ai_digest    TEXT"),
            ("ai_takeaways", "ALTER TABLE articles ADD COLUMN ai_takeaways TEXT"),
            ("tags",         "ALTER TABLE articles ADD COLUMN tags         TEXT"),
            ("author",       "ALTER TABLE articles ADD COLUMN author       TEXT"),
            ("reading_time", "ALTER TABLE articles ADD COLUMN reading_time INTEGER DEFAULT 0"),
            ("processed",    "ALTER TABLE articles ADD COLUMN processed    INTEGER DEFAULT 0"),
        ]
        for col, sql in migrations:
            if col not in cols:
                conn.execute(sql)
                log.info("Added column: %s", col)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_processed ON articles(processed)")


# ---------------------------------------------------------------------------
# HTML extraction & cleaning
# ---------------------------------------------------------------------------

def _unwrap_pictures(soup: BeautifulSoup, base_url: str) -> None:
    """
    Convert <picture><source srcset="..."><img></picture> → plain <img src="...">
    BEFORE readability runs, so it doesn't silently drop them.
    Picks the highest-resolution source available.
    """
    for picture in soup.find_all("picture"):
        img = picture.find("img")
        if not img:
            picture.decompose()
            continue

        # Best src: try img src first, then source srcset
        best_src = img.get("src") or img.get("data-src") or ""

        # If img has no src, pull from <source> tags
        if not best_src or best_src.startswith("data:"):
            for source in picture.find_all("source"):
                srcset = source.get("srcset") or source.get("data-srcset") or ""
                if srcset:
                    # Take the last entry (usually highest res)
                    best_src = srcset.strip().split(",")[-1].strip().split()[0]
                    break

        if best_src and best_src.startswith("//"):
            best_src = "https:" + best_src
        if best_src and not best_src.startswith(("http", "data:")):
            best_src = urljoin(base_url, best_src)

        new_img = BeautifulSoup(
            f'<img src="{best_src}" alt="{img.get("alt","")}" loading="lazy">',
            "lxml",
        ).find("img")

        picture.replace_with(new_img)


def _fix_urls(soup: BeautifulSoup, base_url: str) -> None:
    """Make all relative src/href absolute."""
    for tag in soup.find_all(True):
        for attr in ("src", "href"):
            val = tag.get(attr, "")
            if val and not val.startswith(("http", "//", "data:", "#", "mailto:")):
                tag[attr] = urljoin(base_url, val)
        # Fix srcset
        if tag.get("srcset"):
            parts = []
            for part in tag["srcset"].split(","):
                part = part.strip()
                bits = part.split()
                if bits:
                    bits[0] = urljoin(base_url, bits[0])
                parts.append(" ".join(bits))
            tag["srcset"] = ", ".join(parts)


def _first_image(soup: BeautifulSoup) -> str | None:
    img = soup.find("img")
    return img["src"] if img and img.get("src") else None


def _plain_text(soup: BeautifulSoup) -> str:
    return soup.get_text(separator=" ", strip=True)


def _fix_lazy_images(soup: BeautifulSoup, base_url: str) -> None:
    """
    Resolve lazy-loaded images BEFORE bleach strips data-* attrs.
    Handles: data-src, data-lazy, data-original, data-srcset,
             noscript fallbacks, srcset-only images.
    """
    lazy_attrs = (
        "data-src", "data-lazy", "data-lazy-src",
        "data-original", "data-full", "data-url",
        "data-hi-res-src", "data-image-src",
    )

    for img in soup.find_all("img"):
        # 1. Promote data-src → src
        if not img.get("src") or img["src"].startswith("data:"):
            for attr in lazy_attrs:
                if img.get(attr):
                    img["src"] = img[attr]
                    break

        # 2. Use first entry of srcset if still no src
        if not img.get("src") and img.get("srcset"):
            first = img["srcset"].split(",")[0].strip().split()[0]
            if first:
                img["src"] = first

        # 3. Make src absolute
        src = img.get("src", "")
        if src and not src.startswith(("http", "//", "data:")):
            img["src"] = urljoin(base_url, src)
        elif src.startswith("//"):
            img["src"] = "https:" + src

    # 4. Rescue images hidden inside <noscript> (common lazy-load pattern)
    for noscript in soup.find_all("noscript"):
        inner = BeautifulSoup(noscript.decode_contents(), "lxml")
        for img in inner.find_all("img"):
            src = img.get("src", "")
            if src and not src.startswith("data:"):
                if not src.startswith(("http", "//")):
                    src = urljoin(base_url, src)
                new_img = soup.new_tag("img", src=src,
                                       alt=img.get("alt", ""),
                                       loading="lazy")
                noscript.replace_with(new_img)
                break


def _extract_article_images(orig_soup: BeautifulSoup, base_url: str) -> list[str]:
    """
    Extract in-article images directly from the original page HTML,
    searching common article content selectors.
    Returns list of absolute image URLs (deduped, no trackers).
    """
    # Common article body selectors — ordered by specificity
    selectors = [
        "article", "[itemprop='articleBody']",
        "[class*='article-body']", "[class*='article_body']",
        "[class*='post-content']", "[class*='post_content']",
        "[class*='entry-content']", "[class*='story-body']",
        "[class*='story__body']", "[class*='content-body']",
        "[class*='c-pageArticle']", "[class*='article__body']",
        "main", ".content", "#content",
    ]

    container = None
    for sel in selectors:
        try:
            container = orig_soup.select_one(sel)
            if container:
                break
        except Exception:
            continue

    search_area = container if container else orig_soup

    seen, result = set(), []
    # Look inside <picture>, <figure>, and plain <img> tags
    for el in search_area.find_all(["picture", "img", "figure"]):
        src = ""
        if el.name == "picture":
            img = el.find("img")
            src = img.get("src","") if img else ""
            if not src or src.startswith("data:"):
                for source in el.find_all("source"):
                    srcset = source.get("srcset","") or source.get("data-srcset","")
                    if srcset:
                        # highest res = last or widest descriptor
                        candidates = [p.strip().split()[0] for p in srcset.split(",") if p.strip()]
                        if candidates:
                            src = candidates[-1]
                            break
        elif el.name == "img":
            src = el.get("src","") or el.get("data-src","") or el.get("data-lazy-src","")
        elif el.name == "figure":
            img = el.find("img")
            if img:
                src = img.get("src","") or img.get("data-src","")

        if not src:
            continue
        if src.startswith("//"):
            src = "https:" + src
        if not src.startswith("http"):
            src = urljoin(base_url, src)
        if _is_bad_image(src):
            continue
        if src not in seen:
            seen.add(src)
            result.append(src)

    return result


def _extract_videos(soup: BeautifulSoup) -> list[str]:
    """
    Extract <video> elements from raw HTML and return clean embeddable HTML strings.
    Handles NVIDIA's data-sources JSON pattern and standard <source> children.
    """
    videos = []
    for video in soup.find_all("video"):
        sources = []

        # NVIDIA pattern: sources encoded in data-sources JSON attribute
        raw_ds = video.get("data-sources", "")
        if raw_ds:
            try:
                import html as _html
                ds = json.loads(_html.unescape(raw_ds))
                entries = ds.get("desktop") or ds.get("tablet") or ds.get("mobile") or []
                for s in entries:
                    if s.get("src"):
                        sources.append((s["src"], s.get("type", "video/mp4")))
            except Exception:
                pass

        # Standard <source> children
        for source in video.find_all("source"):
            src = source.get("src", "")
            if src and not any(src == s[0] for s in sources):
                sources.append((src, source.get("type", "video/mp4")))

        if not sources:
            continue

        src_tags = "".join(f'<source src="{s}" type="{t}">' for s, t in sources)
        videos.append(
            f'<figure>'
            f'<video autoplay muted loop playsinline controls style="width:100%;height:auto">'
            f'{src_tags}'
            f'</video>'
            f'</figure>'
        )
    return videos


def _inject_videos(content_html: str, videos: list[str]) -> str:
    """Distribute videos evenly through the article content after paragraphs."""
    if not videos or not content_html:
        return content_html
    soup = BeautifulSoup(content_html, "lxml")
    paras = (soup.find("body") or soup).find_all("p")
    if not paras:
        return content_html
    step = max(2, len(paras) // (len(videos) + 1))
    for i, vid_html in enumerate(videos):
        insert_after = paras[min((i + 1) * step - 1, len(paras) - 1)]
        vid_tag = BeautifulSoup(vid_html, "lxml").find("figure")
        if vid_tag:
            insert_after.insert_after(vid_tag)
    return str(soup.find("body") or soup)


_BAD_IMG_KEYWORDS = (
    "pixel", "tracker", "beacon", "1x1", "blank.gif", "logo",
    "avatar", "icon", "author", "profile", "headshot", "gravatar",
    "frame=1", "height=192", "height=100", "height=64", "height=48",
    "sidebar", "promo", "corp-blog", "related-post", "widget",
    # Tracking pixel domains / patterns
    "adsct", "doubleclick", "googletagmanager", "google-analytics",
    "facebook.com/tr", "bat.bing.com", "analytics.twitter",
)
_BAD_LINK_DOMAINS = (
    "trx-hub.com", "doubleclick.net", "googletagmanager.com",
    "google-analytics.com", "facebook.com/tr", "analytics",
    # Social share buttons
    "facebook.com/sharer", "twitter.com/intent/tweet",
    "linkedin.com/shareArticle", "reddit.com/submit",
    "mailto:?subject=", "pinterest.com/pin/create",
)

def _is_bad_image(src: str) -> bool:
    """Return True if the image should be filtered out."""
    if not src or src.startswith("data:"):
        return True
    s = src.lower()
    if any(kw in s for kw in _BAD_IMG_KEYWORDS):
        return True
    # WordPress sidebar/related-post thumbnails end in -WxH (sub-1000px)
    if re.search(r'-\d{2,3}x\d{2,3}\.(jpe?g|png|webp)$', s, re.IGNORECASE):
        return True
    # Square portrait images via URL params
    w = re.search(r'[?&]width=(\d+)', src)
    h = re.search(r'[?&]height=(\d+)', src)
    if w and h and w.group(1) == h.group(1) and int(w.group(1)) <= 300:
        return True
    return False


def _is_bad_link(href: str) -> bool:
    """Return True for tracking/analytics links that should be stripped."""
    h = href.lower()
    return any(d in h for d in _BAD_LINK_DOMAINS)


def _md_inline(text: str) -> str:
    text = re.sub(r'\*\*(.+?)\*\*', r'<strong>\1</strong>', text)
    text = re.sub(r'\*(.+?)\*', r'<em>\1</em>', text)
    # Strip empty-text links [](url) — always share buttons or decorative anchors
    text = re.sub(r'\[\]\([^)]+\)', '', text)
    def _link_sub(m):
        href = m.group(2)
        return '' if _is_bad_link(href) else f'<a href="{href}">{m.group(1)}</a>'
    text = re.sub(r'\[([^\]]+)\]\(([^)]+)\)', _link_sub, text)
    return text


def _md_to_html(md: str, base_url: str) -> str:
    """Convert Jina markdown output to basic sanitized HTML."""
    parts = []
    for block in re.split(r'\n{2,}', md):
        block = block.strip()
        if not block:
            continue
        m = re.match(r'^(#{1,6})\s+(.*)', block)
        if m:
            lvl = min(len(m.group(1)) + 1, 6)
            parts.append(f"<h{lvl}>{_md_inline(m.group(2))}</h{lvl}>")
            continue
        # Standalone image: ![alt](src)
        m = re.match(r'^!\[([^\]]*)\]\(([^)]+)\)$', block)
        if m:
            src = m.group(2) if m.group(2).startswith("http") else urljoin(base_url, m.group(2))
            if not _is_bad_image(src):
                parts.append(f'<figure><img src="{src}" alt="{m.group(1)}" loading="lazy"></figure>')
            continue
        # Linked image: [![alt](src)](href) — always decorative/share icons, skip
        if re.match(r'^!\[', block) and re.search(r'\]\([^)]+\)\]\([^)]+\)$', block):
            continue
        # Link-wrapped image: [[![alt](src) ...](href)] — ZDNet review cards etc.
        # Skip entirely — the image duplicates the hero already shown at top of page
        if re.match(r'^\[!\[', block):
            continue
        if re.match(r'^[-*]\s', block):
            lis = "".join(
                f"<li>{_md_inline(i.lstrip('-* '))}</li>"
                for i in re.split(r'\n[-*]\s', block)
            )
            parts.append(f"<ul>{lis}</ul>")
            continue
        parts.append(f"<p>{_md_inline(block.replace(chr(10), ' '))}</p>")
    return "\n".join(parts)


def _strip_jina_boilerplate(md: str, article_title: str = "") -> str:
    """
    Remove nav/header/footer noise from Jina's full-page markdown output.

    Order matters:
      1. Strip PREAMBLE first (nav, cookie banners before the article)
      2. Strip TAIL from the now-clean article slice (sidebar, related, footer)
      3. Inline cleanup (countdown timers, social links, noise phrases)
    """
    # ── STEP 1: Find article start — strip leading navigation / cookie banners ──

    # a) Accessibility skip-to-content anchors (most reliable)
    _SKIP_ANCHORS = [
        "[Skip to main content]", "[Skip to content]",
        "Skip to main content", "Skip to content",
        "[Skip Navigation]",
    ]
    found_skip = False
    for anchor in _SKIP_ANCHORS:
        if anchor in md:
            after_nav = md[md.index(anchor) + len(anchor):]
            match = re.search(r'\n#{1,2} [^\n]+\n', after_nav)
            if match:
                md = after_nav[match.start():].strip()
            found_skip = True
            break

    if not found_skip:
        # b) "## Headline | SiteName" or "## Headline - SiteName" heading —
        #    Jina captured the browser <title>. Find where it repeats without suffix.
        m = re.match(r'^#{1,2} (.+?) (?:[|—]| - )\S[^\n]*\n', md)
        if m:
            title_part = m.group(1).strip()
            clean = re.search(r'\n#{1,2} ' + re.escape(title_part) + r'\s*\n', md)
            if clean:
                md = md[clean.start():].strip()

    # c) Article title fast-forward (when title isn't near the top)
    if article_title:
        title_short = re.sub(r'\s+', ' ', article_title.strip())[:60]
        if title_short.lower() not in md[:300].lower():
            match = re.search(
                r'\n#{1,3} (?:' + re.escape(title_short) + r')[^\n]*\n',
                md, re.IGNORECASE,
            )
            if match:
                md = md[match.start():].strip()

    # ── STEP 2: Strip TAIL — sidebar / related / footer appended after article ──
    _TAIL_MARKERS = [
        "\n## Author\n", "\n## Footer\n", "\n## References\n", "\n---\n",
        # Article metadata appended after the last paragraph
        "\nTopics\n",            # TechCrunch post-article tags
        "\n*   Categories:",     # WordPress/NVIDIA list-style categories
        "\nCategories:\n",
        # Related/recommended article widgets
        "\n## Related\n", "\n## Related Articles\n", "\n## Related Posts\n",
        "\n### Related News", "\nRelated News\n",
        "\nLatest in ", "\n### Latest in ", "\n## Latest in ",
        "\nMore from ", "\n### More from ", "\n## More from ",
        "\nRelated Articles", "\nRelated Posts", "\nYou might also like",
        "\nRecommended for you", "\nRecommended Reading",
        # Social / share sections
        "\nFollow Us\n", "\nShare This\n",
        # Newsletter / subscribe CTAs
        "\n### Newsletters\n", "\n## Newsletters\n",
        "\nNewsletter sign", "\nSign up for our", "\nGet the latest",
        "\nBy submitting your email",
        "\n- [x] ",   # checkbox lists in newsletter / cookie-consent forms
        # Comments section
        "\n## Comments\n", "\nLeave a comment", "\nAdd a comment",
        "Loading comments", "[Forum view]",
        "\nJoin the discussion", "\nPost a comment",
        # Ars Technica comments section headers
        "\nStaff Picks\n",           # header above top-picked reader comments
        "\nReader Comments\n",       # alternate header
        "/civis/posts/",             # civis comment thread links
        "/civis/members/",           # civis user profile links (commenter names)
        # Privacy / legal footer
        "\nDo Not Sell", "\nPrivacy Policy\n", "\nCookie Policy",
        "\nTerms of Service", "\nAll rights reserved",
        "\nCopyright ©", "\nCopyright (c)",
        # Cookie consent banners
        "\nNVIDIA uses cookies", "\nWe use cookies",
        "\nThis site uses cookies", "\nBy continuing to use this site",
        # Accessibility / layout notices
        "\nSome areas of this page may shift",
        # Subscription / login UI (Ars Technica and similar)
        "\nCustomize\n", "\nSign in dialog",
        # Article navigation links (Ars Technica "Prev/Next story")
        "Prev story", "Next story",
        # Popular / trending article lists (Ars Technica "Most Read" widget)
        "Listing image for first story in Most Read",
        "Listing image for",
        # Simon Willison sidebar / footer
        "\n## Recent articles\n",       # sidebar with links to other posts
        "\nThis is a link post by Simon",
        "\n### Monthly briefing\n",
        "\nSponsor me for $",
        "\nPay me to send you less",
    ]
    for marker in _TAIL_MARKERS:
        idx = md.find(marker)
        if idx != -1:
            # Snap to the nearest paragraph boundary (double newline) before the marker,
            # so we never cut mid-block and leave orphaned partial markup.
            boundary = md.rfind('\n\n', 0, idx)
            md = md[:boundary] if boundary > 0 else md[:idx]

    # ── STEP 3: Inline noise cleanup ──

    # Countdown timer lines (e.g. "05:02:59:25")
    md = re.sub(r'(?m)^\d{2}:\d{2}(:\d{2}){1,2}\s*$\n?', '', md)

    # Empty / icon-only markdown link rows (social share buttons)
    # Handles: single [](url), multiple chained [](url)[](url), linked images [![](src)](href)
    md = re.sub(r'\[\]\([^)]+\)', '', md)   # strip all empty links inline
    md = re.sub(
        r'(?m)^\s*(\[!\[.*?\]\(.*?\)\]\([^)]+\)\s*)+$\n?',
        '', md,
    )

    # Standalone UI noise phrases on their own line
    _NOISE = {'In Brief', 'Close', 'Posted:', 'Share this:', 'Share', 'Comments',
              'Subscribe', 'Newsletter', 'Sign up', 'Log in', 'Sign in'}
    md = '\n'.join(l for l in md.split('\n') if l.strip() not in _NOISE)

    return md.strip()


def _jina_fetch(url: str, article_title: str = "") -> dict | None:
    """
    Fallback fetch via Jina Reader for blocked/JS-rendered pages.
    Returns dict with content_html and plain_text, or None on failure.
    """
    try:
        resp = requests.get(
            JINA_BASE + url,
            headers={
                **HEADERS,
                "Accept": "application/json",
                # Ask Jina to remove nav/chrome before extracting
                "X-Remove-Selector": (
                    "nav, header, footer, aside, "
                    "[role=navigation], [role=banner], [role=contentinfo], "
                    ".nav, .header, .footer, .sidebar, .menu, .cookie-banner"
                ),
            },
            timeout=FETCH_TIMEOUT * 2,
        )
        if resp.status_code != 200:
            log.debug("Jina returned %d for %s", resp.status_code, url)
            return None
        data = resp.json().get("data", {})
        md = data.get("content", "")
        if not md or len(md.split()) < 30:
            return None
        md = _strip_jina_boilerplate(md, article_title=article_title)
        if len(md.split()) < 30:
            return None
        log.info("Jina Reader succeeded (%d words): %s", len(md.split()), url)
        content_html = _md_to_html(md, url)
        plain_text = re.sub(r"<[^>]+>", " ", content_html).strip()
        return {
            "content_html": content_html,
            "plain_text": plain_text,
            "title": data.get("title", ""),
            "image": data.get("images", [None])[0] if data.get("images") else None,
        }
    except Exception as exc:
        log.debug("Jina fetch failed %s: %s", url, exc)
        return None


def fetch_and_clean(url: str, article_title: str = "") -> dict:
    """
    Fetch + clean article. Strategy:
      1. readability-lxml  →  clean article text
      2. direct selector extraction  →  in-article images
      3. merge images back into the text content
    """
    result = {"content_html": "", "plain_text": "", "image": None, "author": "", "title": ""}

    # Soft-paywall / JS-rendered domains: try Jina first, fall back to readability
    if _prefers_jina(url):
        log.info("Preferred-Jina domain, using Jina Reader directly: %s", url)
        jina = _jina_fetch(url, article_title=article_title)
        if jina:
            result.update({k: v for k, v in jina.items() if v})
            # Secondary fetch: OG image (Jina often misses it) + videos
            try:
                raw = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT).text
                raw_soup = BeautifulSoup(raw, "lxml")

                # OG/Twitter meta image — most reliable hero, grab if Jina didn't provide one
                if not result.get("image"):
                    for prop in ("og:image", "og:image:secure_url", "twitter:image"):
                        meta = (raw_soup.find("meta", property=prop) or
                                raw_soup.find("meta", attrs={"name": prop}))
                        if meta and meta.get("content"):
                            img = meta["content"]
                            img = "https:" + img if img.startswith("//") else img
                            if not _is_bad_image(img):
                                result["image"] = img
                                log.info("OG image from raw HTML: %s", img[:80])
                                break

                videos = _extract_videos(raw_soup)
                if videos and result.get("content_html"):
                    result["content_html"] = _inject_videos(result["content_html"], videos)
                    log.info("Injected %d video(s) from raw HTML: %s", len(videos), url)
            except Exception as exc:
                log.debug("Secondary fetch failed %s: %s", url, exc)
            return result
        log.info("Jina failed for preferred domain, falling back to readability: %s", url)

    use_jina = False
    try:
        resp = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT, allow_redirects=True)
        if resp.status_code in (401, 403, 429):
            log.info("Blocked (%d), trying Jina Reader: %s", resp.status_code, url)
            use_jina = True
        else:
            resp.raise_for_status()
            raw_html = resp.text
    except Exception as exc:
        log.debug("Fetch failed %s: %s", url, exc)
        return result

    if use_jina:
        jina = _jina_fetch(url, article_title=article_title)
        if not jina:
            return result
        result.update({k: v for k, v in jina.items() if v})
        return result

    orig_soup = BeautifulSoup(raw_html, "lxml")

    # OG / Twitter meta image — most reliable hero
    for prop in ("og:image", "og:image:secure_url", "twitter:image"):
        meta = orig_soup.find("meta", property=prop) or orig_soup.find("meta", attrs={"name": prop})
        if meta and meta.get("content"):
            img_url = meta["content"]
            result["image"] = "https:" + img_url if img_url.startswith("//") else img_url
            break

    # Author
    for sel in ('meta[name="author"]', 'meta[property="article:author"]',
                '[rel="author"]', '.author-name', '.byline', '.author'):
        el = orig_soup.select_one(sel)
        if el:
            result["author"] = (el.get("content") or el.get_text(strip=True))[:120]
            break

    # Extract in-article images from original HTML (before readability touches it)
    article_images = _extract_article_images(orig_soup, url)
    log.debug("Direct image extraction: %d images from %s", len(article_images), url)

    # Pre-process for readability: unwrap <picture>, fix lazy-loads
    pre_soup = BeautifulSoup(raw_html, "lxml")
    _unwrap_pictures(pre_soup, url)
    _fix_lazy_images(pre_soup, url)

    try:
        doc = Document(str(pre_soup))
        content_html = doc.summary(html_partial=True)
        result["title"] = doc.title() or ""
    except Exception as exc:
        log.debug("Readability failed %s: %s", url, exc)
        return result

    soup = BeautifulSoup(content_html, "lxml")
    _fix_urls(soup, url)

    # Sanitize
    clean = bleach.clean(str(soup), tags=ALLOWED_TAGS, attributes=ALLOWED_ATTRS, strip=True)
    clean_soup = BeautifulSoup(clean, "lxml")

    # Count images readability kept
    existing_srcs = {img.get("src","") for img in clean_soup.find_all("img") if img.get("src")}

    # Inject missing article images as figures after every ~3 paragraphs
    missing_imgs = [s for s in article_images if s not in existing_srcs]
    if missing_imgs:
        body = clean_soup.find("body") or clean_soup
        paras = body.find_all("p")
        step = max(3, len(paras) // (len(missing_imgs) + 1))
        for i, img_src in enumerate(missing_imgs):
            insert_after = paras[min((i + 1) * step - 1, len(paras) - 1)] if paras else None
            fig_html = (f'<figure><img src="{img_src}" alt="" loading="lazy">'
                        f'</figure>')
            fig = BeautifulSoup(fig_html, "lxml").find("figure")
            if insert_after:
                insert_after.insert_after(fig)
            else:
                body.append(fig)

    # Clean up images: remove trackers, avatars, sidebar promos, etc.
    for img in clean_soup.find_all("img"):
        if _is_bad_image(img.get("src", "")):
            img.decompose()
        else:
            img["loading"] = "lazy"
            img.attrs = {k: v for k, v in img.attrs.items()
                         if k in ("src", "alt", "title", "width", "height", "loading")}

    # Fallback hero: first in-article image
    if not result["image"]:
        first = clean_soup.find("img")
        if first and first.get("src"):
            result["image"] = first["src"]
    if not result["image"] and article_images:
        result["image"] = article_images[0]

    # Remove empty paragraphs
    for p in clean_soup.find_all("p"):
        if not p.get_text(strip=True) and not p.find("img"):
            p.decompose()

    # Inject videos from original HTML (bleach strips them, so add after sanitization)
    videos = _extract_videos(orig_soup)
    if videos:
        result["content_html"] = _inject_videos(str(clean_soup.body or clean_soup), videos)
        log.info("Injected %d video(s): %s", len(videos), url)
    else:
        result["content_html"] = str(clean_soup.body or clean_soup)
    result["plain_text"]   = _plain_text(clean_soup)

    # If readability got thin content (paywall preview / JS SPA), try Jina Reader
    if len(result["plain_text"].split()) < 300:
        log.info("Thin content (%d words), trying Jina: %s",
                 len(result["plain_text"].split()), url)
        jina = _jina_fetch(url, article_title=article_title)
        if jina and len(jina["plain_text"].split()) > len(result["plain_text"].split()):
            result.update({k: v for k, v in jina.items() if v})

    log.debug("Result: %d words, %d imgs total",
              len(result["plain_text"].split()),
              len(clean_soup.find_all("img")))
    return result


# ---------------------------------------------------------------------------
# Claude — tags + excerpt only
# ---------------------------------------------------------------------------

def _call_claude_tags(title: str, text: str, source_name: str) -> dict | None:
    if not ANTHROPIC_API_KEY:
        return None

    snippet = text[:2000] if text else title
    prompt = f"""Article from {source_name}: "{title}"

First 2000 chars:
{snippet}

Return JSON only:
{{
  "excerpt": "<2 punchy sentences summarising the article for a news card, max 40 words>",
  "tags": ["<tag1>", "<tag2>", "<tag3>", "<tag4>", "<tag5>"]
}}"""

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "anthropic-beta": "prompt-caching-2024-07-31",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 200,
                "system": [{"type": "text", "text": SYSTEM_PROMPT,
                             "cache_control": {"type": "ephemeral"}}],
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=20,
        )
        resp.raise_for_status()
        raw = resp.json()["content"][0]["text"].strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as exc:
        log.debug("Claude tags error for '%s': %s", title[:50], exc)
        return None


# ---------------------------------------------------------------------------
# Process pipeline
# ---------------------------------------------------------------------------

def _reading_time(text: str) -> int:
    return max(1, round(len((text or "").split()) / 200))


def process_article(uid: str) -> bool:
    with get_db() as conn:
        row = conn.execute(
            "SELECT uid, title, url, summary, source_name, category, image_url "
            "FROM articles WHERE uid = ?", (uid,)
        ).fetchone()
    if not row:
        return False

    # 1. Fetch + clean full article
    data = fetch_and_clean(row["url"], article_title=row["title"])
    content_html = data["content_html"]
    plain_text   = data["plain_text"]
    author       = data["author"] or ""
    image        = data["image"] or row["image_url"]

    # 2. AI tags + excerpt (cheap — only 200 tokens output)
    ai = None
    if plain_text or row["summary"]:
        ai = _call_claude_tags(
            row["title"],
            plain_text or row["summary"] or "",
            row["source_name"],
        )

    excerpt = (ai or {}).get("excerpt") or row["summary"] or ""
    tags    = json.dumps((ai or {}).get("tags") or [])

    with get_db() as conn:
        conn.execute(
            """UPDATE articles SET
               full_text    = ?,
               content_html = ?,
               ai_digest    = ?,
               tags         = ?,
               author       = COALESCE(NULLIF(?, ''), author),
               reading_time = ?,
               image_url    = COALESCE(NULLIF(?, ''), image_url),
               processed    = 2
             WHERE uid = ?""",
            (
                plain_text or None,
                content_html or None,
                excerpt or None,
                tags,
                author,
                _reading_time(plain_text),
                image or None,
                uid,
            ),
        )

    log.info("Processed %-16s  %4d words  html=%s  ai=%s",
             uid,
             len((plain_text or "").split()),
             "✓" if content_html else "✗",
             "✓" if ai else "✗")
    return True


def process_batch(limit: int = 20, delay: float = 1.2) -> int:
    with get_db() as conn:
        rows = conn.execute(
            "SELECT uid FROM articles WHERE processed = 0 "
            "ORDER BY published DESC LIMIT ?", (limit,)
        ).fetchall()

    uids = [r["uid"] for r in rows]
    if not uids:
        log.info("No unprocessed articles.")
        return 0

    log.info("Processing batch of %d articles...", len(uids))
    done = 0
    for uid in uids:
        try:
            if process_article(uid):
                done += 1
        except Exception as exc:
            log.error("Failed %s: %s", uid, exc)
            with get_db() as conn:
                conn.execute("UPDATE articles SET processed = -1 WHERE uid = ?", (uid,))
        time.sleep(delay)

    log.info("Batch complete — %d/%d processed.", done, len(uids))
    return done


def reprocess_empty(limit: int = 50) -> int:
    """Reset articles that were marked processed but have no content, so they get re-fetched."""
    with get_db() as conn:
        result = conn.execute(
            """UPDATE articles SET processed = 0
               WHERE processed = 2 AND content_html IS NULL
               AND id IN (SELECT id FROM articles WHERE processed = 2
                          AND content_html IS NULL ORDER BY published DESC LIMIT ?)""",
            (limit,),
        )
        count = result.rowcount
    log.info("Reset %d empty articles for reprocessing.", count)
    return count


if __name__ == "__main__":
    import sys
    from crawler import init_db
    init_db()
    init_db_v2()
    if "--reprocess-empty" in sys.argv:
        reprocess_empty(limit=200)
    process_batch(limit=50)
