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

# Tags we allow in the stored HTML
ALLOWED_TAGS = [
    "p", "br", "hr",
    "h1", "h2", "h3", "h4", "h5", "h6",
    "ul", "ol", "li",
    "strong", "b", "em", "i", "u", "s", "mark",
    "blockquote", "q", "cite",
    "pre", "code", "kbd", "samp",
    "a", "img",
    "figure", "figcaption",
    "table", "thead", "tbody", "tfoot", "tr", "th", "td", "caption",
    "div", "section", "article", "aside",
    "span", "small", "sub", "sup",
    "dl", "dt", "dd",
]
ALLOWED_ATTRS = {
    "a":   ["href", "title", "rel", "target"],
    "img": ["src", "alt", "title", "width", "height", "loading"],
    "*":   ["class", "id"],
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


def fetch_and_clean(url: str) -> dict:
    """
    Fetch URL, extract main content with readability-lxml,
    return clean sanitized HTML + plain text + metadata.
    """
    result = {"content_html": "", "plain_text": "", "image": None, "author": "", "title": ""}

    try:
        resp = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT, allow_redirects=True)
        resp.raise_for_status()
        raw_html = resp.text
    except Exception as exc:
        log.debug("Fetch failed %s: %s", url, exc)
        return result

    orig_soup = BeautifulSoup(raw_html, "lxml")

    # OG image first — most reliable hero image
    for prop in ("og:image", "og:image:secure_url", "twitter:image"):
        meta = orig_soup.find("meta", property=prop) or orig_soup.find("meta", attrs={"name": prop})
        if meta and meta.get("content"):
            img_url = meta["content"]
            if img_url.startswith("//"):
                img_url = "https:" + img_url
            result["image"] = img_url
            break

    # Author
    for sel in ('meta[name="author"]', 'meta[property="article:author"]',
                '[rel="author"]', '.author-name', '.byline', '.author'):
        el = orig_soup.select_one(sel)
        if el:
            result["author"] = (el.get("content") or el.get_text(strip=True))[:120]
            break

    try:
        doc = Document(raw_html)
        content_html = doc.summary(html_partial=True)
        result["title"] = doc.title() or ""
    except Exception as exc:
        log.debug("Readability failed %s: %s", url, exc)
        return result

    soup = BeautifulSoup(content_html, "lxml")

    # Fix lazy images and relative URLs BEFORE bleach
    _fix_lazy_images(soup, url)
    _fix_urls(soup, url)

    # Sanitize — bleach now sees proper src= attributes
    clean = bleach.clean(
        str(soup),
        tags=ALLOWED_TAGS,
        attributes=ALLOWED_ATTRS,
        strip=True,
    )

    clean_soup = BeautifulSoup(clean, "lxml")

    # Drop broken/tracker images
    for img in clean_soup.find_all("img"):
        src = img.get("src", "")
        if (not src or src.startswith("data:") or
                any(x in src for x in ("pixel", "tracker", "beacon", "1x1", "blank.gif"))):
            img.decompose()
        else:
            img["loading"] = "lazy"
            img.attrs = {k: v for k, v in img.attrs.items()
                         if k in ("src", "alt", "title", "width", "height", "loading")}

    # First real image as fallback hero if OG not found
    if not result["image"]:
        first_img = clean_soup.find("img")
        if first_img and first_img.get("src"):
            result["image"] = first_img["src"]

    # Remove empty paragraphs
    for p in clean_soup.find_all("p"):
        if not p.get_text(strip=True) and not p.find("img"):
            p.decompose()

    result["content_html"] = str(clean_soup.body or clean_soup)
    result["plain_text"]   = _plain_text(clean_soup)

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
    data = fetch_and_clean(row["url"])
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


if __name__ == "__main__":
    from crawler import init_db
    init_db()
    init_db_v2()
    process_batch(limit=50)
