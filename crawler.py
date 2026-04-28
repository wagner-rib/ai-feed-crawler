"""Feed crawler: fetches RSS feeds and stores articles in SQLite."""

import sqlite3
import logging
import hashlib
import json
import os
import time
import re
from datetime import datetime, timezone
from pathlib import Path

import feedparser
import requests
from bs4 import BeautifulSoup
from dateutil import parser as dateparser
from slugify import slugify

from feeds_config import AI_FEEDS

DB_PATH = Path("/data/articles.db")
SITE_URL = os.environ.get("SITE_URL", "https://deeptrendlab.com").rstrip("/")
INDEXNOW_KEY = os.environ.get("INDEXNOW_KEY", "")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (compatible; AIFeedBot/1.0; +https://aifeed.local)"
    )
}
FETCH_TIMEOUT = 15

# URL path segments that indicate login-required or non-article pages
BLOCKED_PATH_PATTERNS = (
    "/academy/", "/courses/", "/course/",
    "/login", "/signin", "/signup", "/register",
    "/members/", "/subscriber/", "/subscribe",
    "/pricing", "/plans",
)


def _is_public_url(url: str) -> bool:
    """Return False for URLs that are known to require login or are non-articles."""
    from urllib.parse import urlparse
    path = urlparse(url).path.lower()
    return not any(pat in path for pat in BLOCKED_PATH_PATTERNS)


def make_slug(title: str, uid: str) -> str:
    base = slugify(title, max_length=70, word_boundary=True)
    return f"{base}-{uid[:6]}" if base else uid


def get_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    with get_db() as conn:
        conn.executescript("""
            CREATE TABLE IF NOT EXISTS articles (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                uid         TEXT    UNIQUE NOT NULL,
                slug        TEXT,
                title       TEXT    NOT NULL,
                url         TEXT    NOT NULL,
                summary     TEXT,
                image_url   TEXT,
                source_name TEXT    NOT NULL,
                source_url  TEXT    NOT NULL,
                category    TEXT    NOT NULL,
                logo        TEXT,
                published   TEXT,
                fetched_at  TEXT    NOT NULL,
                views       INTEGER DEFAULT 0
            );
            CREATE INDEX IF NOT EXISTS idx_category  ON articles(category);
            CREATE INDEX IF NOT EXISTS idx_published ON articles(published DESC);
            CREATE INDEX IF NOT EXISTS idx_source    ON articles(source_name);
        """)
        # Migration: add slug column to existing databases
        try:
            conn.execute("ALTER TABLE articles ADD COLUMN slug TEXT")
        except Exception:
            pass
        try:
            conn.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_slug ON articles(slug)")
        except Exception:
            pass
    log.info("Database initialised at %s", DB_PATH)


def backfill_slugs():
    """Generate slugs for articles that don't have one yet."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT uid, title FROM articles WHERE slug IS NULL OR slug = ''"
        ).fetchall()
        for row in rows:
            slug = make_slug(row["title"], row["uid"])
            try:
                conn.execute("UPDATE articles SET slug = ? WHERE uid = ?", (slug, row["uid"]))
            except Exception:
                conn.execute("UPDATE articles SET slug = ? WHERE uid = ?", (row["uid"], row["uid"]))
        if rows:
            conn.commit()
            log.info("Backfilled slugs for %d articles", len(rows))


def _uid(url: str) -> str:
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def _clean_html(raw: str) -> str:
    if not raw:
        return ""
    soup = BeautifulSoup(raw, "lxml")
    return soup.get_text(separator=" ", strip=True)


def _first_image(entry, feed_url: str) -> str | None:
    # media_thumbnail
    if hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
        return entry.media_thumbnail[0].get("url")
    # media_content
    if hasattr(entry, "media_content") and entry.media_content:
        for m in entry.media_content:
            if m.get("medium") == "image" or m.get("type", "").startswith("image"):
                return m.get("url")
    # enclosures
    if hasattr(entry, "enclosures") and entry.enclosures:
        for enc in entry.enclosures:
            if enc.get("type", "").startswith("image"):
                return enc.get("href") or enc.get("url")
    # og:image from summary HTML
    summary_html = getattr(entry, "summary", "") or ""
    if "<img" in summary_html:
        soup = BeautifulSoup(summary_html, "lxml")
        img = soup.find("img")
        if img and img.get("src"):
            return img["src"]
    return None


def _parse_date(entry) -> str:
    for attr in ("published_parsed", "updated_parsed"):
        val = getattr(entry, attr, None)
        if val:
            try:
                dt = datetime(*val[:6], tzinfo=timezone.utc)
                return dt.isoformat()
            except Exception:
                pass
    for attr in ("published", "updated"):
        val = getattr(entry, attr, None)
        if val:
            try:
                return dateparser.parse(val).isoformat()
            except Exception:
                pass
    return datetime.now(timezone.utc).isoformat()


def crawl_feed(feed_cfg: dict) -> int:
    url = feed_cfg["rss_url"]
    name = feed_cfg["name"]
    try:
        resp = requests.get(url, headers=HEADERS, timeout=FETCH_TIMEOUT)
        resp.raise_for_status()
        parsed = feedparser.parse(resp.content)
    except Exception as exc:
        # feedparser can also parse directly
        try:
            parsed = feedparser.parse(url)
        except Exception:
            log.warning("Failed to fetch %s (%s): %s", name, url, exc)
            return 0

    saved = 0
    with get_db() as conn:
        for entry in parsed.entries[:30]:  # max 30 per feed per run
            link = entry.get("link") or entry.get("id", "")
            if not link:
                continue
            if not _is_public_url(link):
                log.debug("Skipping non-public URL: %s", link)
                continue
            uid = _uid(link)
            title = _clean_html(entry.get("title", "")).strip()
            if not title:
                continue
            slug = make_slug(title, uid)
            summary_raw = (
                entry.get("summary")
                or entry.get("content", [{}])[0].get("value", "")
                or ""
            )
            summary = _clean_html(summary_raw)[:800].strip()
            image_url = _first_image(entry, url)
            published = _parse_date(entry)

            # Extract tags from RSS feed (lowercase, dedupe, max 8)
            rss_tags = []
            for t in entry.get("tags", []):
                term = (t.get("term") or "").strip().lower()
                # Skip generic/useless terms
                if term and len(term) > 1 and term not in {"uncategorized", "featured", "news", "tech"}:
                    rss_tags.append(term)
            rss_tags = list(dict.fromkeys(rss_tags))[:8]  # dedupe, keep order
            rss_tags_json = json.dumps(rss_tags) if rss_tags else None

            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO articles
                        (uid, slug, title, url, summary, image_url,
                         source_name, source_url, category, logo,
                         published, fetched_at, tags)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        uid, slug, title, link, summary, image_url,
                        feed_cfg["name"], feed_cfg["website_url"],
                        feed_cfg["category"], feed_cfg.get("logo", "📰"),
                        published, datetime.now(timezone.utc).isoformat(),
                        rss_tags_json,
                    ),
                )
                if conn.execute("SELECT changes()").fetchone()[0]:
                    saved += 1
                elif rss_tags_json:
                    # Article already exists — backfill RSS tags if it has none yet
                    conn.execute(
                        "UPDATE articles SET tags = ? WHERE uid = ? AND (tags IS NULL OR tags = '[]' OR tags = '')",
                        (rss_tags_json, uid),
                    )
            except Exception as exc:
                log.debug("Insert failed for %s: %s", link, exc)

    log.info("%-35s  +%d new articles", name, saved)
    return saved


def _ping_indexnow(slugs: list[str]) -> None:
    """Notify IndexNow (Bing/Yandex) about new URLs. Silently skips if key not set."""
    if not INDEXNOW_KEY or not slugs:
        return
    urls = [f"{SITE_URL}/article/{s}" for s in slugs]
    try:
        resp = requests.post(
            "https://api.indexnow.org/indexnow",
            json={"host": SITE_URL.replace("https://", "").replace("http://", ""),
                  "key": INDEXNOW_KEY,
                  "keyLocation": f"{SITE_URL}/{INDEXNOW_KEY}.txt",
                  "urlList": urls[:10000]},
            timeout=10,
        )
        log.info("IndexNow pinged %d URLs — status %d", len(urls), resp.status_code)
    except Exception as exc:
        log.debug("IndexNow ping failed: %s", exc)


def crawl_all():
    log.info("=== Starting full crawl of %d feeds ===", len(AI_FEEDS))
    total = 0
    new_slugs = []
    for feed in AI_FEEDS:
        n = crawl_feed(feed)
        total += n
        time.sleep(0.5)
    # Collect slugs of articles added in this crawl run to ping IndexNow
    if total > 0:
        try:
            with get_db() as conn:
                cutoff = datetime.now(timezone.utc).isoformat()[:16]  # last minute
                rows = conn.execute(
                    "SELECT slug FROM articles WHERE fetched_at >= ? AND slug IS NOT NULL",
                    (cutoff,),
                ).fetchall()
                new_slugs = [r["slug"] for r in rows if r["slug"]]
        except Exception:
            pass
        _ping_indexnow(new_slugs)
    # Always record when the crawler last ran, regardless of new articles found
    with get_db() as conn:
        conn.execute("""CREATE TABLE IF NOT EXISTS crawler_meta
            (key TEXT PRIMARY KEY, value TEXT)""")
        conn.execute("INSERT OR REPLACE INTO crawler_meta (key, value) VALUES ('last_crawl_at', ?)",
                     (datetime.now(timezone.utc).isoformat(),))
    log.info("=== Crawl complete — %d new articles ===", total)
    return total


def get_stats() -> dict:
    with get_db() as conn:
        total = conn.execute("SELECT COUNT(*) FROM articles").fetchone()[0]
        by_cat = conn.execute(
            "SELECT category, COUNT(*) as n FROM articles GROUP BY category ORDER BY n DESC"
        ).fetchall()
        # Use last crawl run time, not last article insert time
        try:
            last_fetch = conn.execute(
                "SELECT value FROM crawler_meta WHERE key='last_crawl_at'"
            ).fetchone()[0]
        except Exception:
            last_fetch = conn.execute(
                "SELECT MAX(fetched_at) FROM articles"
            ).fetchone()[0]
    return {
        "total": total,
        "by_category": {r["category"]: r["n"] for r in by_cat},
        "last_fetch": last_fetch,
    }


if __name__ == "__main__":
    init_db()
    crawl_all()
