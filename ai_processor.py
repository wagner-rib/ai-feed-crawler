"""
AI processor: extracts full article text with trafilatura,
then generates unique digests via Claude API.
"""

import json
import logging
import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
import trafilatura
from bs4 import BeautifulSoup

from crawler import DB_PATH, get_db

log = logging.getLogger(__name__)

ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; AIFeedBot/2.0; +https://aifeed.ai)"
}

# System prompt — cached by Claude API to reduce cost
SYSTEM_PROMPT = """You are an expert AI journalist writing for AIFeed, the premier AI news aggregator.
Your summaries are sharp, technical yet accessible, and genuinely useful to AI practitioners,
researchers, and enthusiasts. You write in clear, engaging prose — no fluff, no filler."""


def init_db_v2():
    """Add new columns for full content and AI processing."""
    with get_db() as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(articles)").fetchall()}
        migrations = [
            ("full_text",    "ALTER TABLE articles ADD COLUMN full_text    TEXT"),
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
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_processed ON articles(processed)"
        )


# ---------------------------------------------------------------------------
# Text & metadata extraction
# ---------------------------------------------------------------------------

def extract_og_image(url: str) -> str | None:
    """Fast OG image extraction without full download."""
    try:
        resp = requests.get(url, headers=HEADERS, timeout=10, stream=True)
        # Read only first 50KB to find meta tags
        chunk = b""
        for c in resp.iter_content(chunk_size=8192):
            chunk += c
            if len(chunk) > 51200:
                break
        soup = BeautifulSoup(chunk, "lxml")
        for tag in ("og:image", "twitter:image", "og:image:secure_url"):
            meta = soup.find("meta", property=tag) or soup.find("meta", attrs={"name": tag})
            if meta and meta.get("content"):
                return meta["content"]
    except Exception:
        pass
    return None


def extract_article(url: str) -> dict:
    """Extract full text, author, and image from an article URL."""
    result = {"text": "", "author": "", "image": None}
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return result
        text = trafilatura.extract(
            downloaded,
            include_comments=False,
            include_tables=False,
            no_fallback=False,
            favor_precision=False,
        )
        metadata = trafilatura.extract_metadata(downloaded)
        result["text"] = (text or "").strip()
        if metadata:
            result["author"] = metadata.author or ""
            result["image"] = metadata.image or None
    except Exception as exc:
        log.debug("Extraction failed for %s: %s", url, exc)
    return result


# ---------------------------------------------------------------------------
# Claude API — generate unique digest
# ---------------------------------------------------------------------------

def _call_claude(title: str, text: str, source_name: str, category: str) -> dict | None:
    if not ANTHROPIC_API_KEY:
        return None

    snippet = text[:4000] if text else "(no content extracted)"

    prompt = f"""Article from **{source_name}** (category: {category})

TITLE: {title}

CONTENT:
{snippet}

---
Write the following in valid JSON (and nothing else):
{{
  "digest": "<400-500 word original digest. Strong opening. Explain key concepts clearly. Why it matters for AI practitioners. No bullet points in the digest — flowing prose only. Attribute the source naturally within the text.>",
  "takeaways": ["<takeaway 1, max 20 words>", "<takeaway 2>", "<takeaway 3>", "<takeaway 4>", "<takeaway 5>"],
  "tags": ["<tag1>", "<tag2>", "<tag3>", "<tag4>", "<tag5>", "<tag6>"]
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
                "max_tokens": 1200,
                "system": [
                    {
                        "type": "text",
                        "text": SYSTEM_PROMPT,
                        "cache_control": {"type": "ephemeral"},
                    }
                ],
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=30,
        )
        resp.raise_for_status()
        raw = resp.json()["content"][0]["text"].strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        return json.loads(raw)
    except Exception as exc:
        log.warning("Claude API error for '%s': %s", title[:60], exc)
        return None


# ---------------------------------------------------------------------------
# Process pipeline
# ---------------------------------------------------------------------------

def _reading_time(text: str) -> int:
    words = len((text or "").split())
    return max(1, round(words / 200))


def process_article(uid: str) -> bool:
    """Extract + AI-process a single article. Returns True on success."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT uid, title, url, summary, source_name, category, image_url "
            "FROM articles WHERE uid = ?", (uid,)
        ).fetchone()
    if not row:
        return False

    # 1. Extract full text
    extracted = extract_article(row["url"])
    full_text = extracted["text"]
    author = extracted["author"]
    image = extracted["image"] or row["image_url"]

    # Try OG image if trafilatura didn't find one
    if not image:
        image = extract_og_image(row["url"])

    reading_time = _reading_time(full_text)

    # 2. Generate AI digest
    ai_data = None
    if full_text or row["summary"]:
        content_for_ai = full_text or row["summary"] or ""
        ai_data = _call_claude(
            row["title"], content_for_ai,
            row["source_name"], row["category"]
        )

    # 3. Persist
    with get_db() as conn:
        conn.execute(
            """UPDATE articles SET
               full_text    = ?,
               ai_digest    = ?,
               ai_takeaways = ?,
               tags         = ?,
               author       = ?,
               reading_time = ?,
               image_url    = COALESCE(?, image_url),
               processed    = 2
             WHERE uid = ?""",
            (
                full_text or None,
                ai_data["digest"] if ai_data else None,
                json.dumps(ai_data["takeaways"]) if ai_data else None,
                json.dumps(ai_data["tags"]) if ai_data else None,
                author or None,
                reading_time,
                image or None,
                uid,
            ),
        )

    log.info("Processed %-16s  text=%4d words  ai=%s",
             uid, len((full_text or "").split()), "✓" if ai_data else "✗")
    return True


def process_batch(limit: int = 20, delay: float = 1.5) -> int:
    """Process up to `limit` unprocessed articles. Returns count processed."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT uid FROM articles WHERE processed = 0 "
            "ORDER BY published DESC LIMIT ?", (limit,)
        ).fetchall()

    uids = [r["uid"] for r in rows]
    if not uids:
        log.info("No unprocessed articles in queue.")
        return 0

    log.info("Processing batch of %d articles...", len(uids))
    done = 0
    for uid in uids:
        try:
            if process_article(uid):
                done += 1
        except Exception as exc:
            log.error("Failed to process %s: %s", uid, exc)
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
