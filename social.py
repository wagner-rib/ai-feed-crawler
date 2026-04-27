"""
Social media auto-poster.
Currently supports: X (Twitter)
"""

import json
import logging
import os
import re
import sqlite3
import time
from datetime import datetime, timezone

import requests
from requests_oauthlib import OAuth1

log = logging.getLogger(__name__)

SITE_URL         = os.environ.get("SITE_URL", "https://deeptrendlab.com")
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")

TWITTER_API_KEY            = os.environ.get("TWITTER_API_KEY", "")
TWITTER_API_SECRET         = os.environ.get("TWITTER_API_SECRET", "")
TWITTER_ACCESS_TOKEN       = os.environ.get("TWITTER_ACCESS_TOKEN", "")
TWITTER_ACCESS_TOKEN_SECRET = os.environ.get("TWITTER_ACCESS_TOKEN_SECRET", "")

DB_PATH = "/data/articles.db"


def _get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _twitter_auth() -> OAuth1:
    return OAuth1(
        TWITTER_API_KEY,
        TWITTER_API_SECRET,
        TWITTER_ACCESS_TOKEN,
        TWITTER_ACCESS_TOKEN_SECRET,
    )


def _generate_tweet(title: str, summary: str, url: str, tags: list[str]) -> str:
    """Use Claude to generate an engaging tweet under 260 chars (leaves room for URL)."""
    if not ANTHROPIC_API_KEY:
        # Fallback: simple truncated title + hashtags + url
        hashtags = " ".join(f"#{t.replace(' ', '')}" for t in tags[:3]) if tags else "#AI"
        text = f"{title[:200]}\n\n{hashtags}"
        return f"{text}\n\n{url}"

    hashtags_hint = " ".join(f"#{t.replace(' ', '')}" for t in tags[:3]) if tags else "#AI #MachineLearning"
    prompt = (
        f"Write a tweet for this AI article. Max 240 characters (excluding the URL). "
        f"Be direct, engaging, no fluff. End with 2-3 relevant hashtags.\n\n"
        f"Title: {title}\n"
        f"Summary: {(summary or '')[:300]}\n"
        f"Suggested hashtags: {hashtags_hint}\n\n"
        f"Return only the tweet text, no quotes, no URL (it gets appended automatically)."
    )

    try:
        resp = requests.post(
            "https://api.anthropic.com/v1/messages",
            headers={
                "x-api-key": ANTHROPIC_API_KEY,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            json={
                "model": "claude-haiku-4-5-20251001",
                "max_tokens": 100,
                "messages": [{"role": "user", "content": prompt}],
            },
            timeout=15,
        )
        resp.raise_for_status()
        tweet_text = resp.json()["content"][0]["text"].strip()
        # Ensure it fits: 240 chars + newline + URL (23 chars Twitter t.co)
        if len(tweet_text) > 240:
            tweet_text = tweet_text[:237] + "…"
        return f"{tweet_text}\n\n{url}"
    except Exception as exc:
        log.debug("Tweet generation failed: %s", exc)
        hashtags = " ".join(f"#{t.replace(' ', '')}" for t in tags[:3]) if tags else "#AI"
        return f"{title[:200]}\n\n{hashtags}\n\n{url}"


def _upload_image_twitter(image_url: str) -> str | None:
    """Download image and upload to Twitter media API. Returns media_id or None."""
    if not image_url:
        return None
    try:
        img_resp = requests.get(image_url, timeout=15, headers={"User-Agent": "Mozilla/5.0"})
        img_resp.raise_for_status()
        content_type = img_resp.headers.get("content-type", "image/jpeg").split(";")[0]

        upload_resp = requests.post(
            "https://upload.twitter.com/1.1/media/upload.json",
            auth=_twitter_auth(),
            files={"media": (
                "image.jpg",
                img_resp.content,
                content_type,
            )},
            timeout=30,
        )
        upload_resp.raise_for_status()
        media_id = upload_resp.json().get("media_id_string")
        log.info("Twitter image uploaded: media_id=%s", media_id)
        return media_id
    except Exception as exc:
        log.warning("Twitter image upload failed: %s", exc)
        return None


def post_to_twitter(article_row) -> bool:
    """Post a single article to X (Twitter). Returns True on success."""
    if not all([TWITTER_API_KEY, TWITTER_API_SECRET,
                TWITTER_ACCESS_TOKEN, TWITTER_ACCESS_TOKEN_SECRET]):
        log.warning("Twitter credentials not configured — skipping")
        return False

    title   = article_row["title"]
    summary = article_row["summary"] or ""
    slug    = article_row["slug"] or article_row["uid"]
    image_url = article_row["image_url"]
    url     = f"{SITE_URL}/article/{slug}"

    tags = []
    try:
        tags = json.loads(article_row["tags"] or "[]")
    except Exception:
        pass

    tweet_text = _generate_tweet(title, summary, url, tags)
    media_id   = _upload_image_twitter(image_url)

    payload = {"text": tweet_text}
    if media_id:
        payload["media"] = {"media_ids": [media_id]}

    try:
        resp = requests.post(
            "https://api.twitter.com/2/tweets",
            auth=_twitter_auth(),
            json=payload,
            timeout=15,
        )
        resp.raise_for_status()
        tweet_id = resp.json().get("data", {}).get("id", "?")
        log.info("Tweeted: %s | tweet_id=%s", title[:60], tweet_id)

        # Mark as posted in DB
        with _get_db() as conn:
            conn.execute(
                "UPDATE articles SET tweeted_at=? WHERE uid=?",
                (datetime.now(timezone.utc).isoformat(), article_row["uid"]),
            )
        return True

    except Exception as exc:
        log.error("Twitter post failed for '%s': %s", title[:50], exc)
        return False


def post_new_articles_to_twitter(limit: int = 5) -> int:
    """Find recently processed articles that haven't been tweeted yet and post them."""
    with _get_db() as conn:
        # Ensure tweeted_at column exists
        try:
            conn.execute("ALTER TABLE articles ADD COLUMN tweeted_at TEXT")
        except Exception:
            pass

        rows = conn.execute(
            """SELECT uid, title, summary, image_url, slug, tags
               FROM articles
               WHERE processed = 2
                 AND tweeted_at IS NULL
                 AND source_name != 'DeepTrendLab'
               ORDER BY published DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()

    if not rows:
        return 0

    posted = 0
    for row in rows:
        if post_to_twitter(row):
            posted += 1
        time.sleep(2)  # avoid rate limits

    return posted


def post_digest_to_twitter(slug: str) -> bool:
    """Tweet a specific digest/editorial post by slug."""
    with _get_db() as conn:
        try:
            conn.execute("ALTER TABLE articles ADD COLUMN tweeted_at TEXT")
        except Exception:
            pass
        row = conn.execute(
            "SELECT uid, title, summary, image_url, slug, tags FROM articles WHERE slug=?",
            (slug,),
        ).fetchone()

    if not row:
        log.warning("Article not found: %s", slug)
        return False
    return post_to_twitter(row)
