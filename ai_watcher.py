#!/usr/bin/env python3
"""
AI watcher — runs on the host alongside the Docker container.
Watches for new articles and processes them with the claude CLI.
Handles: tags, editorial analysis, and daily/weekly/monthly digests.

Usage:
    python3 ai_watcher.py            # run continuously
    python3 ai_watcher.py --digest   # trigger digest now and exit

The container handles crawling + content fetching.
This script handles all AI work (no Anthropic API key needed).
"""

import argparse
import json
import logging
import re
import sqlite3
import subprocess
import sys
import time
from datetime import datetime, timezone, timedelta

DB_PATH = "/var/lib/containers/storage/volumes/ai-feed-crawler_aifeed_data/_data/articles.db"
CLAUDE_CLI = "claude"
POLL_INTERVAL = 300  # seconds between scans (5 min fallback)
SIGNAL_FILE   = "/var/lib/containers/storage/volumes/ai-feed-crawler_aifeed_data/_data/.process_done"
BATCH_SIZE    = 5    # articles to process per cycle (keeps writes short)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ai_watcher")

ANALYSIS_SYSTEM = (
    "You are a senior AI industry analyst writing for DeepTrendLab, an AI news aggregator. "
    "Write sharp, original editorial analysis. No filler. No bullet lists — paragraphs only. "
    "Never copy sentences from the source article. Synthesize, contextualize, and interpret."
)

DIGEST_SYSTEM = (
    "You are the editorial voice of DeepTrendLab, an AI news aggregator. "
    "Write sharp, informed, opinionated editorial digests. "
    "Use clear section headings. No filler. No bullet lists — write in paragraphs. "
    "Assume readers follow AI closely. Be concise but substantive."
)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=30000")
    return conn


# ---------------------------------------------------------------------------
# Claude CLI
# ---------------------------------------------------------------------------

def call_claude(prompt: str, system: str = "", timeout: int = 60) -> str:
    cmd = [CLAUDE_CLI, "-p", prompt, "--model", "claude-haiku-4-5-20251001"]
    if system:
        cmd += ["--append-system-prompt", system]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode == 0:
            return result.stdout.strip()
        log.debug("claude CLI exit %d: %s", result.returncode, result.stderr[:200])
    except subprocess.TimeoutExpired:
        log.warning("claude CLI timed out after %ds", timeout)
    except Exception as e:
        log.warning("claude CLI error: %s", e)
    return ""


# ---------------------------------------------------------------------------
# Tags
# ---------------------------------------------------------------------------

def generate_tags(title: str, text: str, source_name: str) -> list:
    snippet = (text or title)[:1500]
    prompt = (
        f'Article from {source_name}: "{title}"\n\n{snippet}\n\n'
        'Return a JSON array of 3-5 short topic tags (lowercase, no #). '
        'Example: ["llm", "openai", "fine-tuning"]. JSON only, no other text.'
    )
    raw = call_claude(prompt, timeout=20)
    if not raw:
        return []
    try:
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("`").strip()
        match = re.search(r'\[.*?\]', raw, re.DOTALL)
        if match:
            raw = match.group(0)
        result = json.loads(raw)
        if isinstance(result, list):
            return [str(t).lower().strip() for t in result if t][:5]
    except Exception:
        pass
    return []


# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------

def generate_analysis(title: str, text: str, source_name: str, category: str) -> str:
    snippet = (text or title)[:3000]
    prompt = (
        f'Article: "{title}" (from {source_name}, category: {category})\n\n'
        f'Content:\n{snippet}\n\n'
        'Write a 5-6 paragraph editorial analysis for DeepTrendLab readers. Structure:\n'
        '1. What happened — the key facts and announcement details\n'
        '2. Background and context — why this moment, what led here\n'
        '3. Why this matters — significance in the broader AI landscape, context vs competitors\n'
        '4. Who it affects — impact on developers, enterprises, researchers, or consumers\n'
        '5. Competitive angle — how this shifts the landscape vs rivals\n'
        '6. What to watch — implications, open questions, what comes next\n\n'
        'Use <p> tags for each paragraph. Do not copy sentences from the article. '
        'Be specific, analytical, and opinionated. Target 500-750 words total.'
    )
    return call_claude(prompt, system=ANALYSIS_SYSTEM, timeout=60)


# ---------------------------------------------------------------------------
# Digest
# ---------------------------------------------------------------------------

DIGEST_DAYS = {"daily": 1, "weekly": 7, "monthly": 30}


def generate_digest(period: str = "daily") -> bool:
    from slugify import slugify as _slugify
    import hashlib

    days = DIGEST_DAYS.get(period, 1)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    conn = get_conn()
    rows = conn.execute(
        """SELECT title, summary, source_name, category, url, slug
           FROM articles WHERE published >= ? AND source_name != 'DeepTrendLab'
           ORDER BY published DESC LIMIT 60""",
        (cutoff,),
    ).fetchall()
    conn.close()

    if len(rows) < 3:
        log.info("Digest skipped — only %d articles for period=%s", len(rows), period)
        return False

    article_lines = "\n".join(
        f"- [{r['source_name']}] {r['title']}"
        + (f": {r['summary'][:120]}" if r['summary'] else "")
        for r in rows
    )

    now_dt = datetime.now(timezone.utc)
    if period == "daily":
        date_label   = now_dt.strftime("%B %d, %Y")
        date_slug    = now_dt.strftime("%B-%d-%Y").lower()
        period_label = "today"
        title_prefix = f"AI News {date_label}"
        title_example = f"AI News {date_label}: OpenAI, DeepMind, and What Actually Matters"
    elif period == "weekly":
        date_label   = f"Week of {now_dt.strftime('%B %d, %Y')}"
        date_slug    = f"week-of-{now_dt.strftime('%B-%d-%Y').lower()}"
        period_label = "this week"
        title_prefix = f"Top AI Stories — {date_label}"
        title_example = f"Top AI Stories — {date_label}: The Moves That Will Shape the Next Month"
    else:
        date_label   = now_dt.strftime("%B %Y")
        date_slug    = now_dt.strftime("%B-%Y").lower()
        period_label = "this month"
        title_prefix = f"AI Developments {date_label}"
        title_example = f"AI Developments {date_label}: A Month of Model Releases and Policy Shifts"

    prompt = f"""Here are the AI news articles published {period_label} ({len(rows)} articles):

{article_lines}

Write a {period} AI digest editorial post for DeepTrendLab covering {date_label}.

Return a JSON object:
{{
  "title": "<SEO headline — MUST start with '{title_prefix}:' then a compelling hook. Example: '{title_example}'>",
  "summary": "<2 sentence summary. MUST include the date '{date_label}' and mention 2-3 specific topics. Max 200 chars.>",
  "sections": [
    {{"heading": "<section heading>", "body": "<2-3 paragraph editorial, HTML <p> tags only>"}},
    ...
  ],
  "tags": ["<tag1>", "<tag2>", "<tag3>", "<tag4>", "<tag5>"]
}}

Write 4-6 sections grouping related stories by theme. Be opinionated and analytical.
The title format is critical for SEO — people search for 'AI news {date_label}' and 'latest AI developments {date_label}'."""

    raw = call_claude(prompt, system=DIGEST_SYSTEM, timeout=90)
    if not raw:
        log.error("Digest generation returned empty response")
        return False

    try:
        if raw.startswith("```"):
            raw = re.sub(r"^```[a-z]*\n?", "", raw).rstrip("`").strip()
        match = re.search(r'\{.*\}', raw, re.DOTALL)
        if match:
            raw = match.group(0)
        data = json.loads(raw)
    except Exception as e:
        log.error("Digest JSON parse failed: %s", e)
        return False

    title   = data.get("title", f"AI News {date_label}: Top Stories")
    summary = data.get("summary", "")
    base_tags = data.get("tags", [])
    date_tags = [f"ai news {date_label.lower()}", "ai news", "artificial intelligence news", period]
    tags = json.dumps(list(dict.fromkeys(date_tags + base_tags))[:8])

    # Build content HTML
    content_parts = []
    for s in data.get("sections", []):
        content_parts.append(f"<h2>{s.get('heading','')}</h2>\n{s.get('body','')}")

    conn2 = get_conn()
    slug_rows = conn2.execute(
        "SELECT slug, title FROM articles WHERE published >= ? AND source_name != 'DeepTrendLab'",
        (cutoff,),
    ).fetchall()
    conn2.close()

    slug_map = {r["title"].lower(): r["slug"] for r in slug_rows}
    content_parts.append("<h2>All Stories This Period</h2><ul>")
    for r in rows[:30]:
        art_url = f"/article/{r['slug']}" if r['slug'] else r['url']
        content_parts.append(f'<li><a href="{art_url}">{r["title"]}</a></li>')
    content_parts.append("</ul>")
    content_html = "\n".join(content_parts)

    if period == "daily":
        slug_base = _slugify(f"ai-news-{date_slug}", max_length=70, word_boundary=True)
    elif period == "weekly":
        slug_base = _slugify(f"top-ai-stories-{date_slug}", max_length=70, word_boundary=True)
    else:
        slug_base = _slugify(f"ai-developments-{date_slug}", max_length=70, word_boundary=True)

    uid = hashlib.md5(slug_base.encode()).hexdigest()[:16]
    art_url = f"https://www.deeptrendlab.com/article/{slug_base}"
    now_iso = datetime.now(timezone.utc).isoformat()

    conn3 = get_conn()
    conn3.execute(
        """INSERT OR REPLACE INTO articles
           (uid, slug, title, url, summary, image_url, source_name, source_url,
            category, logo, published, fetched_at, content_html, full_text,
            tags, processed, reading_time, author)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (uid, slug_base, title, art_url, summary,
         "https://images.unsplash.com/photo-1504711434969-e33886168f5c?w=1200&q=80",
         "DeepTrendLab", "https://www.deeptrendlab.com",
         "News", "📰", now_iso, now_iso, content_html, content_html,
         tags, 2, max(1, len(content_html.split()) // 200), "DeepTrendLab Editorial"),
    )
    conn3.commit()
    conn3.close()

    log.info("Digest created: %s", title)
    return True


# ---------------------------------------------------------------------------
# Main watcher loop
# ---------------------------------------------------------------------------

def process_pending():
    """Process one batch of articles needing AI enrichment."""
    conn = get_conn()
    rows = conn.execute(
        """SELECT uid, title, source_name, category, full_text, summary, tags
           FROM articles
           WHERE processed = 2
             AND source_name != 'DeepTrendLab'
             AND (ai_digest IS NULL OR ai_digest = '')
             AND (full_text IS NOT NULL OR summary IS NOT NULL)
           ORDER BY published DESC LIMIT ?""",
        (BATCH_SIZE,),
    ).fetchall()
    conn.close()

    if not rows:
        return 0

    count = 0
    for row in rows:
        text = row["full_text"] or row["summary"] or ""
        log.info("Processing: %s", row["title"][:70])

        # Tags (merge with existing RSS tags)
        new_tags = generate_tags(row["title"], text, row["source_name"])
        existing = json.loads(row["tags"]) if row["tags"] else []
        if new_tags:
            merged_tags = json.dumps(list(dict.fromkeys(new_tags + existing))[:8])
        else:
            merged_tags = row["tags"] or "[]"

        # Analysis
        analysis = generate_analysis(row["title"], text, row["source_name"], row["category"])

        if analysis or new_tags:
            conn2 = get_conn()
            conn2.execute(
                "UPDATE articles SET ai_digest=?, tags=? WHERE uid=?",
                (analysis or None, merged_tags, row["uid"]),
            )
            conn2.commit()
            conn2.close()
            count += 1
            log.info("  Done — %d words analysis, %s tags",
                     len(analysis.split()) if analysis else 0,
                     json.loads(merged_tags))
        time.sleep(0.2)

    return count


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--digest", choices=["daily", "weekly", "monthly"],
                        help="Generate a digest and exit")
    parser.add_argument("--backfill", action="store_true",
                        help="Run full backfill until all articles have analysis, then exit")
    args = parser.parse_args()

    if args.digest:
        log.info("Generating %s digest...", args.digest)
        ok = generate_digest(args.digest)
        sys.exit(0 if ok else 1)

    if args.backfill:
        log.info("Running full backfill...")
        total = 0
        while True:
            n = process_pending()
            total += n
            log.info("Batch done: +%d (total %d)", n, total)
            if n == 0:
                log.info("Backfill complete — %d articles processed", total)
                break
        sys.exit(0)

    # Continuous watcher mode
    log.info("AI watcher started — triggered by container signal + %ds fallback poll", POLL_INTERVAL)
    last_daily  = None
    last_weekly = None
    last_poll   = 0

    import os
    while True:
        try:
            now = datetime.now(timezone.utc)
            triggered = os.path.exists(SIGNAL_FILE)
            due_for_poll = (time.time() - last_poll) >= POLL_INTERVAL

            if triggered or due_for_poll:
                if triggered:
                    os.remove(SIGNAL_FILE)
                    log.info("Signal received — processing new articles")

                n = process_pending()
                if n:
                    log.info("Processed %d articles", n)
                last_poll = time.time()

            # Daily digest at 08:00 UTC
            if now.hour == 8 and (last_daily is None or last_daily.date() < now.date()):
                log.info("Triggering daily digest...")
                if generate_digest("daily"):
                    last_daily = now

            # Weekly digest on Sunday at 09:00 UTC
            if now.weekday() == 6 and now.hour == 9 and \
               (last_weekly is None or (now - last_weekly).days >= 6):
                log.info("Triggering weekly digest...")
                if generate_digest("weekly"):
                    last_weekly = now

        except Exception as e:
            log.error("Watcher error: %s", e)

        time.sleep(10)  # tight loop checking signal file only


if __name__ == "__main__":
    main()
