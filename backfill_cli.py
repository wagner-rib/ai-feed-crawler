#!/usr/bin/env python3
"""
Backfill article analysis using the claude CLI instead of the Anthropic API.

WARNING: Only run this script when the Docker container is STOPPED.
Running concurrently with the container will corrupt the SQLite database.

  docker compose down
  python3 backfill_cli.py
  docker compose up -d
"""

import sqlite3
import subprocess
import sys
import time

DB_PATH = "/var/lib/containers/storage/volumes/ai-feed-crawler_aifeed_data/_data/articles.db"
BATCH = 20

SYSTEM = (
    "You are a senior AI industry analyst writing for DeepTrendLab, an AI news aggregator. "
    "Write sharp, original editorial analysis. No filler. No bullet lists — paragraphs only. "
    "Never copy sentences from the source article. Synthesize, contextualize, and interpret."
)


def get_remaining(conn):
    return conn.execute(
        """SELECT COUNT(*) FROM articles
           WHERE processed=2 AND source_name!='DeepTrendLab'
           AND (ai_digest IS NULL OR ai_digest='')
           AND (full_text IS NOT NULL OR summary IS NOT NULL)"""
    ).fetchone()[0]


def fetch_batch(conn):
    return conn.execute(
        """SELECT uid, title, source_name, category, full_text, summary
           FROM articles
           WHERE processed=2 AND source_name!='DeepTrendLab'
           AND (ai_digest IS NULL OR ai_digest='')
           AND (full_text IS NOT NULL OR summary IS NOT NULL)
           ORDER BY published DESC LIMIT ?""",
        (BATCH,),
    ).fetchall()


def generate_analysis(title, source_name, category, text):
    snippet = (text or "")[:3000]
    prompt = (
        f'Article: "{title}" (from {source_name}, category: {category})\n\n'
        f'Content:\n{snippet}\n\n'
        'Write a 5-6 paragraph editorial analysis. Structure:\n'
        '1. What happened — the key facts and announcement details\n'
        '2. Background and context — why this moment, what led here\n'
        '3. Why this matters — significance in the broader AI landscape\n'
        '4. Who it affects — developers, enterprises, researchers, or consumers\n'
        '5. Competitive angle — how this shifts the landscape vs rivals\n'
        '6. What to watch — implications, open questions, what comes next\n\n'
        'Use <p> tags for each paragraph. Do not copy sentences from the article. '
        'Be specific, analytical, and opinionated. Target 500-750 words total.'
    )
    try:
        result = subprocess.run(
            ["claude", "-p", prompt, "--append-system-prompt", SYSTEM],
            capture_output=True, text=True, timeout=60
        )
        if result.returncode == 0 and result.stdout.strip():
            return result.stdout.strip()
    except Exception as e:
        print(f"  CLI error: {e}", flush=True)
    return ""


def main():
    conn = sqlite3.connect(DB_PATH, timeout=60)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout=60000")

    total_done = 0
    batch_num = 1

    while True:
        remaining = get_remaining(conn)
        if remaining == 0:
            print(f"\nDone! Total generated: {total_done}", flush=True)
            break

        print(f"\nBatch {batch_num} — {remaining} remaining...", flush=True)
        rows = fetch_batch(conn)

        for row in rows:
            text = row["full_text"] or row["summary"] or ""
            print(f"  Processing: {row['title'][:70]}", flush=True)
            analysis = generate_analysis(row["title"], row["source_name"], row["category"], text)
            if analysis:
                conn.execute(
                    "UPDATE articles SET ai_digest=? WHERE uid=?",
                    (analysis, row["uid"]),
                )
                conn.commit()
                total_done += 1
                print(f"  ✓ Done ({len(analysis.split())} words)", flush=True)
            else:
                print(f"  ✗ Failed", flush=True)
            time.sleep(0.2)

        batch_num += 1

    conn.close()


if __name__ == "__main__":
    main()
