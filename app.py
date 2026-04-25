"""Flask web application — AIFeed AI News Aggregator."""

import json
import math
import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

from flask import (
    Flask, render_template, request, jsonify,
    g, Response, redirect, url_for,
)
from apscheduler.schedulers.background import BackgroundScheduler
import humanize

from crawler import init_db, crawl_all, get_stats, DB_PATH
from feeds_config import AI_FEEDS, CATEGORIES
from ai_processor import init_db_v2, process_batch

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

SITE_URL = os.environ.get("SITE_URL", "http://localhost:8080")
PER_PAGE = 24


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------

def get_db():
    if "db" not in g:
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        g.db = sqlite3.connect(DB_PATH)
        g.db.row_factory = sqlite3.Row
    return g.db


@app.teardown_appcontext
def close_db(exc):
    db = g.pop("db", None)
    if db is not None:
        db.close()


# ---------------------------------------------------------------------------
# Template helpers
# ---------------------------------------------------------------------------

@app.template_filter("timeago")
def timeago_filter(iso_str):
    if not iso_str:
        return "Unknown"
    try:
        dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return humanize.naturaltime(datetime.now(timezone.utc) - dt)
    except Exception:
        return iso_str[:10]


@app.template_filter("truncate_words")
def truncate_words(text, n=30):
    if not text:
        return ""
    words = text.split()
    if len(words) <= n:
        return text
    return " ".join(words[:n]) + "…"


@app.template_filter("from_json")
def from_json_filter(s):
    if not s:
        return []
    try:
        return json.loads(s)
    except Exception:
        return []


@app.context_processor
def inject_globals():
    return {"site_url": SITE_URL, "current_year": datetime.now().year}


# ---------------------------------------------------------------------------
# Routes — pages
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    db = get_db()
    page = max(1, request.args.get("page", 1, type=int))
    category = request.args.get("category", "")
    source = request.args.get("source", "")
    q = request.args.get("q", "").strip()

    where, params = [], []
    if category:
        where.append("category = ?")
        params.append(category)
    if source:
        where.append("source_name = ?")
        params.append(source)
    if q:
        where.append("(title LIKE ? OR summary LIKE ? OR ai_digest LIKE ? OR tags LIKE ?)")
        params.extend([f"%{q}%"] * 4)

    where_clause = ("WHERE " + " AND ".join(where)) if where else ""

    total = db.execute(
        f"SELECT COUNT(*) FROM articles {where_clause}", params
    ).fetchone()[0]

    offset = (page - 1) * PER_PAGE
    articles = db.execute(
        f"""SELECT * FROM articles {where_clause}
            ORDER BY published DESC
            LIMIT ? OFFSET ?""",
        params + [PER_PAGE, offset],
    ).fetchall()

    total_pages = max(1, math.ceil(total / PER_PAGE))
    stats = get_stats()
    sources = db.execute(
        "SELECT DISTINCT source_name FROM articles ORDER BY source_name"
    ).fetchall()

    return render_template(
        "index.html",
        articles=articles,
        categories=CATEGORIES,
        sources=[r["source_name"] for r in sources],
        selected_category=category,
        selected_source=source,
        query=q,
        page=page,
        total_pages=total_pages,
        total_articles=total,
        stats=stats,
        feeds=AI_FEEDS,
    )


@app.route("/article/<uid>")
def article(uid):
    db = get_db()
    art = db.execute("SELECT * FROM articles WHERE uid = ?", (uid,)).fetchone()
    if art is None:
        return render_template("404.html"), 404

    db.execute("UPDATE articles SET views = views + 1 WHERE uid = ?", (uid,))
    db.commit()

    # Related: same tags or same category
    tags = []
    if art["tags"]:
        try:
            tags = json.loads(art["tags"])[:2]
        except Exception:
            pass

    related = db.execute(
        """SELECT * FROM articles
           WHERE category = ? AND uid != ?
           ORDER BY published DESC LIMIT 6""",
        (art["category"], uid),
    ).fetchall()

    canonical = f"{SITE_URL}/article/{uid}"
    return render_template(
        "article.html",
        article=art,
        related=related,
        tags=tags,
        canonical=canonical,
    )


@app.route("/tag/<tag>")
def tag_page(tag):
    db = get_db()
    page = max(1, request.args.get("page", 1, type=int))
    articles = db.execute(
        "SELECT * FROM articles WHERE tags LIKE ? ORDER BY published DESC LIMIT ? OFFSET ?",
        (f'%"{tag}"%', PER_PAGE, (page - 1) * PER_PAGE),
    ).fetchall()
    total = db.execute(
        "SELECT COUNT(*) FROM articles WHERE tags LIKE ?", (f'%"{tag}"%',)
    ).fetchone()[0]
    total_pages = max(1, math.ceil(total / PER_PAGE))
    stats = get_stats()
    return render_template(
        "index.html",
        articles=articles,
        categories=CATEGORIES,
        sources=[],
        selected_category="",
        selected_source="",
        query="",
        page=page,
        total_pages=total_pages,
        total_articles=total,
        stats=stats,
        feeds=AI_FEEDS,
        tag=tag,
    )


@app.route("/sources")
def sources():
    return render_template("sources.html", feeds=AI_FEEDS, categories=CATEGORIES)


# ---------------------------------------------------------------------------
# Routes — SEO
# ---------------------------------------------------------------------------

@app.route("/sitemap.xml")
def sitemap():
    db = get_db()
    articles = db.execute(
        "SELECT uid, published FROM articles ORDER BY published DESC LIMIT 5000"
    ).fetchall()
    xml = render_template("sitemap.xml", articles=articles, site_url=SITE_URL)
    return Response(xml, mimetype="application/xml")


@app.route("/robots.txt")
def robots():
    txt = f"""User-agent: *
Allow: /
Disallow: /api/

Sitemap: {SITE_URL}/sitemap.xml
"""
    return Response(txt, mimetype="text/plain")


# ---------------------------------------------------------------------------
# Routes — API
# ---------------------------------------------------------------------------

@app.route("/api/stats")
def api_stats():
    return jsonify(get_stats())


@app.route("/api/refresh")
def api_refresh():
    count = crawl_all()
    return jsonify({"status": "ok", "new_articles": count})


@app.route("/api/process")
def api_process():
    limit = request.args.get("limit", 20, type=int)
    done = process_batch(limit=min(limit, 100))
    return jsonify({"status": "ok", "processed": done})


@app.route("/api/process-all")
def api_process_all():
    """Drain entire unprocessed queue — runs in background."""
    import threading
    def _run():
        with app.app_context():
            remaining = 9999
            total = 0
            while remaining > 0:
                n = process_batch(limit=60, delay=1.0)
                total += n
                if n == 0:
                    break
            app.logger.info("process-all complete — %d articles processed", total)
    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started", "message": "Processing all articles in background"})


@app.errorhandler(404)
def not_found(e):
    return render_template("404.html"), 404


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def crawl_and_process():
    crawl_all()
    process_batch(limit=60)


def start_scheduler():
    scheduler = BackgroundScheduler(daemon=True)
    # Crawl + process immediately on start, then every 2 hours
    scheduler.add_job(crawl_and_process, "interval", hours=2,
                      id="crawl_process", next_run_time=datetime.now())
    scheduler.start()
    app.logger.info("Scheduler started — crawl+process every 2 hours")


# ---------------------------------------------------------------------------
# Startup
# ---------------------------------------------------------------------------

import os as _os

init_db()
init_db_v2()

if not _os.environ.get("WERKZEUG_RUN_MAIN"):
    start_scheduler()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
