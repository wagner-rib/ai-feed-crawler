"""Flask web application for the AI Feed Aggregator."""

import sqlite3
import math
from datetime import datetime, timezone
from pathlib import Path

from flask import Flask, render_template, request, jsonify, g
from apscheduler.schedulers.background import BackgroundScheduler
import humanize

from crawler import init_db, crawl_all, get_stats, DB_PATH
from feeds_config import AI_FEEDS, CATEGORIES

app = Flask(__name__)
app.config["JSON_SORT_KEYS"] = False

PER_PAGE = 20


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


# ---------------------------------------------------------------------------
# Routes
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
        where.append("(title LIKE ? OR summary LIKE ?)")
        params.extend([f"%{q}%", f"%{q}%"])

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
    art = db.execute(
        "SELECT * FROM articles WHERE uid = ?", (uid,)
    ).fetchone()
    if art is None:
        return render_template("404.html"), 404
    db.execute("UPDATE articles SET views = views + 1 WHERE uid = ?", (uid,))
    db.commit()
    related = db.execute(
        """SELECT * FROM articles
           WHERE category = ? AND uid != ?
           ORDER BY published DESC LIMIT 6""",
        (art["category"], uid),
    ).fetchall()
    return render_template("article.html", article=art, related=related)


@app.route("/sources")
def sources():
    return render_template("sources.html", feeds=AI_FEEDS, categories=CATEGORIES)


@app.route("/api/stats")
def api_stats():
    return jsonify(get_stats())


@app.route("/api/refresh")
def api_refresh():
    count = crawl_all()
    return jsonify({"status": "ok", "new_articles": count})


@app.errorhandler(404)
def not_found(e):
    return render_template("404.html"), 404


# ---------------------------------------------------------------------------
# Scheduler
# ---------------------------------------------------------------------------

def start_scheduler():
    scheduler = BackgroundScheduler(daemon=True)
    scheduler.add_job(crawl_all, "interval", hours=2, id="crawl_feeds",
                      next_run_time=datetime.now())
    scheduler.start()
    app.logger.info("Scheduler started — crawling every 2 hours")


# ---------------------------------------------------------------------------
# Startup — only init DB and start scheduler once (gunicorn --preload runs
# this in the master process before forking workers)
# ---------------------------------------------------------------------------

import os as _os

init_db()
# Avoid starting the scheduler twice in development (reloader forks a child)
if not _os.environ.get("WERKZEUG_RUN_MAIN"):
    start_scheduler()

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
