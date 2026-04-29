# DeepTrendLab

**AI news aggregator pulling from 50+ sources into one clean feed.**

Live at [deeptrendlab.com](https://deeptrendlab.com)

---

## What it does

Crawls 50+ top AI sources every 2 hours and surfaces articles in one place, organized by category:

| Category | Sources include |
|---|---|
| AI Labs | OpenAI, Anthropic, DeepMind, Meta AI, Google AI, NVIDIA, Hugging Face |
| News | The Verge, VentureBeat, TechCrunch, MIT Tech Review, Wired, Ars Technica |
| Research | Papers With Code, Berkeley AI Research, Distill.pub, fast.ai |
| Newsletters | The Batch, Ben's Bites, Last Week in AI, Towards Data Science |
| Safety | LessWrong, Future of Life Institute, AI Now Institute |
| Tools | Weights & Biases, AssemblyAI, Replicate, Scale AI |
| Business | a16z AI, Benedict Evans, TOPBOTS |

**678+ articles indexed. Updated every 2 hours.**

---

## How it works

**Crawling** — Pure RSS via `feedparser`. Each run pulls up to 30 entries per feed, deduplicates by SHA-256 hash of the URL, and filters paywalled/login-required paths automatically. New articles trigger IndexNow pings to Bing/Yandex for fast indexing.

**Content extraction** — For JS-heavy or soft-paywalled sources (MIT Tech Review, Wired, TechCrunch, Ars Technica), falls back to [Jina Reader](https://r.jina.ai) to extract clean text. Otherwise uses `readability-lxml` + BeautifulSoup directly.

**Tagging** — Categorization is source-based (each feed pre-assigned in config). Claude (Anthropic API) generates tags and a clean card excerpt per article.

**Schedule** — Crawls every 2 hours, processes/tags every 30 minutes. If new articles arrive mid-cycle, processing triggers immediately.

---

## Stack

- **Backend:** Python, Flask, Gunicorn
- **Database:** SQLite
- **Parsing:** feedparser, readability-lxml, BeautifulSoup, trafilatura
- **AI:** Anthropic API (Claude) for tagging and excerpts
- **Infra:** Docker, docker-compose

---

## Running locally

**Prerequisites:** Docker and docker-compose installed.

```bash
git clone https://github.com/wagner-rib/ai-feed-crawler.git
cd ai-feed-crawler
cp .env.example .env  # add your ANTHROPIC_API_KEY
docker compose up -d
```

App runs on `http://localhost:5000`.

**Environment variables:**

| Variable | Description |
|---|---|
| `ANTHROPIC_API_KEY` | Required for AI tagging and excerpts |
| `SITE_URL` | Public URL of the site (default: https://deeptrendlab.com) |
| `INDEXNOW_KEY` | Optional — for IndexNow pings to search engines |

---

## Adding or removing sources

Edit `feeds_config.py` — each source is a dict with `name`, `rss_url`, `website_url`, `category`, and `logo`.

---

## License

MIT
