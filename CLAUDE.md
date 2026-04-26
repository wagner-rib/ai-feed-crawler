# AIFeed — Project Notes

## Deployment

The app runs in a Docker container (`aifeed`) via `docker-compose.yml`.
Templates and static files are **baked into the image** — there is no volume mount for `/app`.

### Every code change requires two steps:

1. **Live preview** (instant, but lost on rebuild):
   ```bash
   docker cp templates/index.html aifeed:/app/templates/index.html
   docker cp static/css/style.css aifeed:/app/static/css/style.css
   ```

2. **Permanent deploy** (bakes changes into the image):
   ```bash
   docker compose build --no-cache
   docker compose down && docker compose up -d
   ```

Always do step 2 before finishing — changes only in the running container will be lost if the container is ever restarted or rebuilt.
