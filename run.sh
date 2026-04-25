#!/bin/bash
# Build and run AIFeed

set -e
cd "$(dirname "$0")"

echo "Building AIFeed image..."
docker build -t aifeed .

echo "Stopping any existing container..."
docker stop aifeed 2>/dev/null || true
docker rm   aifeed 2>/dev/null || true

mkdir -p ./data

# Load .env if present
[ -f .env ] && export $(grep -v '^#' .env | xargs)

echo "Starting AIFeed on port 8080..."
docker run -d \
  --name aifeed \
  -p 8080:5000 \
  -v "$(pwd)/data:/data" \
  -e ANTHROPIC_API_KEY="${ANTHROPIC_API_KEY:-}" \
  -e SITE_URL="${SITE_URL:-http://localhost:8080}" \
  --restart unless-stopped \
  localhost/aifeed:latest

echo ""
echo "AIFeed is running at http://localhost:8080"
echo "View logs: docker logs -f aifeed"
echo "Stop:      docker stop aifeed"
echo ""
echo "To enable AI digests, create a .env file:"
echo "  echo 'ANTHROPIC_API_KEY=sk-ant-...' > .env && ./run.sh"
