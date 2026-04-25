#!/bin/bash
# Build and run AIFeed using Docker (podman-docker) or podman-compose

cd "$(dirname "$0")"

# Option 1: Direct docker run
echo "Building AIFeed image..."
docker build -t aifeed .

echo "Stopping any existing container..."
docker stop aifeed 2>/dev/null || true
docker rm aifeed 2>/dev/null || true

mkdir -p ./data

echo "Starting AIFeed on port 8080..."
docker run -d \
  --name aifeed \
  -p 8080:5000 \
  -v "$(pwd)/data:/data" \
  --restart unless-stopped \
  localhost/aifeed:latest

echo ""
echo "AIFeed is running at http://localhost:8080"
echo "View logs: docker logs -f aifeed"
echo "Stop:      docker stop aifeed"
