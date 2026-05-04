# Minimal image for the daily-narrative-refresh cron service.
#
# Why Dockerfile instead of nixpacks:
#   nixpacks fetches its base image from ghcr.io/railwayapp/nixpacks during build.
#   That step has hung in Railway's builder more than once (40-min timeout, no
#   error output). Pinning to the official node:20-alpine image from Docker Hub
#   makes the build deterministic, ~10x faster, and not dependent on Railway's
#   private registry availability.
#
# This service has zero npm dependencies — it's a single .mjs that calls fetch()
# against the backend's /ideas/focus/daily-narrative/refresh endpoint.

FROM node:20-alpine

WORKDIR /app

# Copy package manifest first for layer-caching (no install needed since there
# are no dependencies, but keeps the structure compatible with future additions).
COPY package.json ./

# Copy the refresh script.
COPY refresh.mjs ./

CMD ["node", "refresh.mjs"]
