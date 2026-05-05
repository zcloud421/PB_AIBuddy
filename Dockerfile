# Backend image for the main API service.
#
# Why Dockerfile instead of nixpacks:
#   Railway's nixpacks builder fetches its base image from
#   ghcr.io/railwayapp/nixpacks during build. This step has hung in
#   Railway's builder multiple times (40-min timeout, no error output).
#   First incident hit the daily-narrative-refresh cron (already migrated).
#   Second incident hit this main backend service. Pinning to the official
#   node:20-alpine image from Docker Hub eliminates the dependency on
#   Railway's private registry availability.
#
# Runtime model unchanged: npm start runs ts-node src/app.ts (same as
# previous nixpacks behavior). All cron services (screener / narrative /
# attrib-health) keep their own railway.*.json configs untouched.

FROM node:20-alpine

# Build essentials needed for native modules (pg, bull) on alpine
RUN apk add --no-cache python3 make g++

WORKDIR /app

# Copy package manifests first for layer caching
COPY package.json package-lock.json ./

# Install all deps (devDeps included — ts-node is a devDep but used in start)
RUN npm ci

# Copy source
COPY tsconfig.json ./
COPY src ./src

# Health check uses /health endpoint (matches railway.json healthcheckPath)
EXPOSE 3000

CMD ["npm", "start"]
