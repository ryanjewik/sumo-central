# Production Dockerfile for a Next.js application (multi-stage)
# This builds the app and runs it with `next start`.

ARG NODE_VERSION=20
FROM node:${NODE_VERSION}-alpine AS deps
WORKDIR /app

# Install only production dependencies for the final image cache layer
# Copy package manifests first for better layer caching
COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --silent --prefer-offline; else npm install --silent; fi

FROM node:${NODE_VERSION}-alpine AS builder
WORKDIR /app

# Ensure local node bin path is available
ENV PATH=/app/node_modules/.bin:$PATH

# Copy source and production deps
COPY . .
COPY --from=deps /app/node_modules ./node_modules

# Build the Next.js production artifacts
RUN npm run build

FROM node:${NODE_VERSION}-alpine AS runner
WORKDIR /app

ENV NODE_ENV=production

# Copy only the files needed to run the app
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/node_modules ./node_modules

# Expose the default Next.js port
EXPOSE 3000

# Start the Next.js server (ensure "start" script exists in package.json and runs `next start`)
CMD ["npm", "run", "start"]
