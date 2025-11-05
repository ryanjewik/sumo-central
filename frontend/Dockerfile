# Production multi-stage Dockerfile for Next.js
ARG NODE_VERSION=20
ARG NEXT_PUBLIC_BACKEND_URL

# Expose build-time backend URL to the builder so Next can embed it at build-time when
# process.env.NEXT_PUBLIC_BACKEND_URL is read by next.config.ts or application code.

# Builder: install deps and build the app
FROM node:${NODE_VERSION}-alpine AS builder
WORKDIR /app
ENV PATH=/app/node_modules/.bin:$PATH

# Set the build-time backend URL for the builder stage
ARG NEXT_PUBLIC_BACKEND_URL
ENV NEXT_PUBLIC_BACKEND_URL=${NEXT_PUBLIC_BACKEND_URL}
ENV BACKEND_URL=${NEXT_PUBLIC_BACKEND_URL}

# Copy package files and install dependencies
COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --silent --prefer-offline; else npm install --silent; fi

# Copy source and build
COPY . .
RUN npm run build

# Runner: smaller image that only contains runtime files
FROM node:${NODE_VERSION}-alpine AS runner
WORKDIR /app
ENV NODE_ENV=production
ENV PORT=3000
ENV PATH=/app/node_modules/.bin:$PATH

# Copy only what is needed to run
COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public
COPY --from=builder /app/next.config.js ./next.config.js

# Expose application port
EXPOSE 3000

# Start Next in production mode. Ensure package.json has a `start` script (next start -p 3000)
CMD ["npm", "run", "start"]
