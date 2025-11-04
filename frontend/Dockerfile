# Development Dockerfile (temporary)
# This image is intended for local development and runs `npm run dev` inside the container.

ARG NODE_VERSION=20
FROM node:${NODE_VERSION}-alpine
WORKDIR /app

# Ensure local node bin path is available
ENV PATH=/app/node_modules/.bin:$PATH

# Install dependencies (dev + prod) to support `npm run dev`
COPY package.json package-lock.json* ./
RUN if [ -f package-lock.json ]; then npm ci --silent --prefer-offline; else npm install --silent; fi

# Copy app source
COPY . .

ENV NODE_ENV=development

# Expose Next dev port
EXPOSE 3000

# Run the Next dev server. The dev server will watch files and keep the container running.
CMD ["npm", "run", "dev"]
