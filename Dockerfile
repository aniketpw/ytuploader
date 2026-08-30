# Multi-stage build for better-sqlite3 native compilation
FROM node:20-alpine AS builder

# Install build tools for native addons (better-sqlite3)
RUN apk add --no-cache python3 make g++

WORKDIR /app

# Copy package definition and install
COPY package*.json ./
RUN npm ci --omit=dev

# --- Production stage (minimal image) ---
FROM node:20-alpine

WORKDIR /app

# Copy pre-built node_modules from builder
COPY --from=builder /app/node_modules ./node_modules

# Copy application files
COPY server.js ./
COPY db.js ./
COPY public ./public

# Create data directory
RUN mkdir -p data

# Expose port
ENV PORT=3000
EXPOSE 3000

# Start server
CMD ["node", "server.js"]
