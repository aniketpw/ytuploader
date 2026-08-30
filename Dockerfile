FROM node:20-bookworm-slim

# Install native addon compilation tools for better-sqlite3
RUN apt-get update && apt-get install -y python3 make g++ gcc libc6-dev && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy package definitions
COPY package*.json ./

# Install production dependencies and compile better-sqlite3 natively
RUN npm install --omit=dev && npm rebuild better-sqlite3 --build-from-source

# Copy all application files
COPY . .

# Ensure data directory exists
RUN mkdir -p data

# Dynamic cloud port
ENV PORT=3000
EXPOSE 3000

# Start server
CMD ["node", "server.js"]
