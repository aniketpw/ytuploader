# Use lightweight official Node.js image
FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package definition
COPY package*.json ./

# Install production dependencies
RUN npm install --omit=dev

# Copy application files
COPY server.js ./
COPY public ./public
COPY data ./data

# Create data directory if not exists
RUN mkdir -p data

# Expose dynamic Cloud Run port
ENV PORT=3000
EXPOSE 3000

# Start server
CMD ["node", "server.js"]
