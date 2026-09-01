# Use lightweight official Node.js image
FROM node:20-alpine

# Set working directory
WORKDIR /app

# Copy package definitions
COPY package*.json ./

# Install production dependencies
RUN npm install --omit=dev

# Copy all application files
COPY . .

# Ensure data directory exists
RUN mkdir -p data

# Expose dynamic cloud port (7860 for Hugging Face Spaces, 3000 for standard)
ENV PORT=7860
EXPOSE 7860 3000

# Start server
CMD ["node", "server.js"]
