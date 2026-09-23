# Stage 1: Build stage
FROM node:24-alpine AS builder

WORKDIR /app

COPY package*.json ./

RUN npm ci

COPY . .

RUN npm run build

# Stage 2: Production runtime stage
FROM node:24-alpine AS runner

WORKDIR /app

ENV NODE_ENV=production

# Copy manifests and install production dependencies only
COPY package*.json ./
RUN npm ci --omit=dev

# Copy compiled files from builder
COPY --from=builder /app/dist ./dist

# Run as non-root user for security
USER node

CMD ["node", "dist/index.js"]