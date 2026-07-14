# Build stage: Go binary
FROM golang:1.22-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
COPY cmd/ cmd/
RUN CGO_ENABLED=0 go build -o mini-kafka ./cmd/mini-kafka/

# Build stage: Frontend
FROM node:20-alpine AS frontend
WORKDIR /app
COPY frontend/package*.json ./
RUN npm install
COPY frontend/ ./
RUN npx vite build

# Final stage: minimal runtime
FROM alpine:3.19
WORKDIR /app
COPY --from=builder /app/mini-kafka .
COPY --from=frontend /app/dist ./frontend/dist/
COPY crash_demo.sh .

# Default ports: TCP broker on 9092, Dashboard on 8080
# For Railway/Render: set PORT env var, dashboard uses it
EXPOSE 8080 9092

CMD ["./mini-kafka", "broker", "--port", "9092", "--dash", "8080"]
