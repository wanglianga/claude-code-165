# ---------- 构建 ----------
FROM golang:1.22-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/server ./cmd/server

# ---------- 运行 ----------
FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates tzdata \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=build /out/server /app/server
COPY migrations /app/migrations
ENV HTTP_ADDR=:8080 \
    MIGRATIONS_DIR=/app/migrations
EXPOSE 8080
CMD ["/app/server"]
