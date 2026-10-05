# ---- 构建阶段 ----
FROM golang:1.21-bookworm AS builder
WORKDIR /src

ENV CGO_ENABLED=0 GOOS=linux

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN go build -trimpath -ldflags="-s -w" -o /out/server ./cmd/server

# ---- 运行阶段 ----
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=builder /out/server /app/server

ENV HTTP_ADDR=":8080"
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/server"]
