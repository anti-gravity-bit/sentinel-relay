# syntax=docker/dockerfile:1
FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY go.mod ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" -o /out/relayd ./cmd/relayd

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/relayd /usr/local/bin/relayd

EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/relayd"]
