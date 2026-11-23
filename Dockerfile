# One image with both Go tools: /collector and /replay.
FROM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/collector ./cmd/collector \
 && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/replay ./cmd/replay

# static: no shell, but CA certificates for HTTPS to Wikimedia
FROM gcr.io/distroless/static-debian12
COPY --from=build /out/ /
# the default paths (data/bronze/..., .state/) are relative to /app
WORKDIR /app
