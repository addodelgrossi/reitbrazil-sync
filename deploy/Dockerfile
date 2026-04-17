# syntax=docker/dockerfile:1.6
FROM golang:1.26-alpine AS build

ARG VERSION=dev

WORKDIR /src

# Leverage the module cache first so source changes don't redownload deps.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ENV CGO_ENABLED=0
RUN go build -trimpath \
    -ldflags "-s -w -X main.version=${VERSION}" \
    -o /out/reitbrazilctl ./cmd/reitbrazilctl

# Minimal runtime image. distroless/static has no shell, no package manager,
# and ships only a trusted CA bundle.
FROM gcr.io/distroless/static:nonroot AS runtime

COPY --from=build /out/reitbrazilctl /reitbrazilctl

USER nonroot:nonroot
ENTRYPOINT ["/reitbrazilctl"]
