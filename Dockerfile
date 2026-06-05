FROM golang:1.25 AS builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG VERSION=dev
ARG COMMIT=unknown

RUN CGO_ENABLED=0 go build \
    -trimpath \
    -ldflags="-X github.com/cirruslabs/orchard/internal/version.Version=${VERSION} -X github.com/cirruslabs/orchard/internal/version.Commit=${COMMIT} -B gobuildid" \
    -o /out/orchard \
    cmd/orchard/main.go

FROM gcr.io/distroless/base

LABEL org.opencontainers.image.source=https://github.com/openai/orchard
ENV GIN_MODE=release
ENV ORCHARD_HOME=/data
EXPOSE 6120

COPY --from=builder /out/orchard /bin/orchard

ENTRYPOINT ["/bin/orchard"]

# default arguments to run controller
CMD ["controller", "run"]
