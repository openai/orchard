FROM gcr.io/distroless/base

ARG TARGETPLATFORM

LABEL org.opencontainers.image.source=https://github.com/openai/orchard
ENV GIN_MODE=release
ENV ORCHARD_HOME=/data
EXPOSE 6120

COPY $TARGETPLATFORM/orchard /bin/orchard

ENTRYPOINT ["/bin/orchard"]

# default arguments to run controller
CMD ["controller", "run"]
