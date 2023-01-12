# Helper image to build renterd.
FROM golang:1.19 AS builder
LABEL maintainer="The Sia Foundation <info@sia.tech>"

ARG BRANCH

WORKDIR /

# Clone and build binary.
RUN git clone https://github.com/SiaFoundation/renterd.git --single-branch --depth 1 --branch ${BRANCH}
WORKDIR /renterd
RUN go build ./cmd/renterd

# Build image that will be used to run renterd.
FROM debian:bookworm-slim
LABEL maintainer="The Sia Foundation <info@sia.tech>"

# User to run renterd as. Defaults to root.
ENV PUID=0
ENV PGID=0

# Renterd env args..
ENV RENTERD_API_PASSWORD= 
ENV RENTERD_WALLET_SEED=

# Copy binary and prepare data dir.
COPY --from=builder /renterd/renterd /usr/bin/renterd
VOLUME [ "/data" ]

EXPOSE 9980/tcp
EXPOSE 9981/tcp

USER ${PUID}:${PGID}

ENTRYPOINT [ "renterd", "-dir", "./data" ]
