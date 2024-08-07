# Helper image to build renterd.
FROM golang:1.22 AS builder

# Define arguments for build tags and to skip running go generate.
ARG BUILD_TAGS='netgo' \
    BUILD_RUN_GO_GENERATE='true'

# Set the working directory.
WORKDIR /renterd

# Copy over the go mod and sum files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy the rest of the application code
COPY . .

# Generate build metadata.
RUN if [ "$BUILD_RUN_GO_GENERATE" = "true" ] ; then go generate ./... ; fi

# Build renterd.
RUN --mount=type=cache,target=/root/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=1 go build -ldflags='-s -w -linkmode external -extldflags "-static"' -tags="${BUILD_TAGS}" ./cmd/renterd

# Build image that will be used to run renterd.
FROM alpine:3
LABEL maintainer="The Sia Foundation <info@sia.tech>" \
      org.opencontainers.image.description.vendor="The Sia Foundation" \
      org.opencontainers.image.description="A renterd container - next-generation Sia renter" \
      org.opencontainers.image.source="https://github.com/SiaFoundation/renterd" \
      org.opencontainers.image.licenses=MIT

# User to run renterd as. Defaults to root.
ENV PUID=0
ENV PGID=0

# Entrypoint env args
ARG BUILD_TAGS
ENV BUILD_TAGS=$BUILD_TAGS

# Renterd env args
ENV RENTERD_API_PASSWORD= 
ENV RENTERD_SEED=
ENV RENTERD_CONFIG_FILE=/data/renterd.yml

# Copy binary and prepare data dir.
COPY --from=builder /renterd/renterd /usr/bin/renterd
VOLUME [ "/data" ]

USER ${PUID}:${PGID}

# Copy the script and set it as the entrypoint.
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh
ENTRYPOINT ["/entrypoint.sh", "-dir", "./data"]
