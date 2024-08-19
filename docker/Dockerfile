# Helper image to build renterd.
FROM golang:1.23 AS builder

# Define arguments for build tags and to skip running go generate.
ARG BUILD_RUN_GO_GENERATE='true'

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
    CGO_ENABLED=1 go build -ldflags='-s -w -linkmode external -extldflags "-static"' ./cmd/renterd

# Build image that will be used to run renterd.
FROM scratch
LABEL maintainer="The Sia Foundation <info@sia.tech>" \
    org.opencontainers.image.description.vendor="The Sia Foundation" \
    org.opencontainers.image.description="A renterd container - next-generation Sia renter" \
    org.opencontainers.image.source="https://github.com/SiaFoundation/renterd" \
    org.opencontainers.image.licenses=MIT

# User to run renterd as. Defaults to root.
ENV PUID=0
ENV PGID=0

# Renterd env args
ENV RENTERD_API_PASSWORD= 
ENV RENTERD_SEED=
ENV RENTERD_CONFIG_FILE=/data/renterd.yml
ENV RENTERD_NETWORK='mainnet'

# Copy binary and prepare data dir.
COPY --from=builder /renterd/renterd /usr/bin/renterd
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
VOLUME [ "/data" ]

USER ${PUID}:${PGID}

# Copy the script and set it as the entrypoint.
ENTRYPOINT ["renterd", "-env", "-http", ":9980", "-s3.address", ":8080", "-dir", "./data"]
