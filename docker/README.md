# DOCKER

This directory provides a `Dockerfile` which can be used for building and
running renterd within a docker container. For the time being it is provided
as-is without any compatibility guarantees as it will change over time and be
extended with more configuration options.

## Building Image

From within the `./docker` folder run the following command to build an image of `renterd` named `renterd` build from the `master`-branch.

`docker build -t renterd --build-arg BRANCH=master .`

## Running Container

Run `renterd` in the background as a container named `renterd` that exposes its
API to the host system and the gateway to the world.

`docker run -d --name renterd -e RENTERD_API_PASSWORD="<PASSWORD>" -e RENTERD_WALLET_SEED="<SEED>" -p 127.0.0.1:9980:9980/tcp -p :9981:9981/tcp <IMAGE_ID>`