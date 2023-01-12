# [![Sia](https://sia.tech/banners/sia-banner-renterd.png)](http://sia.tech)

[![GoDoc](https://godoc.org/go.sia.tech/renterd?status.svg)](https://godoc.org/go.sia.tech/renterd)

A renter for Sia.

## Overview

`renterd` is a next-generation Sia renter, developed by the Sia Foundation. It
aims to serve the needs of both everyday users -- who want a simple interface
for storing and retrieving their personal data -- and developers -- who want to
a powerful, flexible, and reliable API for building apps on Sia.

To achieve this, we have carefully architected `renterd` to isolate its
user-friendly functionality, which we call "autopilot," from its low-level APIs.
Autopilot consists of background processes that scan and rank hosts, form and
renew contracts, and automatically migrate files when necessary. Most users want
this functionality, but for those who want absolute control over their contracts
and data, autopilot can easily be disabled with a CLI flag. Even better, the
autopilot code is built directly on top of the public `renterd` HTTP API,
meaning it can be easily forked, modified, or even ported to another language.

`renterd` also ships with an embedded web UI, rather than yet another Electron
app bundle. Developers and power users can even compile `renterd` without a UI
at all, reducing bloat and simplifying the build process.

## Current Status

All of the key low-level APIs have been implemented and are ready for use.
However, `renterd` currently does not ship with a UI or autopilot functionality,
and it lacks backwards-compatibility with `siad` renter metadata. This means
that, while `renterd` is already capable of serving as the backbone for new Sia
applications, most users should continue to use `siad`.

Going forward, our immediate priority is to implement autopilot functionality,
which will make `renterd` viable as a standalone renter. In tandem, we'll be
designing and integrating the embedded web UI. At this point, `renterd` will
become the recommended renter for new users. However, we also want to make it
painless for existing `siad` users to switch to `renterd`. So before shipping
v1.0, we'll implement compatibility code that will allow you to access all of
your `siad` files with `renterd`.

# DOCKER

Renterd comes with a `Dockerfile` which can be used for building and running
renterd within a docker container. For the time being it is provided as-is
without any compatibility guarantees as it will change over time and be extended
with more configuration options.

## Building Image

From within the root of the repo run the following command to build an image of
`renterd` tagged `renterd`.

`docker build -t renterd .`

## Running Container

Run `renterd` in the background as a container named `renterd` that exposes its
API to the host system and the gateway to the world.

`docker run -d --name renterd -e RENTERD_API_PASSWORD="<PASSWORD>" -e RENTERD_WALLET_SEED="<SEED>" -p 127.0.0.1:9980:9980/tcp -p :9981:9981/tcp <IMAGE_ID>`