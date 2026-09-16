# syntax=docker/dockerfile:1

FROM debian:bookworm

ENV DEBIAN_FRONTEND=noninteractive

ARG CMAKE_VERSION=3.31.0
ARG VCPKG_REF=master

RUN apt-get update && apt-get install -y --no-install-recommends \
    autoconf \
    bison \
    build-essential \
    ca-certificates \
    curl \
    flex \
    git \
    libssl-dev \
    libtool \
    liburing-dev \
    linux-libc-dev \
    ninja-build \
    openssl \
    pkg-config \
    python3 \
    re2c \
    tar \
    unzip \
    wget \
    zip \
    && rm -rf /var/lib/apt/lists/*

RUN curl -fsSL "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.sh" \
    -o /tmp/cmake-install.sh \
    && chmod +x /tmp/cmake-install.sh \
    && /tmp/cmake-install.sh --skip-license --prefix=/usr/local \
    && rm -f /tmp/cmake-install.sh

RUN git clone https://github.com/microsoft/vcpkg /opt/vcpkg \
    && cd /opt/vcpkg \
    && git checkout "${VCPKG_REF}" \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics \
    && ln -sf /opt/vcpkg/vcpkg /usr/local/bin/vcpkg

WORKDIR /workspace

# This image is a reusable Linux build base.
# It intentionally avoids exporting project-specific preset variables.
# Derived images or user presets should decide how build tools are wired in.
