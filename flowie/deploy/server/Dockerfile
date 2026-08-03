# syntax=docker/dockerfile:1.7

FROM debian:bookworm AS builder

ARG CMAKE_VERSION=3.31.0
ARG CMAKE_SHA256=7cfdf4a411c71d13c027199952fd25e8245d85c932ff452e2b9a9e0f6dfe368a
ARG VCPKG_REF=b1b19307e2d2ec1eefbdb7ea069de7d4bcd31f01
ARG SOURCE_REVISION=unknown

ENV DEBIAN_FRONTEND=noninteractive \
    VCPKG_DISABLE_METRICS=1 \
    VCPKG_DEFAULT_BINARY_CACHE=/var/cache/vcpkg

RUN apt-get update && apt-get install -y --no-install-recommends \
      autoconf \
      bison \
      build-essential \
      ca-certificates \
      curl \
      flex \
      git \
      golang-go \
      libssl-dev \
      libtool \
      liburing-dev \
      linux-libc-dev \
      nasm \
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

RUN curl -fsSL \
      "https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/cmake-${CMAKE_VERSION}-linux-x86_64.sh" \
      -o /tmp/cmake-install.sh \
    && echo "${CMAKE_SHA256}  /tmp/cmake-install.sh" | sha256sum -c - \
    && chmod +x /tmp/cmake-install.sh \
    && /tmp/cmake-install.sh --skip-license --prefix=/usr/local \
    && rm -f /tmp/cmake-install.sh \
    && git clone https://github.com/microsoft/vcpkg /opt/vcpkg \
    && git -C /opt/vcpkg checkout "${VCPKG_REF}" \
    && /opt/vcpkg/bootstrap-vcpkg.sh -disableMetrics

COPY --from=turbo_utils . /src/turbo-utils
COPY --from=turbo_net . /src/turbonet
COPY --from=turbo_http . /src/TurboHTTP
COPY --from=rules_forge . /src/rulesforge

ENV FLOWIE_SDK_ROOT=/opt/flowie-sdk

RUN --mount=type=cache,target=/var/cache/vcpkg,sharing=locked \
    set -eux; \
    parallel="$(nproc)"; \
    cd /src/turbo-utils; \
    cmake --fresh --preset linux-release-user \
      -DCMAKE_INSTALL_PREFIX="$FLOWIE_SDK_ROOT/turboutils"; \
    cmake --build --preset linux-release-user --parallel "$parallel"; \
    cmake --install build/linux-gcc-release

COPY . /src/turbo-flow

RUN --mount=type=cache,target=/var/cache/vcpkg,sharing=locked \
    set -eux; \
    parallel="$(nproc)"; \
    cd /src/turbonet; \
    cmake --fresh --preset linux-release-user \
      -DTURBO_UTILS_ROOT="$FLOWIE_SDK_ROOT/turboutils" \
      -DCMAKE_PREFIX_PATH="$FLOWIE_SDK_ROOT/turboutils;/src/turbonet/vcpkg_installed/x64-linux" \
      -DCMAKE_INSTALL_PREFIX="$FLOWIE_SDK_ROOT/turbonet"; \
    cmake --build --preset linux-release-user --parallel "$parallel"; \
    cmake --install build/linux-gcc-release

RUN --mount=type=cache,target=/var/cache/vcpkg,sharing=locked \
    set -eux; \
    parallel="$(nproc)"; \
    cd /src/TurboHTTP; \
    cmake --fresh --preset linux-release-user \
      -DTURBO_UTILS_ROOT="$FLOWIE_SDK_ROOT/turboutils" \
      -DTURBO_NET_ROOT="$FLOWIE_SDK_ROOT/turbonet" \
      -DCMAKE_PREFIX_PATH="$FLOWIE_SDK_ROOT/turboutils;$FLOWIE_SDK_ROOT/turbonet;/src/TurboHTTP/vcpkg_installed/x64-linux" \
      -DCMAKE_INSTALL_PREFIX="$FLOWIE_SDK_ROOT/turbohttp"; \
    cmake --build --preset linux-release-user --parallel "$parallel"; \
    cmake --install build/linux-gcc-release

RUN --mount=type=cache,target=/var/cache/vcpkg,sharing=locked \
    set -eux; \
    parallel="$(nproc)"; \
    cd /src/rulesforge; \
    cmake --fresh --preset linux-release-user \
      -DTURBO_UTILS_ROOT="$FLOWIE_SDK_ROOT/turboutils" \
      -DCMAKE_PREFIX_PATH="$FLOWIE_SDK_ROOT/turboutils;/src/rulesforge/vcpkg_installed/x64-linux" \
      -DCMAKE_INSTALL_PREFIX="$FLOWIE_SDK_ROOT/rulesforge"; \
    cmake --build --preset linux-release-user --parallel "$parallel"; \
    cmake --install build/linux-gcc-release

RUN --mount=type=cache,target=/var/cache/vcpkg,sharing=locked \
    set -eux; \
    parallel="$(nproc)"; \
    cd /src/turbo-flow; \
    cmake --fresh --preset linux-release-user \
      -DTURBO_UTILS_ROOT="$FLOWIE_SDK_ROOT/turboutils" \
      -DTURBO_NET_ROOT="$FLOWIE_SDK_ROOT/turbonet" \
      -DTURBO_HTTP_ROOT="$FLOWIE_SDK_ROOT/turbohttp" \
      -DCMAKE_PREFIX_PATH="$FLOWIE_SDK_ROOT/turboutils;$FLOWIE_SDK_ROOT/turbonet;$FLOWIE_SDK_ROOT/turbohttp;$FLOWIE_SDK_ROOT/rulesforge;/src/turbo-flow/vcpkg_installed/x64-linux" \
      -DCMAKE_INSTALL_PREFIX="$FLOWIE_SDK_ROOT/turboflow" \
      -DBUILD_TESTING=OFF \
      -DENABLE_TESTS=OFF \
      -DBUILD_TESTS=OFF \
      -DBUILD_EXAMPLES=OFF \
      -DFLOWIE_CONTROL_DASHBOARD_RESOURCE_DIR=/opt/flowie/share/control/dashboard-resources \
      -DFLOWIE_RELEASE_REVISION="${SOURCE_REVISION}"; \
    cmake --build --preset linux-release-user --parallel "$parallel"; \
    cmake --install build/linux-gcc-release; \
    install -d /opt/flowie-runtime/bin /opt/flowie-runtime/lib; \
    install -m 0755 "$FLOWIE_SDK_ROOT/turboflow/bin/flowie_server" \
      /opt/flowie-runtime/bin/flowie_server; \
    for libdir in \
      "$FLOWIE_SDK_ROOT/turboutils/lib" \
      "$FLOWIE_SDK_ROOT/turbonet/lib" \
      "$FLOWIE_SDK_ROOT/turbohttp/lib" \
      "$FLOWIE_SDK_ROOT/rulesforge/lib" \
      "$FLOWIE_SDK_ROOT/turboflow/lib" \
      /src/turbo-utils/vcpkg_installed/x64-linux/lib \
      /src/turbonet/vcpkg_installed/x64-linux/lib \
      /src/TurboHTTP/vcpkg_installed/x64-linux/lib \
      /src/rulesforge/vcpkg_installed/x64-linux/lib \
      /src/turbo-flow/vcpkg_installed/x64-linux/lib; do \
      if [ -d "$libdir" ]; then \
        find "$libdir" -maxdepth 1 \( -type f -o -type l \) -name '*.so*' \
          -exec cp -a '{}' /opt/flowie-runtime/lib/ ';'; \
      fi; \
    done; \
    LD_LIBRARY_PATH=/opt/flowie-runtime/lib ldd /opt/flowie-runtime/bin/flowie_server \
      | tee /opt/flowie-runtime/flowie-server.ldd; \
    ! grep -F 'not found' /opt/flowie-runtime/flowie-server.ldd

FROM debian:bookworm-slim AS runtime

ARG SOURCE_REVISION=unknown

LABEL org.opencontainers.image.title="Flowie server" \
      org.opencontainers.image.description="Flowie MQTT and embedded Control server" \
      org.opencontainers.image.revision="${SOURCE_REVISION}"

RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates \
      libgcc-s1 \
      libstdc++6 \
      netcat-openbsd \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 flowie \
    && useradd --uid 10001 --gid flowie --no-create-home --home-dir /var/lib/flowie \
         --shell /usr/sbin/nologin flowie \
    && install -d -o flowie -g flowie \
         /etc/flowie /etc/flowie/certs /opt/flowie/plugins \
         /var/lib/flowie/control /var/lib/flowie/protocol

COPY --from=builder /opt/flowie-runtime/bin/flowie_server /usr/local/bin/flowie_server
COPY --from=builder /opt/flowie-runtime/lib/ /usr/local/lib/
COPY --from=builder /opt/flowie/share/ /opt/flowie/share/
COPY --from=builder /opt/flowie-runtime/flowie-server.ldd /usr/share/doc/flowie/flowie-server.ldd
COPY --chmod=0755 flowie/deploy/server/docker-entrypoint.sh /usr/local/bin/docker-entrypoint.sh
COPY --chmod=0755 flowie/deploy/server/healthcheck.sh /usr/local/bin/flowie-healthcheck

RUN ldconfig \
    && ldd /usr/local/bin/flowie_server | tee /usr/share/doc/flowie/flowie-server-runtime.ldd \
    && ! grep -F 'not found' /usr/share/doc/flowie/flowie-server-runtime.ldd

ENV FLOWIE_CONFIG=/etc/flowie/flowie.yml \
    FLOWIE_GRAPH=/etc/flowie/flowie.flow \
    FLOWIE_CONTROL_CONFIG=/etc/flowie/control.yml \
    FLOWIE_PROTOCOL_STORE_PATH=/var/lib/flowie/protocol/flowie-protocol.sqlite3 \
    FLOWIE_PROFILE=flowie \
    FLOWIE_HEALTH_HOST=127.0.0.1 \
    FLOWIE_HEALTH_PORT=18883

VOLUME ["/var/lib/flowie"]
WORKDIR /var/lib/flowie
USER 10001:10001

HEALTHCHECK --interval=30s --timeout=5s --start-period=15s --retries=3 \
  CMD ["/usr/local/bin/flowie-healthcheck"]

ENTRYPOINT ["/usr/local/bin/docker-entrypoint.sh"]
