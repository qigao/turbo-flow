# Flowie Linux 远程测试 Runbook

本文用于从 Windows 工作站打包当前工作树，将 TurboUtils、TurboNet、TurboHTTP 和 TurboFlow 上传到
`root@eu:/root/dev`，在隔离目录中构建四个仓库，启动 Redis、PostgreSQL 与固定 Mosquitto Docker
服务，并运行 Flowie MQTT release/nightly cases。

## 1. 执行边界

- Windows 工作树是本次源码事实源；打包包含未提交文件，但排除 `.git/`、构建树、vcpkg 安装树、
  CodeGraph 索引和 `.env`。
- 远端每次使用 `/root/dev/runs/<run-id>`；SDK 安装到该 run 目录，不覆盖 `/opt` 下已有包。
- Docker 端口只绑定 `127.0.0.1`。数据库凭据只用于本次临时 PostgreSQL 容器。
- 所有命令 fail fast。不要用发布必需用例的跳过、Disabled 或仅编译结果替代测试成功；
  可选 public broker smoke 默认 Disabled。
- release 与 nightly 分开：release 使用 GCC；libFuzzer/nightly 使用 Clang 独立构建树。
- PostgreSQL、Redis 和固定 broker 是独立发布证据；可选 public broker smoke 只验证公网访问能力。
  发布证据失败时保留本次 run 目录和日志。

## 2. Windows：打包并上传当前源码

在 PowerShell 中执行。四个源码目录应分别为：

- `C:\projects\cpp\turbonet\turbo-utils`
- `C:\projects\cpp\turbonet\turbonet`
- `C:\projects\cpp\TurboHTTP`
- `C:\projects\cpp\turbonet\turbo-flow`

```powershell
$ErrorActionPreference = 'Stop'
$sourceRoot = 'C:\projects\cpp'
$artifactRoot = Join-Path $sourceRoot 'artifacts'
$stamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$bundleBase = "turbo-stack-$stamp"
$bundleName = "$bundleBase.zip"
$bundle = Join-Path $artifactRoot $bundleName
$manifestName = "$bundleBase.revisions.txt"
$manifest = Join-Path $artifactRoot $manifestName
$checksum = "$bundle.sha256"

New-Item -ItemType Directory -Force -Path $artifactRoot | Out-Null

$repositories = [ordered]@{
    'turbo-utils' = Join-Path $sourceRoot 'turbonet\turbo-utils'
    'turbonet'    = Join-Path $sourceRoot 'turbonet\turbonet'
    'TurboHTTP'   = Join-Path $sourceRoot 'TurboHTTP'
    'turbo-flow'  = Join-Path $sourceRoot 'turbonet\turbo-flow'
}

$revisionLines = foreach ($entry in $repositories.GetEnumerator()) {
    if (-not (Test-Path -LiteralPath (Join-Path $entry.Value 'CMakePresets.json'))) {
        throw "missing repository: $($entry.Value)"
    }
    $revision = git -C $entry.Value rev-parse HEAD
    if ($LASTEXITCODE -ne 0) { throw "git revision failed: $($entry.Value)" }
    $dirtyEntries = @(git -C $entry.Value status --porcelain=v1 --untracked-files=all)
    if ($LASTEXITCODE -ne 0) { throw "git status failed: $($entry.Value)" }
    "$($entry.Key) commit=$revision dirty=$([int]($dirtyEntries.Count -gt 0)) entries=$($dirtyEntries.Count)"
}
$revisionLines | Set-Content -LiteralPath $manifest -Encoding utf8

tar.exe -a -cf $bundle `
    --exclude='.git' --exclude='*/.git' `
    --exclude='.codegraph' --exclude='*/.codegraph' `
    --exclude='turbonet/turbo-utils/build' `
    --exclude='turbonet/turbo-utils/vcpkg_installed' `
    --exclude='turbonet/turbonet/build' `
    --exclude='turbonet/turbonet/vcpkg_installed' `
    --exclude='TurboHTTP/build' `
    --exclude='TurboHTTP/vcpkg_installed' `
    --exclude='turbonet/turbo-flow/build' `
    --exclude='turbonet/turbo-flow/vcpkg_installed' `
    --exclude='.env' --exclude='.env.*' --exclude='*.log' `
    -C $sourceRoot `
    'turbonet/turbo-utils' 'turbonet/turbonet' 'TurboHTTP' 'turbonet/turbo-flow' `
    -C $artifactRoot $manifestName
if ($LASTEXITCODE -ne 0) { throw 'source archive failed' }

$hash = (Get-FileHash -Algorithm SHA256 -LiteralPath $bundle).Hash.ToLowerInvariant()
[IO.File]::WriteAllText($checksum, "$hash  $bundleName`n", [Text.Encoding]::ASCII)

ssh root@eu 'install -d -m 0700 /root/dev/incoming /root/dev/runs'
if ($LASTEXITCODE -ne 0) { throw 'remote directory preparation failed' }
scp $bundle $checksum root@eu:/root/dev/incoming/
if ($LASTEXITCODE -ne 0) { throw 'source upload failed' }
ssh root@eu "printf '%s\n' '$bundleName' > /root/dev/incoming/latest-bundle"
if ($LASTEXITCODE -ne 0) { throw 'remote bundle marker failed' }

Write-Host "uploaded=$bundleName sha256=$hash"
```

`tar.exe` 直接读取当前工作树，因此不会漏掉尚未 commit 的测试实现。归档前仍应人工确认没有真实密钥、
生产 `.env` 或数据库 dump 位于源码目录。

## 3. Linux：进入持久会话并准备 run 目录

```bash
ssh root@eu
cd /root/dev
tmux new-session -s flowie-eu
```

后续命令在同一个 `tmux` shell 中执行。需要断开 SSH 时按 `Ctrl-b d`；重新进入使用
`tmux attach-session -t flowie-eu`。

Debian/Ubuntu 主机先准备基线工具；`/opt/vcpkg` 必须由运维预装并固定到团队批准的 revision：

```bash
apt-get update
apt-get install -y build-essential clang cmake ninja-build git curl zip unzip pkg-config \
  openssl ca-certificates tmux docker.io docker-compose-plugin mosquitto-clients libpq-dev
systemctl start docker
test -x /opt/vcpkg/vcpkg
```

```bash
set -Eeuo pipefail
umask 077

BUNDLE_NAME="$(cat /root/dev/incoming/latest-bundle)"
case "$BUNDLE_NAME" in
  turbo-stack-*.zip) ;;
  *) echo "invalid bundle name: $BUNDLE_NAME" >&2; exit 1 ;;
esac

cd /root/dev/incoming
sha256sum -c "$BUNDLE_NAME.sha256"

RUN_ID="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ROOT="/root/dev/runs/$RUN_ID"
SRC_ROOT="$RUN_ROOT/src"
SDK_ROOT="$RUN_ROOT/pkgs"
ARTIFACT_ROOT="$RUN_ROOT/artifacts"
BUNDLE="/root/dev/incoming/$BUNDLE_NAME"
SOURCE_REVISION="$(sha256sum "$BUNDLE" | awk '{print $1}')"

install -d -m 0700 "$SRC_ROOT" "$SDK_ROOT" "$ARTIFACT_ROOT"
unzip -q "$BUNDLE" -d "$SRC_ROOT"

TURBO_UTILS_SRC="$SRC_ROOT/turbonet/turbo-utils"
TURBO_NET_SRC="$SRC_ROOT/turbonet/turbonet"
TURBO_HTTP_SRC="$SRC_ROOT/TurboHTTP"
TURBO_FLOW_SRC="$SRC_ROOT/turbonet/turbo-flow"

for source_dir in "$TURBO_UTILS_SRC" "$TURBO_NET_SRC" "$TURBO_HTTP_SRC" "$TURBO_FLOW_SRC"; do
  test -f "$source_dir/CMakePresets.json"
  test -f "$source_dir/CMakeUserPresets.json"
done

{
  date -u --iso-8601=seconds
  uname -a
  cmake --version
  ninja --version
  gcc --version | head -n 1
  clang --version | head -n 1
  docker --version
  docker compose version
  /opt/vcpkg/vcpkg version
  printf 'source_revision=%s\n' "$SOURCE_REVISION"
} | tee "$ARTIFACT_ROOT/environment.txt"
```

必需工具为 CMake、Ninja、GCC/G++、Clang、Docker Compose、OpenSSL、`unzip`、`tmux`、
`mosquitto_pub/sub` 和 `/opt/vcpkg`。缺失时先安装并重新执行本节；不要在工具缺失状态继续。

## 4. 启动 Redis、PostgreSQL 与固定 Mosquitto

先确认测试端口没有被其他服务占用。发生冲突时停止并确认占用者，不自动复用未知服务。

```bash
for port in 1883 5432 6379 8083 8084 8883; do
  if ss -ltnH | awk '{print $4}' | grep -Eq "[:.]${port}$"; then
    echo "port already in use: $port" >&2
    exit 1
  fi
done

REDIS_IMAGE="redis:8-alpine"
PG_IMAGE="postgres:17-alpine"
REDIS_CONTAINER="flowie-redis-$RUN_ID"
PG_CONTAINER="flowie-pg-$RUN_ID"

docker pull "$REDIS_IMAGE"
docker pull "$PG_IMAGE"

docker run -d --name "$REDIS_CONTAINER" \
  --label "io.turboflow.test-run=$RUN_ID" \
  -p 127.0.0.1:6379:6379 \
  --health-cmd='redis-cli ping || exit 1' --health-interval=2s --health-timeout=2s \
  --health-retries=30 \
  "$REDIS_IMAGE" redis-server --save '' --appendonly no

docker run -d --name "$PG_CONTAINER" \
  --label "io.turboflow.test-run=$RUN_ID" \
  -p 127.0.0.1:5432:5432 \
  -e POSTGRES_USER=turboflow -e POSTGRES_PASSWORD=change-me -e POSTGRES_DB=turboflow \
  --health-cmd='pg_isready -U turboflow -d turboflow' \
  --health-interval=2s --health-timeout=3s --health-retries=30 \
  "$PG_IMAGE"

wait_healthy() {
  local container="$1"
  local state
  for _ in $(seq 1 60); do
    state="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}missing{{end}}' "$container")"
    case "$state" in
      healthy) return 0 ;;
      unhealthy) docker logs "$container"; return 1 ;;
    esac
    sleep 2
  done
  docker logs "$container"
  return 1
}

wait_healthy "$REDIS_CONTAINER"
wait_healthy "$PG_CONTAINER"
test "$(docker exec "$REDIS_CONTAINER" redis-cli ping)" = PONG
docker exec "$PG_CONTAINER" psql -U turboflow -d turboflow -v ON_ERROR_STOP=1 \
  -c 'SELECT current_database(), current_user;'

REDIS_VERSION="$(docker exec "$REDIS_CONTAINER" redis-server --version | \
  sed -n 's/.*v=\([^ ]*\).*/\1/p')"
test -n "$REDIS_VERSION"
export TURBO_FLOW_PGSQL_TEST_CONNINFO='host=127.0.0.1 port=5432 dbname=turboflow user=turboflow password=change-me connect_timeout=3'

CERT_DIR="$RUN_ROOT/certs/mosquitto"
install -d -m 0755 "$CERT_DIR"
openssl req -x509 -newkey rsa:2048 -nodes -days 2 \
  -subj '/CN=Flowie MQTT Test CA' \
  -keyout "$CERT_DIR/ca.key" -out "$CERT_DIR/ca.pem"
openssl req -newkey rsa:2048 -nodes -subj '/CN=localhost' \
  -keyout "$CERT_DIR/server.key" -out "$CERT_DIR/server.csr"
openssl x509 -req -days 2 -in "$CERT_DIR/server.csr" \
  -CA "$CERT_DIR/ca.pem" -CAkey "$CERT_DIR/ca.key" -CAcreateserial \
  -out "$CERT_DIR/server.pem" \
  -extfile "$TURBO_FLOW_SRC/flowie/interop/mosquitto-2.0.22/server.ext"
chmod 0644 "$CERT_DIR/ca.pem" "$CERT_DIR/server.pem" "$CERT_DIR/server.key"

export FLOWIE_MQTT_INTEROP_CERT_DIR="$CERT_DIR"
export COMPOSE_PROJECT_NAME="flowie${RUN_ID,,}"
docker compose -f "$TURBO_FLOW_SRC/flowie/interop/mosquitto-2.0.22/compose.yml" \
  up -d --wait

docker inspect "$REDIS_CONTAINER" "$PG_CONTAINER" > "$ARTIFACT_ROOT/docker-containers.json"
docker image inspect "$REDIS_IMAGE" "$PG_IMAGE" eclipse-mosquitto:2.0.22 \
  > "$ARTIFACT_ROOT/docker-images.json"
```

仅在显式启用可选 public MQTT smoke 时检查以下出站端口；失败不影响 release gate：

```bash
if [[ "${FLOWIE_RUN_PUBLIC_SMOKE:-0}" == "1" ]]; then
  for endpoint in \
    broker.hivemq.com:1883 broker.hivemq.com:8000 \
    broker.emqx.io:1883 broker.emqx.io:8883 broker.emqx.io:8083 broker.emqx.io:8084; do
    host="${endpoint%:*}"
    port="${endpoint##*:}"
    timeout 5 bash -c "</dev/tcp/$host/$port"
  done
fi
```

## 5. 构建、测试并安装依赖 SDK

顺序固定为 TurboUtils → TurboNet → TurboHTTP。每个仓库先完成 Linux release CTest，再安装到本次
run 的私有 SDK 目录。

```bash
cd "$TURBO_UTILS_SRC"
cmake --list-presets
cmake --fresh --preset linux-release-user \
  -DCMAKE_INSTALL_PREFIX="$SDK_ROOT/turboutils"
cmake --build --preset linux-release-user --parallel "$(nproc)"
ctest --preset linux-release-user --output-on-failure \
  --output-junit "$ARTIFACT_ROOT/turboutils-linux-release.xml"
cmake --install build/linux-gcc-release

cd "$TURBO_NET_SRC"
cmake --list-presets
cmake --fresh --preset linux-release-user \
  -DTURBO_UTILS_ROOT="$SDK_ROOT/turboutils" \
  -DCMAKE_PREFIX_PATH="$SDK_ROOT/turboutils;$TURBO_NET_SRC/vcpkg_installed/x64-linux" \
  -DCMAKE_INSTALL_PREFIX="$SDK_ROOT/turbonet"
cmake --build --preset linux-release-user --parallel "$(nproc)"
ctest --preset linux-release-user --output-on-failure \
  --output-junit "$ARTIFACT_ROOT/turbonet-linux-release.xml"
cmake --install build/linux-gcc-release

# Keep the private SDK ahead of host-installed TurboUtils/TurboNet libraries.
export LD_LIBRARY_PATH="$SDK_ROOT/turbonet/lib:$SDK_ROOT/turboutils/lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"

cd "$TURBO_HTTP_SRC"
cmake --list-presets
cmake --fresh --preset linux-release-user \
  -DTURBO_UTILS_ROOT="$SDK_ROOT/turboutils" \
  -DTURBO_NET_ROOT="$SDK_ROOT/turbonet" \
  -DCMAKE_PREFIX_PATH="$SDK_ROOT/turboutils;$SDK_ROOT/turbonet;$TURBO_HTTP_SRC/vcpkg_installed/x64-linux" \
  -DCMAKE_INSTALL_PREFIX="$SDK_ROOT/turbohttp"
cmake --build --preset linux-release-user --parallel "$(nproc)"
ctest --preset linux-release-user --output-on-failure \
  --output-junit "$ARTIFACT_ROOT/turbohttp-linux-release.xml"
cmake --install build/linux-gcc-release

export LD_LIBRARY_PATH="$SDK_ROOT/turbohttp/lib:$LD_LIBRARY_PATH"
```

命令行显式覆盖 TurboNet/TurboFlow Linux preset 中继承的安装前缀，防止它们误写
`/opt/turboutils`。`LD_LIBRARY_PATH` 必须保持本次 run 的 SDK 在主机系统路径之前，避免
`/usr/local/lib` 中旧版本库满足同名 SONAME 后造成 ABI 符号错配。

## 6. 配置并构建 TurboFlow release gate

```bash
cd "$TURBO_FLOW_SRC"
cmake --list-presets
cmake --build --list-presets
ctest --list-presets

cmake --fresh --preset linux-release-user \
  -DTURBO_UTILS_ROOT="$SDK_ROOT/turboutils" \
  -DTURBO_NET_ROOT="$SDK_ROOT/turbonet" \
  -DTURBO_HTTP_ROOT="$SDK_ROOT/turbohttp" \
  -DCMAKE_PREFIX_PATH="$SDK_ROOT/turboutils;$SDK_ROOT/turbonet;$SDK_ROOT/turbohttp;$TURBO_FLOW_SRC/vcpkg_installed/x64-linux" \
  -DCMAKE_INSTALL_PREFIX="$SDK_ROOT/turboflow" \
  -DFLOWIE_MQTT_RELEASE_GATE=ON \
  -DFLOWIE_MQTT_PUBLIC_LIVE_TESTS=OFF \
  -DFLOWIE_MQTT_FIXED_INTEROP_TESTS=ON \
  -DFLOWIE_MQTT_FIXED_CA_FILE="$CERT_DIR/ca.pem" \
  -DFLOWIE_MQTT_FIXED_SUPPORT_31=ON \
  -DFLOWIE_MQTT_FIXED_SUPPORT_31_WS=ON \
  -DFLOWIE_MQTT_SOAK_TESTS=ON \
  -DFLOWIE_MQTT_FUZZ_TARGETS=OFF \
  -DTURBO_FLOW_REDIS_LIVE_TESTS=ON \
  -DTURBO_FLOW_PGSQL_LIVE_TESTS=ON \
  -DFLOWIE_RELEASE_REVISION="$SOURCE_REVISION" \
  -DFLOWIE_REDIS_SERVER_VERSION="$REDIS_VERSION"

cmake --build --preset linux-release-user --parallel "$(nproc)" \
  2>&1 | tee "$ARTIFACT_ROOT/turboflow-linux-release-build.log"
```

## 7. 分层运行 cases

以下顺序先验证外部依赖，再运行 Flowie release label、全量 CTest 和 release evidence。任何一步失败即
停止，不继续用后续结果掩盖失败。

```bash
cd "$TURBO_FLOW_SRC"

ctest --preset linux-release-user -N \
  2>&1 | tee "$ARTIFACT_ROOT/turboflow-linux-tests.txt"

ctest --preset linux-release-user --output-on-failure \
  -R '^(test_turbo_flow_redis_live|test_turbo_flow_pgsql_live|flowie_server_check_redis_session_store|flowie_server_check_smb_product)$' \
  --output-junit "$ARTIFACT_ROOT/storage-live.xml"

ctest --preset linux-release-user --output-on-failure \
  -L 'mqtt-fixed-interop' \
  --output-junit "$ARTIFACT_ROOT/fixed-interop.xml"

ctest --preset linux-release-user --output-on-failure \
  -L 'flowie-release' \
  --output-junit "$ARTIFACT_ROOT/flowie-release.xml"

ctest --preset linux-release-user --output-on-failure \
  --output-junit "$ARTIFACT_ROOT/turboflow-linux-release.xml"

cmake --build --preset linux-release-user --target flowie_release_evidence \
  2>&1 | tee "$ARTIFACT_ROOT/flowie-release-evidence.log"

test -f build/linux-gcc-release/flowie-release-evidence.json
cp build/linux-gcc-release/flowie-release-evidence.json "$ARTIFACT_ROOT/"
cmake --install build/linux-gcc-release
```

本阶段的 `test_flowie_mqtt_soak` 使用默认短时验证。规定的 30/60 分钟 soak 只由下一节 nightly
evidence 生成。

## 8. Clang sanitizer、fuzz 与 30/60 分钟 nightly

本节约需四小时，应保持在 `tmux` 中。它使用 `linux-dev-user` 的独立构建树，并显式选择 Clang；不能在
GCC release build 中开启 `FLOWIE_MQTT_FUZZ_TARGETS`。

```bash
cd "$TURBO_FLOW_SRC"

cmake --fresh --preset linux-dev-user \
  -DCMAKE_C_COMPILER=clang \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DTURBO_UTILS_ROOT="$SDK_ROOT/turboutils" \
  -DTURBO_NET_ROOT="$SDK_ROOT/turbonet" \
  -DTURBO_HTTP_ROOT="$SDK_ROOT/turbohttp" \
  -DCMAKE_PREFIX_PATH="$SDK_ROOT/turboutils;$SDK_ROOT/turbonet;$SDK_ROOT/turbohttp;$TURBO_FLOW_SRC/vcpkg_installed/x64-linux/debug" \
  -DCMAKE_INSTALL_PREFIX="$RUN_ROOT/pkgs/turboflow-clang" \
  -DFLOWIE_MQTT_RELEASE_GATE=OFF \
  -DFLOWIE_MQTT_PUBLIC_LIVE_TESTS=OFF \
  -DFLOWIE_MQTT_FIXED_INTEROP_TESTS=OFF \
  -DFLOWIE_MQTT_SOAK_TESTS=ON \
  -DFLOWIE_MQTT_FUZZ_TARGETS=ON \
  -DTURBO_FLOW_REDIS_LIVE_TESTS=ON \
  -DTURBO_FLOW_PGSQL_LIVE_TESTS=ON \
  -DFLOWIE_RELEASE_REVISION="$SOURCE_REVISION" \
  -DFLOWIE_REDIS_SERVER_VERSION="$REDIS_VERSION"

cmake --build --preset linux-dev-user --target flowie_nightly_evidence \
  --parallel "$(nproc)" 2>&1 | tee "$ARTIFACT_ROOT/flowie-nightly-evidence.log"

test -f build/linux-gcc-debug/flowie-nightly-evidence.json
cp build/linux-gcc-debug/flowie-nightly-evidence.json "$ARTIFACT_ROOT/"
cp -a build/linux-gcc-debug/flowie-fuzz-artifacts "$ARTIFACT_ROOT/"
```

nightly target 固定要求：同一 `SOURCE_REVISION`、非零 seed、corpus/soak/sanitizer 全部 PASS、六项
`resource_monotonic_growth=false`。任一条件缺失会由 verifier 直接失败。

## 9. 收集结果并下载

```bash
cd "$RUN_ROOT"
docker logs "$REDIS_CONTAINER" > "$ARTIFACT_ROOT/redis.log" 2>&1
docker logs "$PG_CONTAINER" > "$ARTIFACT_ROOT/postgresql.log" 2>&1
docker compose -f "$TURBO_FLOW_SRC/flowie/interop/mosquitto-2.0.22/compose.yml" \
  logs --no-color > "$ARTIFACT_ROOT/mosquitto.log" 2>&1

RESULT_NAME="flowie-linux-results-$RUN_ID.zip"
zip -qr "/root/dev/incoming/$RESULT_NAME" artifacts
sha256sum "/root/dev/incoming/$RESULT_NAME" \
  > "/root/dev/incoming/$RESULT_NAME.sha256"
printf '%s\n' "$RESULT_NAME" > /root/dev/incoming/latest-result
```

在 Windows PowerShell 下载并验证：

```powershell
$resultName = ssh root@eu 'cat /root/dev/incoming/latest-result'
$downloadRoot = 'C:\projects\cpp\artifacts\eu-results'
New-Item -ItemType Directory -Force -Path $downloadRoot | Out-Null
scp "root@eu:/root/dev/incoming/$resultName" "$downloadRoot/"
scp "root@eu:/root/dev/incoming/$resultName.sha256" "$downloadRoot/"
Push-Location $downloadRoot
try {
    $actual = (Get-FileHash -Algorithm SHA256 -LiteralPath $resultName).Hash.ToLowerInvariant()
    $expected = ((Get-Content -Raw -LiteralPath "$resultName.sha256").Trim() -split '\s+')[0]
    if ($actual -ne $expected) { throw "result checksum mismatch: $resultName" }
    Write-Host "verified=$resultName sha256=$actual"
} finally {
    Pop-Location
}
```

## 10. 精确清理 Docker 资源

只在日志与结果包生成后执行。以下保护确保仅删除本次 run 的容器和 compose project；源码、SDK 和
证据仍保留在 `$RUN_ROOT`。

```bash
case "$REDIS_CONTAINER" in flowie-redis-*) ;; *) exit 1 ;; esac
case "$PG_CONTAINER" in flowie-pg-*) ;; *) exit 1 ;; esac
case "$COMPOSE_PROJECT_NAME" in flowie*) ;; *) exit 1 ;; esac

docker compose -f "$TURBO_FLOW_SRC/flowie/interop/mosquitto-2.0.22/compose.yml" \
  down -v
docker rm -f "$REDIS_CONTAINER" "$PG_CONTAINER"
```

## 11. 成功判定

一次完整 Linux 结果必须同时满足：

- TurboUtils、TurboNet、TurboHTTP 与 TurboFlow configure/build 成功。
- 四个 release JUnit 结果无失败，TurboFlow 全量 CTest 不是零用例。
- Redis live、PostgreSQL live、固定 Mosquitto、TLS/WSS/mTLS 均有实际 PASS。
- `flowie-release-evidence.json` 通过内置 verifier。
- nightly 的 corpus、六项 30/60 分钟 soak 和 Clang libFuzzer 全部 PASS，且无资源单调增长。
- 结果记录同一个源码归档 SHA-256；Linux 结果不得从 Windows 结果推定。

失败时保留 `$RUN_ROOT`、容器日志、JUnit、evidence JSON、seed 和 fuzz artifacts。先运行失败输出中给出的
单个 CTest 名称或 TinyTest filter 复现；确认根因后重新打包新的源码归档，不在远端源码上做无记录修改。
