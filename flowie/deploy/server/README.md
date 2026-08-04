# Flowie server container

该部署把 standalone MQTT server 和 embedded Control 放在同一个非 root 运行镜像；cluster 是独立
产品和容器，不由本镜像启动。`compose.yml` 只运行已构建镜像，不包含源码构建配置。镜像通过
`docker buildx build` 向 Dockerfile 传入 BuildKit named contexts；Dockerfile 在独立 stage 中安装
TurboUtils、TurboNet、TurboHTTP、RulesForge 私有 SDK，最终 stage 只编译 TurboFlow。运行容器不依赖
宿主机 SDK 或源码。

## 部署边界

- server 与同机 HAProxy/Nginx 继续使用 Linux host network。默认 MQTT backend listener 是
  `127.0.0.1:18883`，Control listener 是配置文件中的 `127.0.0.1:8443`。
- `/etc/flowie`、`/etc/flowie/certs` 和 `/opt/flowie/plugins` 只读；Control SQLite 和明确配置的业务
  数据可写入命名卷 `/var/lib/flowie`。MQTT ProtocolStore 固定为 SQLite `:memory:`，不写入该卷。
- 入口始终传入 `--require-security`，并要求 `FLOWIE_PROTOCOL_STORE_PATH=:memory:`。缺少 server
  config、graph、Control config，或尝试把协议存储改为文件路径时，容器直接失败。
- 容器以 UID/GID `10001` 运行，根文件系统只读，移除全部 Linux capabilities，并启用
  `no-new-privileges`。

## 构建上下文

默认目录布局如下：

```text
cpp/
  TurboHTTP/
  rulesforge/
  turbonet/
    turbo-utils/
    turbonet/
    turbo-flow/
```

从 `turbo-flow` Repository 根目录构建本地镜像：

```sh
export FLOWIE_SOURCE_REVISION="$(git rev-parse HEAD)"
export FLOWIE_SERVER_IMAGE="flowie-server:local"

docker buildx build \
  --file flowie/deploy/server/Dockerfile \
  --build-context turbo_utils=../turbo-utils \
  --build-context turbo_net=../turbonet \
  --build-context turbo_http=../../TurboHTTP \
  --build-context rules_forge=../../rulesforge \
  --build-arg "SOURCE_REVISION=${FLOWIE_SOURCE_REVISION}" \
  --tag "${FLOWIE_SERVER_IMAGE}" \
  --load \
  .
```

CI 发布镜像时使用 registry tag 并把 `--load` 替换为 `--push`。发布后应记录 Buildx 输出的 digest，并让
部署环境的 `FLOWIE_SERVER_IMAGE` 引用该 digest，而不是浮动 tag。

在本目录创建不入库的 `.env`，以 `.env.example` 为起点设置配置、graph、证书和插件目录。证书路径应与
`control.yml` 以及 server 配置中的绝对容器路径一致，例如 `/etc/flowie/certs/server.pem`。
`FLOWIE_SECRET_ENV_FILE` 必须指向一个权限为 `0600`、不入库的 env 文件，内容提供配置中所有
`env://NAME` 引用，例如 `FLOWIE_AUTH_SERVICE_TOKEN=...`。该文件即使当前为空也必须存在，使缺少 secret
注入边界在 Compose 展开阶段失败，而不是启动后隐式降级。

```sh
mkdir -p config certs plugins
install -m 0600 /dev/null secrets.env
docker compose config
docker compose up -d --no-build flowie-server
docker compose ps
docker compose logs --tail=200 flowie-server
```

生产部署不执行上面的构建命令，而是由 CI 构建并推送 `FLOWIE_SERVER_IMAGE`，最好将它设置为 registry
digest。部署主机只需要运行以下命令，不需要五个源码仓库或编译工具链：

```sh
docker compose config
docker compose pull flowie-server
docker compose up -d --no-build flowie-server
```

`FLOWIE_SOURCE_REVISION` 必须在构建时设置为实际 TurboFlow commit。五个仓库的 manifest 都固定同一
vcpkg baseline；Dockerfile 使用固定 commit 的 vcpkg 工具，避免使用浮动 `master`。

### SDK 构建缓存

依赖 SDK stage 只复制自身源码和它实际依赖的上游 SDK。TurboUtils stage 还封装完整产品构建所需的
DataBind `tbe_compiler` 及其模板资源，最终 TurboFlow stage 通过显式 CMake 路径消费该宿主工具。TurboFlow 源码或
`FLOWIE_SOURCE_REVISION` 变化时，TurboUtils、TurboNet、TurboHTTP 与 RulesForge stage 应命中
BuildKit cache；TurboNet 变化只会使 TurboNet、TurboHTTP 和最终 TurboFlow stage 失效。生产构建把
依赖仓库的测试/示例选项设为关闭且不执行测试；若上游 `install` target 仍依赖其 `all` target，首次 SDK
构建可能继续编译部分测试程序。各仓库的实际验证仍由其 preset/CI 独立执行。

连续运行两次相同构建可检查缓存是否生效：

```sh
export FLOWIE_SOURCE_REVISION="$(git rev-parse HEAD)"
export FLOWIE_SERVER_IMAGE="flowie-server:local"

build_flowie_server() {
  docker buildx build \
    --file flowie/deploy/server/Dockerfile \
    --build-context turbo_utils=../turbo-utils \
    --build-context turbo_net=../turbonet \
    --build-context turbo_http=../../TurboHTTP \
    --build-context rules_forge=../../rulesforge \
    --build-arg "SOURCE_REVISION=${FLOWIE_SOURCE_REVISION}" \
    --tag "${FLOWIE_SERVER_IMAGE}" \
    --progress=plain \
    --load \
    .
}

build_flowie_server 2>&1 | tee first-build.log
build_flowie_server 2>&1 | tee cached-build.log
grep -E 'turboutils_builder|turbonet_builder|turbohttp_builder|rulesforge_builder|CACHED' \
  cached-build.log
```

不要从宿主机 `/usr/local/lib` 复制现成 `.so`。它缺少同一构建链的 headers、CMake package 和 ABI
来源信息。跨构建主机复用时，应把上述 SDK stages 发布为按依赖 commit 固定、可按 digest 引用的 SDK
基础镜像，或配置 BuildKit registry cache；不能使用浮动 tag 作为发布事实源。

## 健康检查

健康检查同时确认 PID 1 存活，并对 `FLOWIE_HEALTH_HOST:FLOWIE_HEALTH_PORT` 建立 TCP 连接。该探针验证
实际 listener 已绑定，但不证明 Redis、PostgreSQL 或 Control 的完整业务链路可用；部署监控仍应增加经过
认证的 MQTT CONNECT/CONNACK 与 Control HTTPS 探针。`flowie_server --check` 只校验配置和 graph，不能
替代运行态 readiness。

## 运维命令

```sh
docker compose exec flowie-server flowie_server --help
docker compose exec flowie-server sh -c 'id && test -w /var/lib/flowie && test ! -w /etc/flowie'
docker inspect --format '{{.State.Health.Status}}' flowie-server
```

`FLOWIE_STORAGE_BACKEND_PLUGINS` 是以冒号分隔的容器内 `.so` 路径列表；每个路径都必须位于只读插件挂载
中并可由 UID 10001 读取。Redis 与 PostgreSQL 的连接参数属于 Flowie/Control 配置，不在入口脚本中提供
隐式默认值。Compose `env_file` 注入的值会出现在容器进程环境和 `docker inspect` 中；部署主机与 Docker
daemon 访问权限必须视为密钥权限。若需要文件型 secret，必须先扩展 Flowie 的 key-provider 契约，不能在
入口脚本中把任意文件静默转换为环境变量。

容器重启会清空 session、subscription、inflight、retained 和 pending Will。Client 必须重连并重订阅。
需要长期保存的业务消息和设备业务状态必须进入独立 BusinessStore；Redis/PostgreSQL BusinessStore 不会
恢复或替代 ProtocolStore。
