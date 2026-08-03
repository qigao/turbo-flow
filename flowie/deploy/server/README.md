# Flowie server container

该部署把 MQTT server、cluster runtime 和 embedded Control 编译进同一个非 root 运行镜像。构建使用
BuildKit named contexts，按 TurboUtils、TurboNet、TurboHTTP、RulesForge、TurboFlow 顺序安装私有 SDK；
运行容器不依赖宿主机 SDK 或源码。

## 部署边界

- server 与同机 HAProxy/Nginx 继续使用 Linux host network。默认 MQTT backend listener 是
  `127.0.0.1:18883`，Control listener 是配置文件中的 `127.0.0.1:8443`。
- `/etc/flowie`、`/etc/flowie/certs` 和 `/opt/flowie/plugins` 只读；协议 SQLite、Control SQLite 及其他
  运行状态只能写入命名卷 `/var/lib/flowie`。
- 入口始终传入 `--require-security`。缺少 server config、graph、Control config 或不可写协议存储目录时，
  容器直接失败，不启用匿名或内存 fallback。
- 容器以 UID/GID `10001` 运行，根文件系统只读，移除全部 Linux capabilities，并启用
  `no-new-privileges`。

## 构建上下文

默认目录布局如下；Compose 中的相对路径按此布局解析：

```text
cpp/
  TurboHTTP/
  rulesforge/
  turbonet/
    turbo-utils/
    turbonet/
    turbo-flow/
```

在本目录创建不入库的 `.env`，以 `.env.example` 为起点设置配置、graph、证书和插件目录。证书路径应与
`control.yml` 以及 server 配置中的绝对容器路径一致，例如 `/etc/flowie/certs/server.pem`。
`FLOWIE_SECRET_ENV_FILE` 必须指向一个权限为 `0600`、不入库的 env 文件，内容提供配置中所有
`env://NAME` 引用，例如 `FLOWIE_AUTH_SERVICE_TOKEN=...`。该文件即使当前为空也必须存在，使缺少 secret
注入边界在 Compose 展开阶段失败，而不是启动后隐式降级。

```sh
mkdir -p config certs plugins
install -m 0600 /dev/null secrets.env
docker compose build flowie-server
docker compose config
docker compose up -d flowie-server
docker compose ps
docker compose logs --tail=200 flowie-server
```

构建参数 `FLOWIE_SOURCE_REVISION` 应在发布时设置为实际 TurboFlow commit。五个仓库的 manifest 都固定同一
vcpkg baseline；Dockerfile 默认用该 commit 的 vcpkg 工具，避免使用浮动 `master`。

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
