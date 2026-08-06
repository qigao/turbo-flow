# Flowie 第三方系统接入指南

本文说明第三方业务系统如何接入 Flowie Control 和 Flowie Broker。接入方可以管理自己
Domain 内的用户、密码、Group 和 ACL，也可以让 MQTT 客户端使用已创建的身份连接 Broker。

Flowie Broker 不配置、选择或拥有业务 Domain。Domain 是 Flowie Control 数据库中的第三方系统
接入 ID；认证成功后由 Auth service 返回用户所属 Domain，ACL 再用该 Domain 的已发布策略判定
topic 操作。

## 身份与凭据边界

三类凭据用途不同，不能互换：

| 凭据 | 使用方 | 用途 | 发送位置 |
| --- | --- | --- | --- |
| Management session token | 第三方管理后端 | 增删改用户、密码、Group、Role 和 ACL | `/v2/control/rpc` 的 `Authorization: Bearer` |
| Service token | Flowie Broker 或其他受信服务 | 调用 Auth/ACL decision endpoint | `/v4/authenticate`、`/v4/acl/check` 的 bearer token |
| MQTT username/password | 设备或应用客户端 | MQTT CONNECT，然后 PUBLISH/SUBSCRIBE | MQTT CONNECT User Name 和 Password |

第三方系统修改 Auth/ACL 数据时必须先用自己的 Domain、管理账号和密码创建 Management session，
再用 session token 调 Management RPC。数据库生成的 service token 当前不授予 Management RPC
权限，也不是 MQTT 客户端密码。

`service_domain` 只定位 service principal 的凭据命名空间。它不限制 Broker 可以认证的业务
Domain，也不会自动加到 topic 前缀。拥有 `flowie_auth_client` 或 `flowie_acl_client` 的 service
principal 是 Broker 级受信身份，应只分配给受控后端。

## 接入流程

```text
平台管理员
  -> 创建第三方 Domain 和初始管理账号
  -> 分配最小 Management role

第三方管理后端
  -> 使用 Domain + username/password 登录
  -> 使用 session token 调 Management RPC
  -> 创建 MQTT 用户、设置密码、维护 Group 和 ACL、发布策略

MQTT 客户端
  -> 使用 username/password 连接 Flowie Broker
  -> Broker 调 /v4/authenticate
  -> Broker 对 CONNECT、SUBSCRIBE、PUBLISH 调 /v4/acl/check
```

初始 Domain 和首个管理账号必须由 Flowie 的 `system` Domain 管理员或部署引导流程创建。第三方
系统不能在没有受信身份的情况下自助创建自己的信任根。

## 1. 配置管理账号

为每个第三方系统分配一个稳定 Domain ID，例如 `warehouse`。在该 Domain 创建专用管理 principal、
设置强密码，并按职责分配保留 Role：

| 管理需求 | 最小 Role |
| --- | --- |
| 只读查询 | `viewer` |
| 用户、Group 和成员关系 | `user_admin` |
| ACL 草稿和发布 | `policy_admin` |
| 密码、credential、Role、审计及全部 Domain 内管理 | `security_admin` |

Role 不隐式包含 `viewer`。需要写入和查询的集成必须同时分配相应写 Role 与 `viewer`。不要把
`system/admin` 或 `system_admin` 账号交给业务系统。

管理后端通过同源 HTTPS 登录：

```http
POST /v2/control/login HTTP/1.1
Host: control.example.com
Origin: https://control.example.com
Content-Type: application/x-www-form-urlencoded

domain=warehouse&principal=warehouse-control&password=<url-encoded-password>
```

成功响应为 `303`。从 `Set-Cookie` 读取 `flowie_session` 的不透明值，后续作为 bearer token 使用：

```http
POST /v2/control/rpc HTTP/1.1
Host: control.example.com
Authorization: Bearer <flowie_session-value>
Content-Type: application/json

{"jsonrpc":"2.0","method":"control.system.status","params":{},"id":"status-1"}
```

Management session 有容量和 TTL 限制，Flowie 重启后也必须重新登录。账号 disabled、Role 被移除、
session 过期或被淘汰后，请求立即失效。浏览器跨域应用应通过自己的后端调用 Flowie，不能把管理
密码或 session token 放到前端。

完整 method、参数、分页、幂等和错误码见
[MANAGEMENT_RPC_API.md](MANAGEMENT_RPC_API.md)。

## 2. 创建 MQTT 用户

创建用户和设置密码是两个独立写操作。每个写请求都要使用稳定且唯一的业务 `request_id`：

```json
{
  "jsonrpc": "2.0",
  "method": "control.user.create",
  "params": {
    "principal_id": "warehouse-device-202",
    "principal_type": "device",
    "request_id": "warehouse-device-202-create"
  },
  "id": "user-create-1"
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "control.password.set",
  "params": {
    "principal_id": "warehouse-device-202",
    "new_password": "<16-or-more-byte-secret>",
    "mode": "create",
    "request_id": "warehouse-device-202-password-v1"
  },
  "id": "password-set-1"
}
```

Domain-scoped session 可以省略 `domain_id`，此时使用登录 Domain；也可显式发送相同 Domain。普通
Domain 管理账号不能选择其他 Domain。

MQTT 登录时：

- User Name 是 `principal_id`，例如 `warehouse-device-202`。
- Password 是通过 `control.password.set` 设置的原始密码。
- Client Identifier 是独立的 MQTT session/routing ID，不是用户名或凭据。
- 当前全局 username 解析要求一个 username 只对应一个 enabled Domain；跨 Domain 重名会 fail
  closed，因此接入方应使用全局唯一 username，例如 `warehouse-device-202`。

## 3. 创建 Group 与 ACL

Group 可以有多层父子关系，最大深度为 16。topic 中必须写出完整父链：

```text
warehouse/groups/china/east/operators/devices/warehouse-device-202/event
```

为用户保存一份 canonical ACL 文档：

```json
{
  "jsonrpc": "2.0",
  "method": "control.policy.rule.put",
  "params": {
    "ordinal": 10,
    "rule_line": "user warehouse-device-202 allow {\n  write topic warehouse/groups/china/east/operators/devices/%u/{event,heartbeat}\n  read topic warehouse/groups/china/east/operators/devices/%c/command\n  deny readwrite topic warehouse/groups/china/east/operators/devices/%u/private\n}",
    "request_id": "warehouse-device-202-acl-v1"
  },
  "id": "acl-put-1"
}
```

然后校验并发布整个 Domain 的草稿：

```json
{"jsonrpc":"2.0","method":"control.policy.validate","params":{},"id":"acl-validate-1"}
```

```json
{
  "jsonrpc": "2.0",
  "method": "control.policy.publish",
  "params": {"request_id": "warehouse-policy-v1"},
  "id": "acl-publish-1"
}
```

同一 subject 在同一 Domain 只能保存一份 ACL 文档。更新时替换该文档，删除时删除整份文档；只有
成功 `publish` 后才改变 Broker 使用的 active policy。`read` 是 SUBSCRIBE，`write` 是 PUBLISH，
`deny` 只拒绝匹配的操作，不表示拒绝 MQTT 连接。`%u` 匹配 MQTT username，`%c` 匹配 MQTT
client ID。

完整 grammar、canonical 格式、wildcard 和容量限制见 [ACL_GRAMMAR.md](ACL_GRAMMAR.md)。

## 4. 配置 Broker 的 service credential

Flowie Broker 调用 Control Auth/ACL endpoint 前，需要一个数据库生成的 service principal：

1. 在一个受控 Domain 创建 `principal_type: service` 的 principal，例如 `broker-main`。
2. 创建并分配精确 Role `flowie_auth_client` 和 `flowie_acl_client`。
3. 调 `control.credential.generate` 生成 token；明文只在成功响应中返回一次。
4. 将 token 原样写入 secret manager 或环境变量，不要 Base64 解码，不要写入 YAML。

也可以在 Control Dashboard 完成同一流程。`system/system_admin` 在 Overview 选择 **Add domain**
创建 Domain，然后切换到该 Domain；在 Users 页面创建 Type 为 `service` 的用户，在 Roles 页面创建并
分配所需 endpoint Role，最后回到 Users 对该 service 用户选择 **Issue token**。成功后页面只显示一次
Domain、Service ID 和 token；应立即复制到 secret manager。刷新或关闭提示后不能恢复明文，再次选择
**Issue token** 会立即轮换 credential 并使旧 token 失效；**Revoke token** 会立即禁用当前 token。
Domain 本身不持有 token，token 始终属于该 Domain 下选定的 service principal。

以下请求均由 `platform-services` Domain 的 `security_admin` session 发送。先创建 service
principal：

```json
{
  "jsonrpc": "2.0",
  "method": "control.user.create",
  "params": {
    "principal_id": "broker-main",
    "principal_type": "service",
    "request_id": "broker-main-create"
  },
  "id": "broker-create-1"
}
```

首次部署时创建两个 endpoint Role；已经存在时不要重复创建：

```json
{
  "jsonrpc": "2.0",
  "method": "control.role.create",
  "params": {"role_id": "flowie_auth_client", "request_id": "role-auth-client-create"},
  "id": "role-create-1"
}
```

```json
{
  "jsonrpc": "2.0",
  "method": "control.role.create",
  "params": {"role_id": "flowie_acl_client", "request_id": "role-acl-client-create"},
  "id": "role-create-2"
}
```

分别调用 `control.role.assign`，将两个 Role 分配给 `broker-main`：

```json
{
  "jsonrpc": "2.0",
  "method": "control.role.assign",
  "params": {
    "principal_id": "broker-main",
    "role_id": "flowie_auth_client",
    "request_id": "broker-main-auth-role"
  },
  "id": "role-assign-1"
}
```

第二次请求把 `role_id`、`request_id` 和 `id` 分别改为 `flowie_acl_client`、
`broker-main-acl-role` 和 `role-assign-2`。最后生成 token：

```json
{
  "jsonrpc": "2.0",
  "method": "control.credential.generate",
  "params": {"principal_id": "broker-main", "request_id": "broker-main-token-v1"},
  "id": "credential-generate-1"
}
```

成功响应中的 `result.token` 是 `flw_mqtt_v1_<Base64URL-no-padding>` 形式的完整 secret；前缀是
credential 格式版本，不表示它只能作为 MQTT 客户端密码。

Flowie channel 配置示例：

```yaml
channels:
  mqtt.auth-service:
    kind: auth_provider
    config:
      backend: https
      url: https://control.example.com/v4/authenticate
      method: password
      service_id: broker-main
      service_domain: platform-services
      service_token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN
      timeout_ms: 3000
      max_secret_size: 4096

  mqtt.acl-service:
    kind: acl_provider
    config:
      backend: https
      url: https://control.example.com/v4/acl/check
      service_id: broker-main
      service_domain: platform-services
      service_token_ref: env://FLOWIE_AUTH_SERVICE_TOKEN
      timeout_ms: 3000
      max_response_size: 65536
```

`platform-services` 是 `broker-main` 所属的 service credential Domain，不是 Broker 唯一支持的业务
Domain。认证结果中的用户 Domain 才决定 ACL policy 和 topic tree。

HTTP 请求必须同时提供：

```http
Authorization: Bearer <service-token>
X-Flowie-Service-Id: broker-main
X-Flowie-Service-Domain: platform-services
```

缺少 header、token 错误、principal disabled、credential revoked、Domain/ID 不匹配或缺少 endpoint
Role 时均拒绝。不要把 service token 交给 MQTT 客户端。

## 5. MQTT 客户端行为

客户端只需要 Broker 地址、username、password、client ID 和最终 topic 字符串，不需要 Domain、
service token、HTTP header 或 Auth/ACL JSON。最小 `config.yml` 例如：

```yaml
mqtt:
  broker: mqtts://broker.example.com:8883
  username: warehouse-device-202
  password: <mqtt-password>
  client_id: warehouse-device-202-runtime-1
  topic: warehouse/groups/china/east/operators/devices/warehouse-device-202/event
```

客户端应用读取这些字段，用 username/password/client ID 发送 CONNECT，然后直接用 `topic` 的完整
字符串执行 SUBSCRIBE/PUBLISH。客户端不根据 username、password 或 client ID 猜测 Domain，也不在
运行时拼接 topic；若部署系统需要按设备生成 topic，应在下发 `config.yml` 前完成模板渲染。password
只能进入 CONNECT，绝不能成为 topic 的一部分。生产部署还应通过客户端既有的 secret provider 注入
password，避免保存明文。

典型流程是：

1. MQTT CONNECT 发送 username/password/client ID。
2. Broker 通过 `/v4/authenticate` 获得 principal、Domain、Group、Role 和 policy version。
3. Broker 对 CONNECT 和后续 SUBSCRIBE/PUBLISH 调 `/v4/acl/check`。
4. ACL 默认拒绝；只有已发布规则允许的操作通过，匹配的显式 deny 优先。

客户端不能通过 topic、header 或 username 自行声明 Domain。Domain 来自通过 credential 校验的
Control 记录；客户端应用只负责对配置中的 topic 收发消息。

## Token 轮换与撤销

- Management session：过期后重新登录；不持久化到客户端设备。
- MQTT password：用 `control.password.set` 的 `replace` mode 更换。
- Service token：用 `control.credential.rotate` 更换，并原子更新 secret manager；确认新 token 生效
  后销毁旧值。
- Dashboard 的 **Issue token** 对尚无 credential 的 service principal 执行首次生成；已有 credential
  时执行轮换。页面不会保存明文，关闭、刷新或复制失败后只能再次签发新 token。
- 立即终止 service credential：调用 `control.credential.revoke` 或 disable principal。
- generate/rotate 的响应丢失时，不能通过重试取回同一明文；使用新的 `request_id` 再 rotate。

所有传输必须使用 HTTPS/TLS。token、密码和 session 不得进入 URL、日志、审计 payload 或错误
遥测。

## 接入验收清单

- 第三方管理账号只能访问自己的 Domain，且只有所需 Management Role。
- username 在所有 enabled Domain 中唯一。
- ACL topic 的首段与用户 Domain 一致，Group path 与数据库父链一致。
- ACL 已 validate 并 publish，而不只是保存到 draft。
- Broker 配置没有业务 `domain` 字段，只有 service credential 的 `service_domain`。
- Auth 与 ACL URL 分别为 `/v4/authenticate` 和 `/v4/acl/check`。
- service token 只存在于 secret provider，MQTT 客户端未持有该 token。
- credential rotate/revoke、账号 disable、错误 Domain 和错误 token 均经过拒绝测试。
