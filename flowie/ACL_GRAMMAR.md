# Flowie Control ACL 文法与使用

本文说明 Flowie Control 当前用户 ACL 文档的语法、MQTT 权限语义、topic 树约束，以及通过
Control UI 或 Management JSON-RPC 发布规则的方法。

该语法是 Flowie 的控制面格式，不是 Mosquitto ACL 文件格式。`read`、`write`、topic wildcard
等概念与 MQTT/Mosquitto 的常用权限模型一致，但不能把 Mosquitto ACL 行直接提交给 Flowie。

实现事实源为：

- lexer：[control/parser/acl_lexer.re](control/parser/acl_lexer.re)
- Lemon grammar：[control/parser/acl_grammar.y](control/parser/acl_grammar.y)
- parser、canonical formatter 和 compiler：[control/flowie_control_acl.c](control/flowie_control_acl.c)
- Repository 校验：[control/flowie_control_validation.c](control/flowie_control_validation.c)

## 完整示例

```text
user device-7 allow {
  write topic root-a/groups/china/east/operators/devices/%u/{event,heartbeat,process}
  read topic root-a/groups/china/east/operators/devices/%c/{command,payment}
  deny readwrite topic root-a/groups/china/east/operators/devices/%u/private
}
```

这份文档产生 7 条内部规则：1 条 CONNECT allow、3 条 publish allow、2 条 subscribe allow，以及
1 条同时覆盖 publish/subscribe 的 deny。末尾的 `{...}` 每个候选值分别展开为一条内部规则。

## 权限语义

顶层决定控制 MQTT CONNECT：

```text
user <subject> allow
user <subject> deny
```

- `subject` 是当前 Control Domain 中的 `principal_id`，保存规则时该用户必须存在且 enabled。
- `allow` 允许该用户建立 MQTT 连接；没有 topic block 时只允许连接，不授予发布或订阅权限。
- `deny` 拒绝该用户建立 MQTT 连接，并且不能带 topic block。
- 没有匹配 allow 的操作默认拒绝。

topic block 中每条语句控制 MQTT topic 操作：

| 关键字 | MQTT 权限 |
| --- | --- |
| `read` | SUBSCRIBE |
| `write` | PUBLISH |
| `readwrite` | SUBSCRIBE 和 PUBLISH |
| 无 effect 前缀 | allow |
| `deny` 前缀 | deny |

同时匹配 allow 与 deny 时，显式 deny 优先；`ordinal` 不会改变 deny 优先级。

## 语法

以下 EBNF 描述 parser 接受的结构。`SP` 表示一个或多个空白字符，`LF` 表示换行：

```text
document       = "user" SP subject SP connection [ SP "{" LF entries LF "}" ] ;
connection     = "allow" | "deny" ;
entries        = entry *(LF entry) ;
entry          = [entry-effect SP] access SP "topic" SP topic-pattern ;
entry-effect   = "allow" | "deny" ;
access         = "read" | "write" | "readwrite" ;
topic-pattern  = domain "/groups/" group-path "/devices/" device "/" leaf ;
group-path     = identifier *("/" identifier) ;
device         = identifier | "%u" | "%c" | "+" ;
leaf           = identifier | "+" | "#" | alternatives ;
alternatives   = "{" identifier "," identifier *("," identifier) "}" ;
subject        = identifier ;
domain         = identifier ;
identifier     = 1*(ALPHA | DIGIT | "_" | "." | ":" | "@" | "~" | "-") ;
```

在 document 层，`user`、`allow`、`deny`、`read`、`write`、`readwrite` 和 `topic` 是关键字，不能作为
`subject`。进入 topic 后，`groups` 和 `devices` 是结构关键字，不能作为静态 Domain、group、device
或 leaf segment；其他 document 关键字在 topic 中仍按普通 segment 解析。

## Canonical 格式

Repository 不只解析文档，还要求输入与 formatter 生成的 canonical 文本 byte-for-byte 相同。通过
JSON-RPC 提交时必须遵循以下格式：

- 第一行是 `user <subject> allow {`，每条 topic 语句独占一行并缩进两个空格，最后一行只有 `}`。
- allow topic 语句不写 `allow` 前缀；`allow read topic ...` 可被底层 parser 接受，但不是 canonical
  格式，Repository 会拒绝。
- deny topic 语句必须以 `deny ` 开头。
- 不允许空行、行尾空格、CRLF 或末尾额外换行。
- `user <subject> allow` 和 `user <subject> deny` 这类无 block 文档只有一行。

Control UI 会从表单生成 canonical 文档，并在提交前显示 `Canonical document` 预览。

## Topic 树

每个 topic 必须具有以下树形结构：

```text
<domain>/groups/<root-group>[/<child-group>...]/devices/<device>/<leaf>
```

例如：

```text
root-a
└── groups
    └── china
        └── east
            └── operators
                └── devices
                    └── %u
                        ├── event
                        ├── heartbeat
                        └── process
```

约束如下：

- `<domain>` 必须与 Management 请求选定的 Domain 完全相同。
- group path 至少一层、最多 16 层。
- path 中每个 group 必须存在且 enabled；第一层必须是根 group，后续每层的 parent 和 depth 必须与
  path 一致。不能省略中间父 group。
- group path 只接受静态 group ID，不接受 `+`、`#`、`%u` 或 `%c`。
- group path 描述 topic 的资源树，不会把 ACL subject 改成 group，也不隐式授予 group 成员权限。

## Device 与 leaf 匹配

`<device>` 支持：

| 写法 | 含义 |
| --- | --- |
| 静态 ID | 只匹配该 topic segment |
| `%u` | 精确匹配当前 MQTT CONNECT username |
| `%c` | 精确匹配当前 MQTT client ID |
| `+` | MQTT 单层 wildcard |

`%u` 和 `%c` 必须占据完整 segment。运行时值必须非空，且不能包含 `/`、`+` 或 `#`。`subject`、
MQTT username 和 client ID 是三个不同字段，不要求彼此相同。

`<leaf>` 支持静态 ID、`+`、末尾 `#`，或者 2 到 16 个互不重复的静态候选值：

```text
{event,heartbeat,process}
```

alternatives 只能位于最后一个 leaf，每个候选值展开为独立规则。`+` 匹配一个 topic level，`#` 匹配
剩余 topic levels；grammar 保证 `#` 只能出现在末尾。

## 容量限制

| 项目 | 限制 |
| --- | ---: |
| 一个 Domain 中的 draft ordinal | `0..4095` |
| 同一用户在同一 Domain 中的 ACL 文档 | 1 |
| 一个 ACL 文档的 topic 语句 | 64 |
| group path 深度 | 16 |
| leaf alternatives | 16 |
| subject、Domain 或 group ID | 255 bytes |
| 编译后的单个 topic pattern | 511 bytes |
| Repository 内部 ACL 文档 | 16383 bytes |
| Control UI topic 输入 | 16000 characters |
| Management JSON-RPC `rule_line` | 2047 bytes |
| 一个已发布 bundle 的内部规则 | 4096 |

每个文档固定产生 1 条 CONNECT 规则；普通 topic 语句产生 1 条规则，alternatives 产生候选值数量的
规则。`control.policy.validate` 返回的是展开后的 `rule_count` 和 `deny_rule_count`。

## 通过 Control UI 使用

1. 使用具有 `policy_admin` 权限的 Domain 账号登录 Flowie Control。
2. 打开 `ACLs`，选择 `Add`；一个用户只创建一份 ACL 文档。
3. 选择用户和 Connection 行为，设置稳定的 Evaluation order。
4. 在 Topic permissions 中每行输入一条 topic 语句，不要输入外层 `user ... {}`。
5. 选择 `Add to draft`，或者在已有用户 ACL 上选择 `Edit` 和 `Save ACL`。
6. 检查完整 draft 后选择 `Publish`。发布会重新校验整个 draft，并原子生成一个新的 active policy
   version。

修改或删除 draft 不会立即改变当前 active policy；只有成功 Publish 后 Broker 才会取得新版本。发布失败
时旧版本继续有效。不要直接修改 PostgreSQL 或 SQLite 中的 ACL 表。

## 通过 Management JSON-RPC 使用

Management RPC 的 endpoint、登录和 bearer session 见
[MANAGEMENT_RPC_API.md](MANAGEMENT_RPC_API.md)。`rule_line` 必须作为一个 JSON string 提交，因此换行写为
`\n`。

添加或替换一份用户 ACL：

```json
{
  "jsonrpc": "2.0",
  "id": "acl-put-1",
  "method": "control.policy.rule.put",
  "params": {
    "domain_id": "root-a",
    "ordinal": 10,
    "rule_line": "user device-7 allow {\n  write topic root-a/groups/china/east/operators/devices/%u/{event,heartbeat}\n  read topic root-a/groups/china/east/operators/devices/%c/command\n}",
    "request_id": "acl-device-7-v1"
  }
}
```

在提交整个 draft 前显式校验：

```json
{
  "jsonrpc": "2.0",
  "id": "acl-validate-1",
  "method": "control.policy.validate",
  "params": {"domain_id": "root-a"}
}
```

发布新的 active policy：

```json
{
  "jsonrpc": "2.0",
  "id": "acl-publish-1",
  "method": "control.policy.publish",
  "params": {
    "domain_id": "root-a",
    "request_id": "acl-publish-root-a-v1"
  }
}
```

同一次业务写入在传输结果不确定时必须复用原 `request_id`；不同写入不能复用。删除使用
`control.policy.rule.delete`，参数为 `domain_id`、`ordinal` 和新的 `request_id`，删除后仍需 Publish 才会
改变 active policy。

## 常见拒绝原因

- 提交了旧的 `effect|subject_kind|...` pipe 格式或 Mosquitto ACL 行。
- 文本能解析但不是 canonical 格式。
- 文档 Domain 与请求 Domain 不一致。
- subject 不存在、已 disabled，或同一 Domain 的其他 ordinal 已保存相同 subject。
- group 不存在、已 disabled、depth 错误，或 path 与实际 parent chain 不一致。
- `user ... deny` 后仍包含 topic block。
- alternatives 重复、少于 2 个、超过 16 个，或不在末尾 leaf。
- 文档、topic、条目数、展开规则数超过容量限制。
