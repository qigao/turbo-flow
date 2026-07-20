# ADR：Flowie Control 启动配置来源

## 状态

已采纳。独立 `flowie-control` 进程、严格配置 schema 和 HTTPS/mTLS listener 已接入；完整生产开放仍受
`ACL_AUTH_CONTROL_TODO.md` 中 bootstrap、限流、HA/migration 与安全发布门槛约束。

## 背景

控制面最终需要从命令行、进程环境和开发环境使用的 DotEnv 文件选择独立配置文件。手写参数解析会与
TurboUtils 重复，也容易让各入口形成不同的优先级。另一方面，自动加载当前目录 `.env` 会引入不可见的
部署状态，允许 DotEnv 覆盖进程环境也会削弱编排系统注入配置的权威性。

## 决策

新增内部 `flowie_control_startup` target，通过 `turbo_parser.h` 提供的 CMD 与 DotEnv API 解析：

- `--config/-c` 或 `FLOWIE_CONTROL_CONFIG`：独立 controller 配置文件；没有来源时 fail fast。
- `--env-file/-E` 或 `FLOWIE_CONTROL_ENV_FILE`：显式 DotEnv 文件；不自动读取当前目录 `.env`。
- `--check` 或 `FLOWIE_CONTROL_CHECK`：只验证配置并退出的启动意图。

解析顺序为先选择 DotEnv 文件并调用 `turbo_dotenv_load(path, false)`，再通过
`turbo_cmd_set_env()`/`turbo_cmd_parse()` 合并参数。因此有效优先级固定为：

```text
CLI > existing process environment > explicitly selected DotEnv
```

结果复制到有界、无外部所有权的结构体。DotEnv 会修改进程环境，所以该入口只能在创建线程和其他组件前
调用一次。CLI 语法错误和帮助输出仍由 TurboUtils CMD parser 在进程边界处理。

## 安全边界

startup options 只选择配置文件和运行模式，不接受密码、service token、私钥内容或 credential。生产 secret
继续使用 `env://...` 等 secret reference，由受信 key provider 解析；DotEnv 仅用于本地开发，不是生产 secret
store。指定的 DotEnv 文件无法读取时立即失败，不回退到默认 `.env`。

## 影响与回滚

`flowie_control_core` 不依赖 CMD/DotEnv；新依赖被隔离在不安装的 `flowie_control_startup` target。
`flowie-control` 只在 SQLite control、Iris HTTP server 与 RPC 均启用时构建和安装。回滚时可停止并移除该
可执行文件，不影响 MQTT 数据面；配置 version 1 不提供隐式旧格式 fallback。
