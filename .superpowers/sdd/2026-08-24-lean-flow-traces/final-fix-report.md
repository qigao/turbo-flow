# Lean Flow Traces 最终修复报告

## 提交边界

- Base：`5ccb820`
- Head：`HEAD`（本报告与实现由同一个最终 fix commit 原子提交；实际 SHA 见交付时的 `git rev-parse HEAD`）
- Commit：`fix: model route decisions per edge`
- 禁止范围：未修改 C/C++、CMake、presets 或 vcpkg 文件。

## 设计选择与接口影响

`事实`：`turbo_flow/src/flow_completion.c:52-58` 在遍历每条 edge 时读取该 edge 的 target，并分别与 named route 比较；`turbo_flow/tests/test_flow_policy.c:354-386` 的双 target fan-out 实际选择 `selected`、跳过 `skipped`。

最小修复将 `RouteObservation` 的全局 `decision : RouteDecision` 改为 `edges : List RouteEdgeObservation`，其中每个 `RouteEdgeObservation` 同时持有 `kind : EdgeKind` 与该 edge 自己的 `decision : RouteDecision`。这避免平行列表的长度错配，也允许同一个 named fan-out 表达 `[true, false]`。

接口影响仅限 Lean trace 模型：构造 `RouteObservation` 的代码需把 decision 放入每个 edge observation。`edgeActive`、`RouteDecision`、route 优先级、Flow/task/fan-in wrapper 及全部 C API 均未改变。unknown named route 仍由模型外的 `flow_data_route_exists()` / `flow_apply_completion()` 在 `turbo_flow/src/flow_completion.c:84-96,179-183` fail fast；predicate 错误仍不转换为 false。

新增 `routeMask_pointwise`：对任意索引，mask 的 `getElem?` 等于同索引 edge 使用自身 decision 调用 `edgeActive` 的结果。`routeMask_length` 保留并适配新结构。

## RED / GREEN

### RED

先向旧 API 加入双 target 字面量期望：单个 `.namedRoute true`、两条 edge，期望 `[true, false]`。

- 命令：`lake --dir formal build TurboFlow.TraceProofs`
- Exit：`1`
- 预期失败：Lean 报告 `decide` 已证明该命题为 false；旧模型把 `true` 广播成两项，无法产生第二项 `false`。失败来自缺失的 per-edge 语义，不是工具链、路径或语法错误。

### GREEN

实现 `RouteEdgeObservation`、逐 edge 的 `routeMask`、双 target `[true, false]` executable example 与 `routeMask_pointwise` 后：

- 首次最小命令：`lake --dir formal build TurboFlow.TraceProofs`
- Exit：`0`
- 干净重跑最小 target：exit `0`，`TurboFlow.Trace` 与 `TurboFlow.TraceProofs` 均成功构建。

## 文档与行号核对

- completion 防重：`turbo_flow/src/flow_completion.c:190-214,235-240`。
- named target 逐 edge 比较：`turbo_flow/src/flow_completion.c:52-58`。
- unknown-route fail-fast：`turbo_flow/src/flow_completion.c:84-96,179-183`。
- 双 target C 回归：`turbo_flow/tests/test_flow_policy.c:354-386`。
- `docs/FORMAL_FLOW_TRACE_MODEL.md` 与 `formal/README.md` 的缩写 `src/...` 已统一为 `turbo_flow/src/...`。
- trace 规范、实现计划及 README theorem 清单均增加 pointwise theorem 与第五个 executable example。

## 最终验证

| 命令 | Exit | 结果 |
| --- | ---: | --- |
| `codegraph sync .` | 0 | 索引已是最新 |
| `lean --version` | 0 | Lean 4.33.1 |
| `lake --dir formal clean` | 0 | 干净构建树 |
| `lake --dir formal build TurboFlow.TraceProofs` | 0 | 最小 proof target 通过 |
| `lake --dir formal env lean formal/TurboFlow/TraceProofs.lean` | 0 | 直接 Lean 检查通过 |
| `lake --dir formal build` | 0 | 完整 Lake build 通过 |
| `rg.exe -n "\b(sorry\|admit\|axiom)\b" formal -g "*.lean"` | 1 | 预期无匹配 |
| `git diff --check` | 0 | 无 whitespace 错误 |
| `git diff --name-only 5ccb820 -- CMakeLists.txt CMakeUserPresets.json presets vcpkg.json turbo_flow` | 0 | 无输出，禁止范围未改 |
| `rg.exe -n "(^\|[^/])src/" docs/FORMAL_FLOW_TRACE_MODEL.md formal/README.md` | 1 | 预期无缩写路径匹配 |

## 残余边界

该修复证明 Lean reference evaluator 的 per-edge route mask 语义，并未声称建立 C/Lean refinement proof。版本化 C trace exporter、序列化契约、自动差分 runner 与并发交错仍在既有模型边界之外。
