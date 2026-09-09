# Discovery stable resource commands Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development for implementation and independent review.

**Goal:** 完成 #95 的 adapter command 删除：discovery 改为稳定资源命令，删除旧 API/type/callback，并在副作用前保证有界历史与补偿容量。

**Architecture:** provider 是连接状态与 generation 的事实源；Graph dispatcher 是命令历史事实源；discovery 仅拥有已提交 peer set/version。一次 discovery 调用使用内部、同步、不可重入的命令 scope 预留历史空间，所有命令共享既有 dispatcher 实现，没有另一条执行路径。

**Tech Stack:** C11、CSTL vec、TinyTest、Windows Debug/ASan 与 Release user presets。

**Spec:** 用户要求删除所有旧 DLL/接口且无 C/CMake fallback；https://github.com/qigao/turbo-flow/issues/95 。PR #96/#97 已分别移除聚合 DLL 与 control fallback。本阶段完成 adapter command 项，stage/provider 旧入口仍由 #95 跟踪。

## Global Constraints

- 删除 turbo_flow_adapter_command、turbo_flow_adapter_command_t、turbo_flow_adapter_command_kind_t、turbo_flow_adapter_endpoint_t、turbo_flow_adapter_command_fn、TURBO_FLOW_ADAPTER_QUIESCE/RESUME/REPLACE_ENDPOINT 和 turbo_flow_adapter_ops_t.command。禁止 alias、wrapper、保留槽位或 C/CMake fallback。现代 resource provider 的 ops.command 必须保留。
- 保持包 2.0.0、控制 DSL 文本与插件 ABI 1.4；adapter ops 删除尾字段是批准的 C breaking 迁移，仓内消费者全部重编译；插件 root vtable 的实际布局不变，不盲目扩大 ABI 更改。
- 不增加命令历史上限、不驱逐/清空历史换取容量、不绕过 dispatcher；总历史上限仍 TURBO_FLOW_RESOURCE_COMMAND_HISTORY_MAX。不修改外部 SDK/依赖/presets。
- 同一 flow 的 discovery、resource commands、control 和生命周期调用由 host 串行化；回调不可修改注册表或销毁/reset flow。内部 scope 阻止回调重入另一个命令/scope，不声称新增跨线程并发支持。
- 先真实 RED 后生产改动；使用 apply_patch，已有 worktree，rg.exe/fd.exe；scratch/.codegraph 不提交。所有错误检查/补偿不得静默吞错。

## 协议、权衡与迁移

HIGH/事实：discovery 当前直接调用旧 adapter command；旧 create 的补偿返回值被丢弃。资源 dispatcher 当前在副作用之后 vec_push 历史，空间或分配失败会影响记录；历史上限256，discovery槽位上限也256。

选择内部预留 scope：先对剩余配额做 checked arithmetic，再 vec_reserve 存储，成功才执行 metadata/owner。仅改变内部结构，不引入公开批事务 API。备选“机械换函数名”可能在补偿前耗尽；“增大上限/驱逐历史”改变幂等窗口；“私下直接调 provider”绕过 generation/历史；均不采用。

| 协议项 | 契约 |
|---|---|
| 数据/所有权 | Graph vec 拥有复制后的 command/result 历史；scope 在 discovery 调用栈内，flow 仅借用其身份直到 end；controller 借用 flow，保存 peer set 和 stable UID，不保存独立 generation |
| 顺序/并发 | 单 flow host 串行化；scope 不可嵌套；scope 执行中 public resource command 或另一个 scope 返回 EBUSY，owner 回调不得改变生命周期 |
| 配额 | create 预留 2*N；replace 预留 3*A+2*U+2*R，A新增、U替换、R移除，仅计算实际变化；无变化/同版本幂等成功不消耗历史。先减法核对 available，再乘加或累积，禁止溢出 |
| 背压 | 预算大于剩余历史返回 ENOSPC，零 owner 副作用、peer/version 不变；vec_reserve 失败原样返回分配错误；不阻塞、不自动重试、不清历史 |
| 状态 | inactive scope -> reserved -> commands/compensation -> end；失败同样 end；只消耗实际记录数，未使用预算释放，历史保留 |
| 失败补偿 | 每条命令读取相同 stable UID 的当前 generation；补偿使用新 key/current generation。replace 补偿失败保留 prior committed peer/version、consistent=0 并返回 rollback_status；后续替换拒绝。create 补偿失败 out=NULL，返回补偿错误并在 flow error 保留原错误与补偿错误上下文 |
| 关闭 | discovery create/replace/poll 要求 STARTED；stop 后不得执行命令或 fetch；controller destroy 只销毁 metadata；flow 必须晚于 controller 销毁 |
| 观测 | 返回明确 status/rollback_status/version，现有资源 metadata 是连接实际状态查询来源；不新增日志系统或双份状态 |

计算：create 最坏正向N、补偿最多N-1，预算2N为保守上界；replace 新增最多replace+resume+quiesce=3条，替换或移除各正向+补偿=2条。固定256配额下，create 大于128slots 即使空历史也拒绝；这是为了保证补偿的显式资源边界，不改变公开 slots 常量、不能隐式拆批或退旧路径。后续容量策略另议，不在本次增大上限。

HIGH/迁移：旧 C 命令及 ops.command 消费者改为 stable resource providers 并重编译；discovery 缺少/歧义 provider 在任何 quiesce 前失败。配置文本和存储无迁移。删除源码可通过 Git 显式回退，不提供运行时回退。测试覆盖配额边界、回调失败/重入、generation、生命周期及安装 ABI 编译边界。

### Task 1: Migrate discovery and remove legacy adapter commands atomically

**Files:** Modify turbo_flow/src/flow_resource_command.c, flow_internal.h, flow_core.c, flow_control.c, flow_discovery.c; turbo_flow/include/turbo_flow.h, turbo_flow_discovery.h; turbo_flow/tests/test_flow_discovery.c, test_flow_control.c, test_turbo_flow.c, test_flow_resource_document.c；按 rg 结果修改实际 legacy 引用与当前 docs。优先不新增生产文件；新 fault test 仅在已有模式无法承载时注册到 turbo_flow/tests/CMakeLists.txt。禁止只删除现代 resource ops.command。

**Internal interfaces:** 在 flow_internal.h 声明完整内部 scope 类型和下列函数，不安装新的公共接口、不添加 legacy export：

```c
typedef struct flow_resource_command_scope_s {
  turbo_flow_t *flow;
  size_t remaining;
} flow_resource_command_scope_t;
int flow_resource_command_scope_begin(turbo_flow_t *flow, size_t budget,
                                     flow_resource_command_scope_t *scope);
int flow_resource_command_scope_execute(flow_resource_command_scope_t *scope,
                                       const turbo_flow_resource_command_t *command,
                                       turbo_flow_resource_command_result_t *out);
void flow_resource_command_scope_end(flow_resource_command_scope_t *scope);
int flow_find_adapter_command_resource(turbo_flow_t *flow, const char *adapter_name,
                                       turbo_flow_resource_metadata_t *metadata);
int flow_resource_command_init(turbo_flow_resource_command_t *command,
                              turbo_flow_resource_command_kind_t kind,
                              const char *uid, uint64_t generation);
```

flow 内部保存 active scope 指针和 command_in_progress guard（默认0）；scope begin 要求 caller 初始化为{0}，失败不安装 active scope。begin 对预算为0返回EINVAL；无变化路径无需 begin。execute 必须验证 active scope 身份及 remaining，复用统一执行函数。public dispatcher 与 scope execute 都拒绝 command_in_progress 重入；common 单条路径在校验/replay之后、任何metadata/owner回调之前，以 raw vec_resize(history, old_size+1) 取得记录槽并检查返回值，然后用 vec_at 取得槽位。普通命令可能在此分配；scope已reserve覆盖预算，因此raw resize无新增分配。取得槽位时消耗scope预算，该命令所有执行结果（包括deadline/metadata错误）必须填充同一槽，不再vec_push/vec_set。命令进行中槽位是内部pending状态，重入guard及host串行化禁止另一次history查询看到它；callback不得修改history/注册表/生命周期。确保错误/早返回释放执行 guard；历史 replay 不新增记录或消耗预算。end 仅释放scope预留身份，不删除历史。

已核对 Salts cstl/src/vec.c：vec_push(:212)经vec_prepare_copy(:62)无条件分配临时值；raw vec_resize(:187)在容量内仅清零/改size，故采用副作用前claim、之后直接填槽。禁止把reserve后push作为无分配保证；不更改外部Salts。

- [ ] Step 1: 先添加真实 RED：discovery fixture 同时有旧 callback 与正确 stable connection metadata/command；创建后应仅stable command调用且 generation 推进，旧callback计数0。当前实现将走旧 callback 而失败。运行 test_flow_discovery，记录实际失败不是编译错误。

```c
check_equal(turbo_flow_discovery_controller_create(&config, &controller), SALTS_OK);
check_equal(first.legacy_calls, 0);
check_equal(first.resource_calls, 1);
check_equal(first.generation, 2u);
```

- [ ] Step 2: 把 control 中现有唯一 provider 查找和全局原子 sequence/key 初始化移动到上述内部共享函数，control 使用它们，不改变查找歧义/metadata验证。key 使用统一内部前缀和全局递增，overflow ERANGE；不由每个controller独立从0生成碰撞key。实现内部scope/统一dispatcher guard与预留，使用既有 CSTL vec_reserve，不手写容器；历史容器物理上限也设为既有256常量。不扩展资源命令类型或改变现有 replay/error 语义。
- [ ] Step 3: discovery创建先分配/验证所有名字和provider，保存stableUID，再为正向与补偿预留。每条执行前查询该UID当前generation并核对owner/kind，不能在UID消失时改选同owner的新UID。所有命令与补偿调用scope_execute。替换在输入/版本校验之后计算实际A/U/R预算；版本no-op不begin。先完成资源命令再提交peer/version。所有scope有唯一end清理路径，补偿失败不得(void)丢弃；补偿尽力执行其余已应用slots并保留首个错误。新增槽位回滚后inactive，其非active endpoint不是已提交peer事实，不虚称恢复未建模endpoint。
- [ ] Step 4: 删除旧公开类型/枚举/endpoint/函数声明、flow_core.c旧实现及adapter ops.command尾字段。删除test_turbo_flow旧API专用test/helper/fields；有用状态/错误覆盖在新discovery或现有resource测试中承接。test_flow_control旧callback探针移除，缺provider测试改为只有普通adapter、无commandprovider仍ENOENT。迁移discoveryfixture到仅stable ops，不保留旧命名callback壳。
- [ ] Step 5: 分别增加以下真实行为测试（未实现前应RED）：create缺provider/歧义预检零副作用；两slot创建中途失败恢复前slot、恢复失败明确报错；replace原有版本幂等/冲突/回滚及回滚失败后拒绝新操作；执行前不足历史拒绝且peer/version/provider状态不变；精确预算可容纳失败及补偿，少一条则零副作用；scope释放后普通命令可执行；provider回调重入命令返回EBUSY且不消耗预留；stop后replace/poll拒绝且fetch无调用；不同controller keys不碰撞、每次读取当前generation。历史填充用真实resource命令，不直接伪造history vec。资源command既有 replay/同key异payload/full行为回归保留。
- [ ] Step 6: 增加可复验存储分配失败测试：使用仓内已有fault-test宏替换/编译方式（先查现有 *_fault.c），让scope历史reserve和普通命令resize claim分别失败，断言 owner调用0、generation未变，且后续调用可重试（guard已释放）；不要修改全局分配器或污染公开头。普通命令record也禁止副作用后push/分配。使用真实rawvec实现的测试验证scope预留后连续记录及补偿，不造替代容器。
- [ ] Step 7: 更新公开discovery/resourcecommand文档：host序列化、不可重入、STARTED、stableprovider要求、配额公式/ENOSPC、create补偿错误诊断和destroy借用期；当前旧API文档改为移除/迁移说明，历史计划不机械清理。rg确认旧symbols在有效C/header已无引用，不碰新 resource commands。验证安装Graph导出没有旧函数（既有Windows dumpbin检查可扩展，不引入旧DLLfixture）。
- [ ] Step 8: 两profileconfigure/build，先最小相关tests再全量CTest各一次，安装consumer验证source/ABI重编译和包不回退；git diff --check、自审、显式提交；完整报告只写scratch不提交。

```powershell
cmd.exe /d /c 'call "C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat" -no_logo -arch=amd64 -host_arch=amd64 && cmake --preset win-dev-user && cmake --build --preset win-dev-user && ctest --preset win-dev-user --output-on-failure'
```

Release使用win-release-user；聚焦 `^(test_flow_discovery|test_flow_control|test_flow_resource_document|test_turbo_flow)$` 并纳入新增faulttest。每次工具运行保留真实exitcode和summary，不用日志grep冒充全量通过。
