# Typed-operation ABI 3.0 Runtime Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 实现 ABI3 同步 typed-operation DLL→generation→Graph→独立结果域的完整最小执行路径。

**Architecture:** PluginHost的有界catalog是唯一注册事实源，generation缓存工厂和执行函数。
Graph保留原projection并原子提交一个独立result槽，caller-owned result-domain保护DLL
元数据和结果callback越过generation退役。无第二执行器，无worker snapshot修改。

**Tech Stack:** C11、C++公开头验证、Salts CMeta/CSTL/同步原语、TinyTest、CMake user presets。

**Spec:** [精确ABI3契约](../../architecture/typed-operation-abi3.md)。实施者须完整阅读该文件，
以下所有类型、字段、错误、容量和线程规则以该规格的完整C声明为准。

## Global Constraints

- shared plugin ABI必须统一为3.0，拒绝1.x/2.x、未来版本、短布局；无旧布局fallback。
- 不改变CMeta原始descriptor ABI，不把TurboFlow package版本自动改成3。
- 所有preflight成功之前不转移Graph、不创建有副作用实例。
- 只接受inline/thread_safe/none、deadline0、steps-charged、固定纯值schema和空permissions。
- 首profile通过不关闭#93其余线程/取消验收，更不关闭#73真实引擎/链接解耦。
- 不发布半实现capability、公共stub或安装中间SDK；Task2/4未接入代码只在内部非安装测试目标。
- callback代码和metadata由匹配snapshot保护；worker不碰控制面非原子lease。
- 所有源码改动前维护CodeGraph并至少读3处实现/测试；文件检索仅rg.exe/fd.exe。
- 启动实施时使用plugin-system/cflow/cmeta/memory-design-protocols/cmake-presets/tinytest相关skill。

## 文件职责与命令约定

`flow_plugin.c`维护operation事务与snapshot，不拆新registry。`flow_plugin_operation.c`
新增编译binding、budget和callback包装。`flow_plugin_result_domain.c`只处理结果域控制面
所有权。`flow_message.c`/`flow_projection_owner.c`处理Graph原子result槽，
`flow_data_schema.c`新增有限CMeta语义验证。`flow_plugin_generation.c`负责装配与退役。
新文件不引入RulesForge/TurboScript头。测试fixture独立DLL，仅导出canonical entry。

所有下列cmake/ctest命令在已由本机实际`VsDevCmd.bat -arch=x64 -host_arch=x64`
初始化的终端执行。执行者先用vswhere定位VsDevCmd，不写死MSVC版本路径；自动化用
`cmd /c`调用该bat后执行相同命令。先读CMakeUserPresets及其继承presets，保留manifest。
Debug为`win-dev-user`，Release为`win-release-user`，首次新增源文件后重新configure：

```text
cmake --preset win-dev-user
cmake --preset win-release-user
```

新CTest必须用原生add_executable/target_link_libraries/add_test。TinyTest参考建议helper
与cmake-presets禁止新helper冲突时，遵循仓库构建约束和更专项cmake-presets规则，保留
无关既有helper不重写。每个任务RED必须先确认预期断言/缺接口失败，GREEN不能只编译。

### Task 1: 全边界ABI拒绝与同步消费者迁移

**Files:** Modify `turbo_flow/include/turbo_flow_plugin_operation.h`、
`turbo_flow/src/flow_plugin.c`、`turbo_flow/src/flow_plugin_generation.c`、
`turbo_flow/src/flow_plugin_projection.c`、`io/cnet/src/turbo_flow_cnet_plugin.c`、
`io/chttp/src/turbo_flow_chttp_plugin.c`、`ingress/protocol/src/flow_protocol_plugin_support.c`；
Test `tests/plugin_host_abi_test.c`、`turbo_flow/tests/test_flow_plugin_host.c`、
`turbo_flow/tests/test_flow_plugin_schema.c`、`plugin_fixture.c`、`plugin_schema_fixture.c`、
`plugin_operation_header_cpp.cpp`、`plugin_generation_header_cpp.cpp`（后二者位于turbo_flow/tests）。
另以`rg.exe -n 'abi_major|abi_minor|VERSION_MAJOR' turbo_flow io ingress tests`确认全部插件入口。

**Interfaces:** shared ABI宏3u/0u；既有root/host/registration/provider/owner/catalog精确size，
同一结构的新旧版本均只走一个validator。当前generation签名本任务不变，非空binding gate保留。

- [ ] 写实际旧root/host服务fixture，逐个构造 major={1,2,3,4}、minor={0,1}，
  size={0,版本头前一字节,sizeof(T)-1,sizeof(T),sizeof(T)+1}。测试分配/load/register
  计数为0；在root3.0通过后分别注入短registration、owner、catalog验证不读尾字段。
  现有CNet/CHTTP仅检查major必须产生RED。C++断言改为3，并编译所有INIT。

```c
_Static_assert(TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR == 3u, "plugin ABI major");
_Static_assert(TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR == 0u, "plugin ABI minor");
/* 在现有host fixture测试体，先保存完整API再只改一个字段。 */
turbo_flow_plugin_host_config_t cfg = TURBO_FLOW_PLUGIN_HOST_CONFIG_INIT;
turbo_flow_plugin_host_t *host = NULL;
turbo_flow_plugin_error_t error = TURBO_FLOW_PLUGIN_ERROR_INIT;
cfg.abi_minor = 1u;
check_equal(turbo_flow_plugin_host_create(&cfg, &host, &error), SALTS_EINVAL);
check_null(host);
```

- [ ] RED：`cmake --build --preset win-dev-user --target test_flow_plugin_host test_flow_plugin_schema`
  然后`ctest --preset win-dev-user -R 'plugin.*(abi|host|schema)' --output-on-failure`。
  预期旧断言或minor/短结构接受测试失败，记录失败测试名。
- [ ] GREEN：宏改3/0，所有边界先size后version再尾字段；不再规范化旧host配置。
  校验核心条件为以下代码，复制错误处理入口的既有具体类型，不改CMeta ABI：

```c
if (!value || value->size != sizeof(*value)) return SALTS_EINVAL;
if (value->abi_major != 3u || value->abi_minor != 0u) return SALTS_EINVAL;
```

- [ ] 同步重编译协议/CNet/CHTTP及全部fixture，跑上述CTest并追加
  `ctest --preset win-dev-user -R 'cnet_plugin|chttp_plugin|protocol_plugins|plugin_generation|plugin_projection' --output-on-failure`。
- [ ] `git diff --check`后显式add任务文件，commit `feat(plugin): require exact shared ABI 3.0`。
  期望非operation既有功能通过，旧DLL拒绝且没有loader副作用。

### Task 2: 有界operation catalog与可信schema匹配

**Files:** Modify `turbo_flow/src/flow_plugin.c`；Create
`turbo_flow/src/flow_data_schema.c`、`turbo_flow/src/flow_plugin_operation_internal.h`、
`turbo_flow/tests/test_flow_plugin_operation.c`、`turbo_flow/tests/plugin_operation_fixture.c`、
`turbo_flow/tests/plugin_operation_fixture.h`；Modify `turbo_flow/tests/CMakeLists.txt`。
公共operation声明此时放非安装internal header，Task5原样移入公开头。

**Interfaces:** spec中`turbo_flow_plugin_operation_v3_t`及全部factory/vtable/limits/schema
完整声明；内部实现 `turbo_flow_plugin_catalog_snapshot_operation_catalog(snapshot,catalog_out)`
与 `turbo_flow_data_schema_match(left,left_data,right,right_data)`，签名同spec，不发布stub。
测试目标独立编译这些实现并链接既有依赖；生产Host仍拒绝bit8。

- [ ] 用现有schema fixture模式声明两个独立TU的同义int32 scalar metadata以及两个field
  struct，分别变更offset、bits、stable_id、alignment、字段顺序；相同地址不是通过条件。
  下列测试中的left/right及data来自这两份fixture静态描述，不手写伪CMeta布局：

```c
check_not_equal(left_data, right_data);
check_equal(turbo_flow_data_schema_match(left, left_data, right, right_data), SALTS_OK);
turbo_flow_data_schema_t changed = *right;
++changed.schema_version;
check_equal(turbo_flow_data_schema_match(left, left_data, &changed, right_data), SALTS_EPROTO);
```

- [ ] 注册fixture矩阵：0/1/N/N+1容量；同key同descriptor与不同descriptor均EALREADY；
  不同operation版本通过；input/output schema未注册、属于其他module、无效shape拒绝；
  在adapter→schema→operation各步骤注入失败，DLL吞掉add错误仍整模块回滚。
  对照snapshot的operation/schema/adapter数量都等于load前，observer无COMMIT。
- [ ] RED：新建原生CTest `test_flow_plugin_operation`，build该target并运行
  `ctest --preset win-dev-user -R '^test_flow_plugin_operation$' --output-on-failure`，
  期望缺函数或未实现语义断言失败。
- [ ] GREEN：catalog entry附带registering module归属，新增同一vec的事务尾部回滚和snapshot
  拷贝；首错检查放每个add开头，register返回后先解析schema再capability匹配再commit。

```c
if (registration->first_error != SALTS_OK) return registration->first_error;
/* operation验证函数返回spec精确定义的错误，不改变已有条目。 */
if (rc != SALTS_OK) {
  registration->first_error = rc;
  return rc;
}
```

- [ ] schema walker只接受spec固定kind、深度16/节点256/fields64；逐层比较语义，不调用
  生命周期callback、不解引用payload。节点计数/offset用减法检查溢出后访问，循环ENOTSUP。
- [ ] 运行新CTest与现有schema/host回归，确认生产root带bit8仍被拒绝；commit
  `feat(plugin): implement bounded operation catalog contract internally`。

### Task 3: Graph typed input provenance与原子result槽

**Files:** Modify `turbo_flow/include/turbo_flow_projection.h`、`turbo_flow/src/flow_message.c`、
`turbo_flow/src/flow_internal.h`、`turbo_flow/src/flow_projection_owner.c`、
`turbo_flow/src/flow_projection_owner_internal.h`、`turbo_flow/CMakeLists.txt`；
Create `turbo_flow/tests/test_flow_operation_result.c`，Modify `turbo_flow/tests/CMakeLists.txt`。

**Interfaces:** 完整实现并公开spec中的bind_typed_projection/projection_data/schema_match，
以及result_claim/commit/abort/result/clear_result；这些独立Graph API不依赖PluginHost，
全部成功/失败路径已工作才公开。复用已有projection owner容量与同步原语。
新增完整公开接口`turbo_flow_value_require_disjoint(const void *, size_t, const void *, size_t)`、
`turbo_flow_result_memory_requirements(size_t, size_t, turbo_flow_result_memory_requirements_t *)`，
成本结构及init函数按spec定义。Graph是范围判定与私有布局成本的唯一实现方。

- [ ] RED：原message绑定schema A projection并拥有descriptor，result schema B；
  记录payload内容、projection指针、descriptor、settlement callback计数。
  claim后注入NULL、input alias、OOM、owner满/stop；断言上述原字段完全不变且计数0。
  在现有owner fixture建立owner与value后使用以下准确调用顺序：

```c
turbo_flow_result_claim_t *claim = NULL;
void *value = NULL;
check_equal(turbo_flow_msg_result_claim(&msg, owner, result_data, &claim), SALTS_OK);
check_equal(turbo_flow_msg_result_commit(&claim, &value), SALTS_EPROTO);
check_not_null(claim);
check_null(turbo_flow_msg_result(&msg, NULL, NULL));
check_equal(turbo_flow_msg_projection(&msg, NULL), input_value);
turbo_flow_msg_result_abort(&claim);
check_null(claim);
```

- [ ] `cmake --build --preset win-dev-user --target test_flow_operation_result`及
  `ctest --preset win-dev-user -R '^test_flow_operation_result$' --output-on-failure`，记录RED。
- [ ] GREEN：组合content保留原字段，独立result槽预分配并reserve；commit最后一次
  pointer安装后清io。不在commit新分配，不清原projection。clone先完成临时组合再发布：

```c
/* output_size/input_size来自已通过schema验证的固定storage size。 */
const int independent = turbo_flow_value_require_disjoint(
    input_value, input_size, value, output_size);
rc = independent == SALTS_OK ? turbo_flow_msg_result_commit(&claim, &value) : SALTS_EPROTO;
if (rc != SALTS_OK) {
  if (independent == SALTS_OK) destroy_result(value, result_context);
  turbo_flow_msg_result_abort(&claim);
}
```

  callback本身失败时也先用同一helper得到independent，仅OK才销毁返回值；保留callback
  首错，alias诊断写结果phase。helper不得解引用candidate；NULL/区间溢出/重叠都不destroy。
  该示例borrowed span是当前typed input；clone以原result span代入，其他已知借用span
  也必须全部验证通过后才允许清理，不能单独比较根指针。
- [ ] 添加struct输入两个int32字段，DLL分别在成功/失败返回时给出第二字段地址，
  断言EPROTO或原callback错误、destroy_result调用0次、两个输入字段原值不变、result为空、
  abort后outstanding/retained_bytes回到调用前；clone内部地址别名同样不destroy。
- [ ] 范围helper单测覆盖相同根、内部地址、尾部交叠、完全包含、首尾相接、独立区间、
  NULL/0长度和UINTPTR_MAX加法溢出。查询成本单测验证O+C*(K+M)及C*R、capacity/R零、
  SIZE_MAX容量导致乘加溢出，错误不发布部分数值；查询过程工厂和allocation计数均0。

- [ ] 加clone原projection失败/result失败/alias/NULL、move、clear_content后result仍在、
  clear_result后descriptor仍在、cleanup只释放一次、已claim后stop仍可commit的测试。
  fault注入复用`test_flow_projection_owner_fault.c`的分配器测试方法，不加生产开关。
  bind_typed_projection验证失败仍归caller，普通projection进入typed路径ENOTSUP。
- [ ] GREEN及相邻 `ctest --preset win-dev-user -R 'operation_result|projection_owner|message|content_descriptor' --output-on-failure`；
  commit `feat(graph): commit independent typed results atomically`。

### Task 4: Caller-owned result-domain与可重试清理

**Files:** Create `turbo_flow/src/flow_plugin_result_domain.c`、
`turbo_flow/tests/test_flow_plugin_result_domain.c`；Modify
`turbo_flow/src/flow_plugin_operation_internal.h`、`turbo_flow/tests/plugin_operation_fixture.c`、
`turbo_flow/tests/CMakeLists.txt`；此任务未集成generation前仍为非安装内部实现。

**Interfaces:** spec完整result_domain_create/destroy/snapshot及snapshot struct；内部
generation独占attach/detach规则：READY→ATTACHED→DETACHED→RETIRING，最多一次attach，
无隐式计数变更。先预留owner slots，不让失败路径依赖vec增长。

- [ ] fixture为result_context提供atomic计数与一次release失败；用真实DLL callback创建
  Graph owner，绑定result并clone，释放caller snapshot。测试域attached销毁EBUSY无stop；
  detach后generation无引用，clone仍成功；domain destroy先EBUSY、clear后EIO、重试OK，
  host此时才可卸载。
- [ ] RED命令：build `test_flow_plugin_result_domain`，运行
  `ctest --preset win-dev-user -R '^test_flow_plugin_result_domain$' --output-on-failure`。
  下列断言加入实际fixture生命周期测试，state变量由spec init函数初始化：

```c
check_equal(turbo_flow_plugin_result_domain_snapshot(domain, &state), SALTS_OK);
check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED);
check_equal(turbo_flow_plugin_result_domain_destroy(domain, &error), SALTS_EBUSY);
check_equal(turbo_flow_plugin_result_domain_snapshot(domain, &state), SALTS_OK);
check_equal(state.state, (uint32_t)TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED);
```

- [ ] GREEN实现所有spec失败raw context槽：create_result_context成功但owner_create失败
  仍保存已验证release；context==factory_ctx不销毁；result/session alias只一个拥有引用。
  release失败不释放ctx/snapshot，成功才清槽；snapshot query无清理副作用。
- [ ] 用barrier停在clone/destroy callback内，验证控制线程不能仅看outstanding0就free。
  join后重试destroy；worker无snapshot retain/destroy调用，观测snapshot操作发生在线程记录的
  控制线程。覆盖容量0/1/1024/1025和重复attach失败，失败generation消耗域不能再复用。
- [ ] GREEN执行domain、projection、result回归；commit
  `feat(plugin): own independent results in explicit retirement domains`。

### Task 5: Generation装配、budget执行与失败句柄完整集成

**Files:** Modify `turbo_flow/include/turbo_flow_plugin.h`、
`turbo_flow/include/turbo_flow_plugin_operation.h`、`turbo_flow/include/turbo_flow_plugin_generation.h`、
`turbo_flow/src/flow_plugin.c`、`turbo_flow/src/flow_plugin_generation.c`、
`turbo_flow/src/flow_plugin_operation_internal.h`、`turbo_flow/CMakeLists.txt`、
`turbo_flow/tests/test_flow_plugin_generation.c`、`turbo_flow/tests/test_flow_plugin_projection.c`、
`turbo_flow/tests/plugin_generation_header_cpp.cpp`、`turbo_flow/tests/CMakeLists.txt`；
Create `turbo_flow/src/flow_plugin_operation.c`、`turbo_flow/tests/test_flow_plugin_operation_runtime.c`。
同步迁移 `io/cnet/tests/test_cnet_plugin.c`、`io/chttp/tests/test_chttp_plugin.c`、
`tests/install_consumer/main.c`、`tests/install_cnet_plugin_consumer/main.c`、
`tests/install_chttp_plugin_consumer/main.c` 的generation调用/函数指针。

**Interfaces:** 从internal移出spec所有完整operation/result-domain声明与init函数；host
operation_capacity、registration.add_operation、generation.operation_memory_budget_bytes；
spec新的generation_create 8参数、operation_error/cleanup_error查询，FAILED_CLEANUP=7。
preflight消费Task3的Graph成本查询，只在PluginHost本模块计算bridge/ledger成本H；
不得include Graph private header或复制owner/claim/content布局进行预算计算。
最终生产库统一编入所有已通过测试的实现，此时才接受bit8、移除非空bindings总ENOTSUP门。
先将spec定义的`fixture.double` Graph metadata通过`turbo_flow_register_operation`注册到
测试Graph（DATA/message→DATA/message、MESSAGE/NONE/CALL/INLINE_LANE/DATA_MUTATION、
STAGE、INLINE、DIRECT、settlement0、deadline0），再parse fixture DSL；不要假设插件
catalog自动创建Graph operation metadata。实际typed输入由bind_typed_projection建立。

- [ ] 在真实DLL fixture设置可配置模式（仅测试宏）：factory失败前/后ctx非NULL，
  result/session alias，execute失败独立临时值/alias/NULL，吞charge错误，session/context
  release一次失败。fixture操作将int32输入7转换成结果14，不碰input或terminal；调用
  charge(1)两次，max_steps=1失败、2成功。通过真实generation+Graph provider调用，
  不用直接调用fixture execute冒充集成。
- [ ] RED场景表：每项断言Graph指针仍为original、两个out均NULL、工厂计数0：

| 输入 | 期望 |
|---|---|
| operation/plugin/version/schema不存在或类型不匹配 | EINVAL或EPROTO，明确对应binding字段 |
| config owner/thread/coro/cooperative/deadline>0 | ENOTSUP |
| 非空permissions、缺steps guarantee、emit/keyed/settlement/effect不符 | ENOTSUP |
| max_inflight或bytes/steps超过声明，域容量不足，C<max_inflight | ENOSPC |
| 元数据size/version非法、乘加溢出、两out别名、非READY域 | EINVAL |
| 任意provider preflight失败 | 原错误码，全部factory0 |

- [ ] 内存预算用真实Graph成本查询复算required；预算=required时通过，预算=required-1
  时ENOSPC；Graph查询capacity乘法/加法溢出、多个binding合计溢出时EINVAL。
  所有拒绝都断言Graph未转移、domain仍READY、两个out为NULL、全部factory计数0。
  query返回的peak_metadata_bytes变化必须自然反映到required，不允许测试/Host维护另一
  份硬编码Graph每槽成本。

- [ ] build `test_flow_plugin_operation_runtime`再
  `ctest --preset win-dev-user -R '^test_flow_plugin_operation_runtime$' --output-on-failure`，记录RED。
- [ ] 实现atomic admission、有限budget与缓存callback；charge的核心完整判定为：

```c
typedef struct flow_plugin_operation_budget_state_s {
  uint32_t max_steps, used;
  int status;
} flow_plugin_operation_budget_state_t;
static int flow_plugin_operation_charge(void *ctx, uint32_t steps) {
  flow_plugin_operation_budget_state_t *budget_state = ctx;
  if (budget_state->status != SALTS_OK) return budget_state->status;
  if (steps == 0u) return (budget_state->status = SALTS_EINVAL);
  if (steps > budget_state->max_steps - budget_state->used)
    return (budget_state->status = SALTS_ENOSPC);
  budget_state->used += steps;
  return SALTS_OK;
}
```

  每个同步调用栈上初始化`{configured_max_steps, 0u, SALTS_OK}`，ctx只在该调用内有效。

- [ ] 执行顺序严格为schema验证→inflight accept→result claim→execute→首错/NULL/alias
  验证→commit/销毁临时值+abort→inflight release。每种return检查destroy计数、reservation
  回归0、原payload/descriptor/settlement未变。最近错误复制到binding mutex保护的固定槽。
  success/failure返回输入第二字段地址均通过Task3同一范围helper处理；无法证明独立时
  destroy0，只abort并归还reservation，不因callback返回OK就信任其拥有权。
- [ ] 改create事务：所有preflight成功才attach和转移；所有ctx先入预留ledger。替换void
  rollback为spec同一退役流程；失败out规则用以下调用点模式同步迁移，不能丢cleanup：

```c
turbo_flow_plugin_generation_t *generation = NULL;
turbo_flow_plugin_generation_t *cleanup = NULL;
turbo_flow_config_error_t cleanup_error = TURBO_FLOW_CONFIG_ERROR_INIT;
int rc = turbo_flow_plugin_generation_create(snapshot, resolved, &flow, &config,
                                             domain, &generation, &cleanup, &error);
if (rc != SALTS_OK && cleanup) {
  /* 调用方已有lifecycle owner保存此句柄，后续控制面重试同一对象。 */
  int cleanup_rc = turbo_flow_plugin_generation_destroy(cleanup, 0u, &cleanup_error);
  if (cleanup_rc == SALTS_OK) cleanup = NULL;
  /* 非OK的cleanup仍由当前调用方状态持有，不能在函数返回时丢失。 */
}
```

  上述变量必须属于现有调用者的生命周期状态，不能是返回即遗失的临时局部owner；
  测试fixture context增加cleanup字段，close失败保留该字段和host/domain，禁止如旧
  test_close那样错误返回仍把host设NULL。无operation消费者传domain=NULL，但也必须
  保存cleanup_out；函数指针消费者更新完整8参数类型。结果域每次create失败也由外层
  owner显式destroy，不因cleanup_out为NULL跳过域清理。

- [ ] 测试compile失败、第二factory失败、session release EIO、Graph stop失败、lease忙、
  execute callback barrier未返回；查询FAILED_CLEANUP只读，重试不重复成功transition。
  domain在cleanup未完成仍ATTACHED，全部退役后DETACHED；失败generation不能start。
- [ ] generation销毁成功而result仍在，先销毁resolved/caller snapshot，再clone读取14，
  domain destroy EBUSY；clear结果后release_context失败重试，最后DLL卸载。证明generation
  不持有result的最后lease；证明session先释放不影响result_context。
- [ ] GREEN运行runtime、generation、host、schema、projection、result/domain及CNet/CHTTP。
  重新检索全部generation_create调用与2u硬断言，确认函数指针消费者同步；commit
  `feat(plugin): execute ABI3 operations through transactional generations`。

### Task 6: 安装消费者、双profile与边界文档收口

**Files:** Modify `tests/install_consumer/main.c`、`tests/install_cnet_plugin_consumer/main.c`、
`tests/install_chttp_plugin_consumer/main.c`、`tests/plugin_host_abi_test.c`、
`cmake/TurboFlowConfig.cmake.in`（只在新公开CMeta依赖确需传播时）、
`docs/architecture/typed-operation-plugins.md`、`docs/architecture/typed-operation-abi3.md`；
必要测试注册在根`CMakeLists.txt`和既有`cmake/`安装验证文件中，复用既有消费preset。

**Interfaces:** 已实施ABI3及Graph新增公开函数；安装消费只能使用安装头/导入target，
fixture DLL唯一导出canonical entry。不提前修改#73旧引擎闭包或安装引擎SDK。

- [ ] 安装消费者增加新init函数、result-domain与新generation签名的C/C++编译/链接。
  动态加载真实fixture并验证7→14、generation退出后clone、域释放后卸载；导出表仅
  turbo_flow_plugin_get_api，失败依赖根明确报错。`tests/plugin_host_abi_test.c`同时验证
  CNet/CHTTP/protocol root与host-service未来minor拒绝，无allocation。
- [ ] RED先运行对应现有install-consumer CTest，确认旧消费者缺符号/签名断言能失败。
  获取实际注册名：`ctest --preset win-dev-user -N -R 'install.*consumer|plugin.*abi'`，
  不虚构消费preset，不用裸临时工程绕过manifest。
- [ ] GREEN按profile匹配依赖根运行，不复制runtime DLL，不改变编译器/sanitizer选项：

```text
cmake --preset win-dev-user
cmake --build --preset win-dev-user
ctest --preset win-dev-user --output-on-failure
cmake --preset win-release-user
cmake --build --preset win-release-user
ctest --preset win-release-user --output-on-failure
ctest --preset win-dev-user -R 'plugin_operation|plugin_result_domain|operation_result' --repeat until-fail:20 --output-on-failure
ctest --preset win-release-user -R 'plugin_operation|plugin_result_domain|operation_result' --repeat until-fail:20 --output-on-failure
```

- [ ] 以上全量build/CTest均exit0后，按既有安装消费测试的staging方式验证；仅最终SDK发布
  授权范围内才运行`cmake --build --preset install-win-dev-user`与
  `cmake --build --preset install-win-release-user`，不在本设计任务安装任何产物。
  若安装测试本身生成独立staging而无需覆盖SDK，使用该已存在机制直接完成验证。
- [ ] 文档将“首profile已实施”与未完成项区分；记录真实测试数、失败及环境限制。
  ASan不能证明无数据竞争；barrier和线程计数测试证明所覆盖协议，不宣称全局race-free。
- [ ] `git diff --check`，显式commit `test(plugin): verify ABI3 installed consumers and lifecycle`。

## 自审验收清单

- [ ] ABI所有wrapper/host/registration/owner/query的major/minor/size拒绝：Task1/6。
- [ ] catalog重复/容量/首错粘住/吞错/整模块回滚/来源/多TU语义：Task2。
- [ ] 输入metadata可信绑定、原descriptor保留、结果失败原子性：Task3/5。
- [ ] 内部地址alias无invalid free、验证与清理共享checked范围helper：Task3/5。
- [ ] Graph唯一私有成本查询、精确预算/少一字节/乘加溢出在factory前验证：Task3/5。
- [ ] 独立结果越过generation、clone、worker不动snapshot、busy和release重试：Task4/5。
- [ ] preflight不移Graph、权限/effect/thread/cancel/quota拒绝、steps强制观察：Task5。
- [ ] cleanup_out不丢、factory异常输出、Graph compile失败与逆序退役：Task5。
- [ ] 真实DLL/Graph、安装消费者唯一导出、Debug/ASan与Release全量build/CTest：Task6。
- [ ] #93余项与#73引擎迁移保持开放；没有ENOTSUP公共stub或把未完成能力标为可执行。
