# Typed-operation ABI 3.0 精确契约

状态：首个同步 ABI 3.0 profile 已实施；事实基线为 `91fdf62ad9845e287a3bc773081a3f6a088b6ea8`。
用户批准 shared ABI 3.0、拒绝旧 ABI/短布局、插件与消费者同步重编译。
本文件定义 #93 首个完整同步 profile；#93 的 owner/thread/coro/cooperative 和
#73 的真实 RulesForge/TurboScript、旧入口删除、Core 链接解耦继续开放。

## 证据、候选与决策

| 级别/证据 | 现状与影响 | 决策 |
|---|---|---|
| HIGH 事实 | `flow_message.c:313` bind 拒绝已有 value；`:371` clear 保留 descriptor；`:48` descriptor 校验要求原 schema | 不替换原 projection；新增独立结果槽，提交不改变 payload、descriptor、route、settlement |
| HIGH 事实 | `flow_plugin_generation.c:271` 的 `flow_plugin_generation_rollback` 为 void，直接 destroy 所有 owner | 用同一可重试退役状态机处理构建失败，显式返回 cleanup 句柄 |
| HIGH 事实 | `flow_plugin.c:1212` snapshot destroy 和 `:1232` retain 为普通控制面计数 | 结果域在控制线程持有一次 snapshot；worker 只触及 Graph owner 原有同步 reservation |
| MED 事实 | `turbo_flow.h:1634` 1:1 provider 回调接收 mutable msg；`:1676` emitting 限定独立无 transport 输出；`:1702` keyed 涉及 node 状态事务 | 选 1:1 provider 包装；emit 会改变消息/settlement 语义，keyed 增加不需要的状态事务，均不采用 |
| HIGH 事实 | `turbo_flow_data_schema_t` 只有 schema/type/projection 字符串，没有 CMeta storage descriptor | 新增可信 typed projection 绑定入口；绝不按字符串把既有 void* 强制当新布局 |
| MED 推论 | 异步可取消 invocation 需要 retained input、等待/唤醒、取消竞争与运行器对接，当前同步 provider 不提供这些能力 | 本 profile 仅 inline + thread_safe + cancellation none + deadline 0；拒绝其他请求，不阻塞等待自建 executor |

结果域候选：让 generation destroy 因正常输出滞留而 EBUSY，虽安全却把整个 Graph
寿命绑定给输出；全局 orphan 表引入隐式事实源；选择调用方显式持有 result-domain。
域仅承载结果 owner、已 pin 的 snapshot 和退役游标，generation 成功销毁不影响结果。
Graph 不依赖 PluginHost；新增 Graph API 只接受 Graph owner 和 CMeta 元数据。

## ABI 基本规则

`TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR=3u`，minor `0u`。所有 shared size/version
结构必须 `size == sizeof(T)`、major 3、minor 0；旧版本、未来 minor/major、短结构、
过长未知布局均 EINVAL，读取尾字段或调用 callback 之前检查。root、host services、
registration、provider wrappers、owner、catalog、配置和错误输出逐一校验。
只允许先读取合法内存中的 size；size 小于版本头前不得读取版本。无自动布局转换。
已有 `_v1_t` 名称保留为类型名，绝不保留旧二进制布局路径。CMeta 自身 ABI 不改，
Graph projection 独立 ABI 1.0 也不变；传入插件边界时其完整结构仍须验证。
TurboFlow package 2.0 与插件 ABI 3.0 独立，不自动提升包版本。

Product owner publish 的源和目标都必须预置精确 ABI 3.0；先验证目标 size、再验证目标
版本，之后才检查源并复制。任一头部不兼容返回 EINVAL，目标全部字节保持不变。
operation 的 preflight、create_result_context、create_session、execute 每次返回后，
Host 必须先检查 error 的精确 size，再检查 major/minor，之后才读取诊断尾字段、
进入后续业务 callback 或提交结果。非法 error 头优先返回 EINVAL，即便 callback
同时返回其他错误；Host 不读取或保留未知头的 status/engine_status/phase/message，
而以全新 Host 诊断记录当前边界 phase、EINVAL、engine_status=0。已独立返回的
context/session/result 仍进入原有安全清理或重试路径，借用 alias 不释放。只有合法
error 头才适用保留原 callback 错误与 phase 的规则，不允许修补未知头伪装成合法输出。

以下 C 声明放入 `turbo_flow_plugin_operation.h`，依赖该文件既有 Graph/CMeta 头，
以及 `turbo_flow_resolved_config.h`。所有 enum 字段实际使用 uint32_t，避免 C enum
宽度作为 DLL 布局；返回值采用既有 SALTS 错误码，不重编号。
下列每个含 size/abi 的新结构提供静态 inline 初始化函数：先 `{0}`，再令 size=sizeof(T)、
abi_major=3、abi_minor=0；这是 C11/C++ 兼容的逐字段初始化函数实现规则，不能 memcpy
短前缀。具体初始化函数名为类型去掉 `_t` 后加 `_init(T *out)`，返回 void，out 必须非 NULL。
含嵌套wrapper的operation/request初始化函数还调用每个嵌套类型的init；其余字段为零。
例如 `turbo_flow_plugin_operation_v3_init(turbo_flow_plugin_operation_v3_t *out)`。

```c
typedef struct turbo_flow_plugin_result_domain_s turbo_flow_plugin_result_domain_t;
typedef struct turbo_flow_plugin_error_s turbo_flow_plugin_error_t;
enum {
  TURBO_FLOW_PLUGIN_CAP_OPERATION = UINT64_C(1) << 8,
  TURBO_FLOW_PLUGIN_OPERATION_SYNC = 1u,
  TURBO_FLOW_PLUGIN_OPERATION_THREAD_SAFE = 1u,
  TURBO_FLOW_PLUGIN_OPERATION_CANCEL_NONE = 0u,
  TURBO_FLOW_PLUGIN_OPERATION_EFFECT_RESULT = 1u,
  TURBO_FLOW_PLUGIN_OPERATION_STEPS_CHARGED = 1u,
  TURBO_FLOW_PLUGIN_OPERATION_MAX_PERMISSIONS = 32u,
  TURBO_FLOW_PLUGIN_OPERATION_MAX_SCHEMA_DEPTH = 16u,
  TURBO_FLOW_PLUGIN_OPERATION_MAX_SCHEMA_NODES = 256u,
  TURBO_FLOW_PLUGIN_OPERATION_MAX_STRUCT_FIELDS = 64u
};
typedef struct turbo_flow_plugin_operation_schema_v3_s {
  size_t size; uint32_t abi_major; uint32_t abi_minor;
  uint32_t schema_version;
  const cmeta_data_desc *data;
  const turbo_flow_data_schema_t *projection;
} turbo_flow_plugin_operation_schema_v3_t;
typedef struct turbo_flow_plugin_operation_limits_v3_s {
  size_t size; uint32_t abi_major; uint32_t abi_minor;
  uint32_t max_inflight;
  size_t max_input_bytes, max_result_bytes, max_retained_bytes;
  uint32_t max_steps;
  uint32_t deadline_ms;
} turbo_flow_plugin_operation_limits_v3_t;
typedef struct turbo_flow_plugin_operation_request_v3_s {
  size_t size; uint32_t abi_major; uint32_t abi_minor;
  const turbo_flow_resolved_config_t *resolved;
  const char *operation_name;
  const char *resource_name;
  turbo_flow_plugin_operation_limits_v3_t limits;
} turbo_flow_plugin_operation_request_v3_t;
typedef struct turbo_flow_plugin_operation_input_v3_s {
  size_t size; uint32_t abi_major; uint32_t abi_minor;
  const void *value;
  const cmeta_data_desc *data;
  size_t bytes;
} turbo_flow_plugin_operation_input_v3_t;
typedef int (*turbo_flow_plugin_operation_charge_fn)(void *ctx, uint32_t steps);
typedef struct turbo_flow_plugin_operation_budget_v3_s {
  size_t size; uint32_t abi_major; uint32_t abi_minor;
  void *ctx;
  turbo_flow_plugin_operation_charge_fn charge;
} turbo_flow_plugin_operation_budget_v3_t;
typedef struct turbo_flow_plugin_operation_error_v3_s {
  size_t size; uint32_t abi_major; uint32_t abi_minor;
  int status;
  int64_t engine_status;
  uint32_t phase;
  char message[256];
} turbo_flow_plugin_operation_error_v3_t;
enum {
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_NONE = 0u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_PREFLIGHT = 1u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT_CONTEXT = 2u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_SESSION = 3u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_EXECUTE = 4u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_RESULT = 5u,
  TURBO_FLOW_PLUGIN_OPERATION_PHASE_RELEASE = 6u
};
typedef int (*turbo_flow_plugin_operation_preflight_fn)(void *factory_ctx,
  const turbo_flow_plugin_operation_request_v3_t *request,
  turbo_flow_plugin_operation_error_v3_t *error);
typedef int (*turbo_flow_plugin_operation_result_context_create_fn)(void *factory_ctx,
  const turbo_flow_plugin_operation_request_v3_t *request, void **context_out,
  turbo_flow_plugin_operation_error_v3_t *error);
typedef int (*turbo_flow_plugin_operation_session_create_fn)(void *factory_ctx,
  const turbo_flow_plugin_operation_request_v3_t *request, void *result_context,
  void **session_out, turbo_flow_plugin_operation_error_v3_t *error);
typedef int (*turbo_flow_plugin_operation_execute_fn)(void *session,
  const turbo_flow_plugin_operation_input_v3_t *input,
  const turbo_flow_plugin_operation_budget_v3_t *budget,
  void **result_out, turbo_flow_plugin_operation_error_v3_t *error);
typedef int (*turbo_flow_plugin_operation_release_fn)(void *ctx);
typedef struct turbo_flow_plugin_operation_vtable_v3_s {
  size_t size; uint32_t abi_major; uint32_t abi_minor;
  turbo_flow_plugin_operation_execute_fn execute;
  turbo_flow_projection_clone_fn clone_result;
  turbo_flow_destroy_fn destroy_result;
  turbo_flow_plugin_operation_release_fn release_session;
  turbo_flow_plugin_operation_release_fn release_result_context;
} turbo_flow_plugin_operation_vtable_v3_t;
typedef struct turbo_flow_plugin_operation_v3_s {
  size_t size; uint32_t abi_major; uint32_t abi_minor;
  const char *operation_name;
  uint32_t operation_version;
  turbo_flow_plugin_operation_schema_v3_t input, output;
  uint32_t execution, threading, cancellation, effects, guarantees;
  const char *const *permissions;
  size_t permission_count;
  turbo_flow_plugin_operation_limits_v3_t limits;
  size_t max_session_bytes, max_result_context_bytes;
  void *factory_ctx;
  turbo_flow_plugin_operation_preflight_fn preflight;
  turbo_flow_plugin_operation_result_context_create_fn create_result_context;
  turbo_flow_plugin_operation_session_create_fn create_session;
  turbo_flow_plugin_operation_vtable_v3_t vtable;
} turbo_flow_plugin_operation_v3_t;
typedef int (*turbo_flow_plugin_add_operation_fn)(void *ctx,
  const turbo_flow_plugin_operation_v3_t *operation);
typedef struct turbo_flow_plugin_operation_catalog_entry_v3_s {
  const char *plugin_id;
  turbo_flow_plugin_operation_v3_t operation;
} turbo_flow_plugin_operation_catalog_entry_v3_t;
typedef struct turbo_flow_plugin_operation_catalog_v3_s {
  size_t size; uint32_t abi_major; uint32_t abi_minor;
  const turbo_flow_plugin_operation_catalog_entry_v3_t *entries;
  size_t count;
} turbo_flow_plugin_operation_catalog_v3_t;
TURBO_FLOW_C_API int turbo_flow_plugin_catalog_snapshot_operation_catalog(
  const turbo_flow_plugin_catalog_snapshot_t *snapshot,
  turbo_flow_plugin_operation_catalog_v3_t *catalog_out);
```

`turbo_flow_plugin_host_config_t` 在 schema_capacity 后增加 `size_t operation_capacity`，
INIT 默认64，上限65536，0允许且首个 add 返回 ENOSPC。
`turbo_flow_plugin_registration_v1_t` 在 add_schema 后增加
`turbo_flow_plugin_add_operation_fn add_operation`。root 导出仍唯一
`turbo_flow_plugin_get_api`；capability 原值0..7不变，operation为bit8。
catalog、result-domain与generation执行作为一个未发布的集成单元实施；最终ABI类型
只在公开头定义一次，不增加影子头、条件布局或第二注册入口。完整执行链路通过前不提交
可发布的中间实现、不安装ABI3 SDK，完成集成时才统一开放bit8与非空binding执行准入。
分阶段保留catalog/domain/runtime测试检查点；不发布ENOTSUP公共stub。

## Catalog、来源与 schema

唯一事实源是 `flow_plugin.c` 有界 catalog。operation key 为
`(root.plugin_id, operation_name, operation_version)`；config.version 是 operation
数值版本，与 root.plugin_version 字符串无关。同key一律 EALREADY（即便相同 descriptor），
不同 operation 版本可并存；Graph 同 `(operation, resource)` 不允许多 provider。
schema key 维持 `(data.stable_id, schema_version)` 全 host 唯一，同key EALREADY。
先允许本次事务暂存 operation，再在 register 返回后解析 schema：input/output 所用
schema 必须由同一 registering module 在本次/既有自有条目提供，不能借其他 DLL 的
裸 callback。复制 wrapper，冻结指向模块静态 metadata 的引用。factory_ctx 由该 root
实例拥有；所有工厂/vtable/metadata/字符串及递归数据描述必须属于该模块或其加载器
已固定的原生依赖闭包，不得引用可独立卸载的其他 PluginHost 模块。
这是可信 native DLL 契约和 fixture 审计边界，不是假称可认证任意函数指针来源；host
能验证 catalog module归属和snapshot包含该module，不能检测恶意DLL伪造地址。

注册首错粘住；验证 descriptor、重复、容量、schema、capability及回调返回码。任何
失败（包含DLL吞错）回滚该模块全部类别尾部，再执行现有root逆序清理；不提交部分条目。
snapshot 复制 operation wrapper 与 plugin_id引用，pin 所有对应模块。
查询要求live snapshot和完整INIT输出；成功借用数组，失败输出保持初始化空状态。
这里新结构的“INIT”指调用上文对应的`*_init`函数，既有结构继续用既有INIT宏。

首个profile接收 BOOL/SINT/UINT/FLOAT 和这些类型组成的 STRUCT；其他CMeta kind
明确 ENOTSUP，不把名字相同视为布局相同。最多16层、256节点、每struct64字段；拒绝循环。
纯值、无指针/外部借用、对齐和大小在preflight证明；scalar检查kind和shape.bits；
每层检查cmeta_data_desc_valid、cmeta_type_equal、storage size/alignment、stable_id。
struct按声明顺序比较field_count、每field stable_id/name/offset和递归value，检查
offset <= parent_size 且 child_size <= parent_size-offset。不能只比较storage_type。
display_name不构成身份；shape/layout/descriptor/callback地址不构成语义身份。
定义 Graph 导出比较函数，失败区分错误参数、不可支持shape、语义不同：

```c
TURBO_FLOW_C_API int turbo_flow_data_schema_match(
  const turbo_flow_data_schema_t *left, const cmeta_data_desc *left_data,
  const turbo_flow_data_schema_t *right, const cmeta_data_desc *right_data);
TURBO_FLOW_C_API int turbo_flow_msg_bind_typed_projection(turbo_flow_msg_t *msg,
  const turbo_flow_data_schema_t *schema, const cmeta_data_desc *data,
  void *value, turbo_flow_projection_clone_fn clone,
  turbo_flow_destroy_fn destroy, void *ctx);
TURBO_FLOW_C_API const cmeta_data_desc *
  turbo_flow_msg_projection_data(const turbo_flow_msg_t *msg);
```

match 比较 domain/encoding/schema_name/type_name/projection_type/schema_id/schema_version
全部精确值（字符串按内容）、CMeta上述语义；schema_text不比较。schema_name必须等于
data.stable_id，schema_version必须等于catalog版本；projection_type等于storage_type->name。
返回OK/EINVAL/ENOTSUP/EPROTO。Graph新增CMeta公开依赖以保持头和安装
消费者闭包正确（若目标已有该依赖则复用）。算法O(节点+字段)，有上述硬上限。

bind_typed_projection 是可信输入适配器的声明边界，先完整验证再按原bind所有权规则
提交；失败value仍属于caller。metadata/string与原借用projection同寿命，clone保留。
普通bind_projection没有CMeta证明，projection_data返回NULL；operation拒绝ENOTSUP，
不能从catalog反推裸指针真实布局。preflight验证catalog↔operation↔config；运行时
在execute前验证可信message metadata↔已缓存input，bytes取已证明固定storage size。
原descriptor独立按已有规则验证和持有，不因为附加结果而扩展其借用期限。

## Graph 原子结果槽

这些新增声明位于 `turbo_flow_projection.h`，实现仅在Graph，供普通Graph消费者复用。
一个message最多一个operation result；重复operation阶段不隐式覆盖，返回EALREADY，
调用方可显式clear_result后再执行。result不是第二份payload事实源，是只读决策建议。

```c
typedef struct turbo_flow_result_claim_s turbo_flow_result_claim_t;
TURBO_FLOW_C_API int turbo_flow_value_require_disjoint(
  const void *borrowed, size_t borrowed_bytes,
  const void *candidate, size_t candidate_bytes);
typedef struct turbo_flow_result_memory_requirements_s {
  size_t size;
  uint32_t abi_major, abi_minor;
  size_t owner_bytes;
  size_t claim_bytes;
  size_t message_bytes;
  size_t peak_metadata_bytes;
  size_t payload_bound_bytes;
} turbo_flow_result_memory_requirements_t;
static inline void turbo_flow_result_memory_requirements_init(
  turbo_flow_result_memory_requirements_t *out) {
  turbo_flow_result_memory_requirements_t initial = {0};
  initial.size = sizeof(initial);
  initial.abi_major = TURBO_FLOW_PROJECTION_ABI_MAJOR;
  initial.abi_minor = TURBO_FLOW_PROJECTION_ABI_MINOR;
  *out = initial;
}
TURBO_FLOW_C_API int turbo_flow_result_memory_requirements(
  size_t capacity, size_t max_result_bytes,
  turbo_flow_result_memory_requirements_t *out);
TURBO_FLOW_C_API int turbo_flow_msg_result_claim(turbo_flow_msg_t *msg,
  turbo_flow_projection_owner_t *owner, const cmeta_data_desc *data,
  turbo_flow_result_claim_t **claim_out);
TURBO_FLOW_C_API int turbo_flow_msg_result_commit(turbo_flow_result_claim_t **claim_io,
  void **value_io);
TURBO_FLOW_C_API void turbo_flow_msg_result_abort(turbo_flow_result_claim_t **claim_io);
TURBO_FLOW_C_API const void *turbo_flow_msg_result(const turbo_flow_msg_t *msg,
  const turbo_flow_data_schema_t **schema_out, const cmeta_data_desc **data_out);
TURBO_FLOW_C_API void turbo_flow_msg_clear_result(turbo_flow_msg_t *msg);
```

claim需要exclusive msg、live owner、schema匹配的data。先reserve owner count/bytes，
再分配私有槽，失败撤销reservation，msg完全不动；有result返回EALREADY，容量满ENOSPC，
stop后EBUSY，OOM为ENOMEM。claim_out失败NULL。临时claim记住msg与owner，调用方从
claim到commit/abort期间不得移动、clone、cleanup或改变msg（包括原projection）。
claim内不存DLL session。private `_content_handle` 扩充成含原projection、descriptor和
独立result字段的组合对象；若原content为空，claim预分配组合对象，直到commit才挂入msg。

commit检查非空独立value和原msg未变，NULL/与原projection或已有result根指针别名
返回EPROTO；失败claim/value仍由caller拥有，可销毁独立value再abort。成功只交换准备好
的result槽，不分配、不再次reserve、不调用DLL；清空两个io参数。claim已获准后owner
stop不撤销它，允许完成。abort不销毁caller value，只释放私有claim和reservation，
接受NULL。数据不允许内嵌指针，本profile没有无法检测的嵌套alias；与input根alias返回
EPROTO且绝不可用result destroy释放输入。typed输入还要检查完整固定storage地址范围
重叠（先验证uintptr_t加法不溢出）；指向输入内部字段的输出同样是alias，不可销毁。
范围检查的唯一实现是Graph公开`turbo_flow_value_require_disjoint`：两指针必须非NULL、
两长度非零且可表示为uintptr_t，分别检查base<=UINTPTR_MAX-length；不满足返回EINVAL。
两个半开区间`[base, base+length)`有交集返回EPROTO，否则OK。仅进行整数比较，不解引用
candidate，首尾相接属于不重叠。它证明范围独立，不认证任意DLL指针是否来自合法分配。
helper的EINVAL是范围参数错误；已验证input/layout的operation和commit边界将无效candidate
（包括NULL、地址加法溢出）统一转换为EPROTO，避免改变成功返回NULL的既定错误语义。
execute验证、失败输出清理、Graph commit和result clone必须调用同一个函数；只有返回OK
才能按可信DLL独立分配契约销毁candidate。NULL、重叠或无法证明范围独立都不得destroy，
只记录错误并abort/归还reservation；不得改用`candidate != borrowed`的另一套判定。
execute的borrowed_bytes取可信input storage size，candidate_bytes取已验证output storage
size；result clone取原result和output同一固定storage size。对commit可达的每个已知借用
span都要通过此函数；未经typed绑定且无可信长度的普通projection，仅凭不同根地址不能
证明独立，typed-result commit返回ENOTSUP，调用方仍按自身原始所有权协议处理value。
本profile的typed输入已提供完整长度，因此不会走这条拒绝分支。

clone必须先在临时目标完整clone原projection和result；任一失败销毁已成功的独立临时值，
保留source不变，目标按现有clone失败契约清空。result clone预reserve同一owner，拒绝
NULL与source alias；move转移组合对象，无retain。clear_projection/clear_content仅处理
原content，保留result；clear_result仅释放result；cleanup恰好各释放一次，不重复settlement。
复制descriptor仍只按原API复制结构，不深复制其外部借用。bytes/count在payload destroy
返回以后才归还。结果schema来自owner复制wrapper，data来自domain pin的不可变模块。

`turbo_flow_msg_retain_view(dst,src)`不共享拥有型result。只要src有result，必须在分配
content副本、修改dst或retain buffer之前返回SALTS_EINVAL；即便此前已调用
clear_projection或clear_content，独立result仍使该调用被拒绝。失败不改变src/dst、
owner outstanding/retained_bytes、buffer引用计数，不调用result clone/destroy。
携带result的消息只能用clone创建独立值，或用move转移所有权。无result时保留既有
retain_view规则及成功路径，包括清除原projection后保留descriptor的buffer借用视图。
依据：`flow_message.c:227`目前只检查owned_payload和原projection，`:234`浅复制content；
不增加此准入检查会浅复制新增result/owner而不reserve，造成重复释放及额度下溢。

## 执行准入、额度与错误

支持值必须 execution=SYNC(1)、threading=THREAD_SAFE(1)、cancellation=NONE(0)、
effects=RESULT(1)、guarantees=STEPS_CHARGED(1)。descriptor未知值ENOTSUP；config
execution必须inline、threading必须thread_safe、cancellation必须none、deadline_ms=0。
声明不支持和配置不支持均preflight ENOTSUP且不创建任何实例。不得用调用后耗时检查
冒充deadline。cooperative/cancel与complete竞争在本profile不存在：请求被提前拒绝，
已接受的同步调用仅有一次返回，stop不取消它；以后增加异步需另定ABI，不发布占位标签。

operation必须是既有domain注册的stage、1:1 MESSAGE、无settlement、DIRECT INLINE、
DATA_MUTATION授权，provider options为MUTATES_IN_PLACE和EFFECT_NONE；只改变附加result，
不设置message decision/terminal。预先检查DSL execution/effect/runtime contract一致。
Graph descriptor继续是优化屏障；不把DLL callback标pure，不融合/重排。
无节点/Graph业务状态、无keyed、无emit、无跨domain bridge。不符合返回ENOTSUP。
Graph的operation metadata由调用方在parse/compile前通过既有
`turbo_flow_register_operation`注册，本profile不由DLL私自添加新Graph词汇。
metadata.name/version必须与binding选择一致；input_domain/output_domain及input_type/
output_type必须分别相等，因为主消息仍携带原内容；result schema是附加结果的类型，
不得冒充Graph主输出类型改变下游类型链。metadata缺失EINVAL，版本或类型契约不符EPROTO。
测试fixture通过此公开入口注册`fixture.double`的DATA/message→DATA/message契约，
验证新增result不影响Graph边的原类型。
operation绑定的resource若非NULL，必须精确匹配DSL和resolved已有channel；request中
resolved只借用到工厂调用返回，工厂复制需要的配置/加载有界artifact，禁止保留指针。

首profile无宿主I/O服务：descriptor permissions必须空数组/count0，binding permissions
必须空；非空明确ENOTSUP，且注册校验标识符/去重/32项上限。不得把列表忽略后执行。
未来权限服务属于#93余项，不能由原生DLL白名单推论已经有沙箱/崩溃隔离。

limits范围沿用resolved config：max_inflight 1..1048576；bytes 1..1073741824；
max_steps 1..UINT32_MAX；deadline为0。配置每项<=descriptor上限，否则ENOSPC；
input固定storage size<=max_input_bytes；output固定storage size<=max_result_bytes。
host在invoke之前用独立原子inflight admission计数，满立即ENOSPC；每个accept恰好减一次。
先claim结果再执行，payload(含allocation overhead的可信上界)每份固定收费R=max_result_bytes。
owner容量C=floor(max_retained_bytes/R)，额外受1048576上限；config要求C>=max_inflight，
否则ENOSPC。clone与执行共用这一个Graph count/bytes事实源。没有另一份插件内收费计数。

计算：result保留上限=C*R<=max_retained_bytes。Graph私有大小只能由
`turbo_flow_result_memory_requirements(C,R,&cost)`计算，PluginHost不得复制私有结构、
使用猜测的sizeof或硬编码每槽字节数。查询不分配、不建owner、不调用DLL，不改变状态。
out由`turbo_flow_result_memory_requirements_init`初始化：清零后size=sizeof(T)，
abi_major/minor使用Graph的TURBO_FLOW_PROJECTION_ABI_MAJOR/MINOR（当前1/0），不是插件3/0。
完整size及精确版本必须匹配；有效out在参数失败时所有成本字段清零。capacity/R非零，
所有乘加先checked，溢出或非法参数EINVAL。查询不限制capacity为1048576，以便通用Graph
用户做预检；PluginHost另验证其profile上限。ENOMEM不能由纯查询产生。

返回值单位全为字节，定义如下：O=owner_bytes为一个Graph owner的直接分配请求大小；
K=claim_bytes为一个尚未commit的claim私有存储请求大小；M=message_bytes为一份完整
组合content/result槽的直接分配请求大小。Graph实现从自己的最终布局计算O/K/M，
`peak_metadata_bytes=O+C*(K+M)`，`payload_bound_bytes=C*R`。这是有界保守峰值：允许
每个reservation同时持有claim和一个目标message槽，成功commit后claim必须立即释放；
clone源的result占自己的reservation，目标另占一个，故不另加隐式双倍容量。原输入
已有content仍归上游input预算；这里始终计满目标组合槽，不能因为复用而降低准入额度。
如果实现新增Graph直接分配或改变同时存活对象数，必须同步调整查询公式和边界测试。
这些数值是Graph直接请求的存储字节，非进程RSS；allocator元数据、Salts opaque mutex
后端/OS内核对象不属于可由Graph sizeof计算的字节值，不得将此查询称为进程总内存硬限。
这些外部资源仍受每owner一个mutex及C个reservation的计数上限约束。

所有bindings预算相加也checked；查询输出是Graph bookkeeping的唯一成本事实源。
session与result_context各一个/binding，descriptor max_session_bytes/max_result_context_bytes
各1..1GiB；generation_config增加 `size_t operation_memory_budget_bytes`（默认64MiB），
包括所有session/context声明上界及Graph查询的peak_metadata_bytes，不含已单独收费payload；
超过预算ENOSPC、算术溢出EINVAL。每个callback分配仍由可信DLL兑现声明，不能拦截native
malloc或保证恶意DLL不超限；真实引擎必须以原生allocator/资源预算证明才能验收。
命名常量为`TURBO_FLOW_PLUGIN_OPERATION_MEMORY_BUDGET_DEFAULT = 67108864u`；配置为0
且有bindings时ENOSPC。domain控制面的entry数组内存按capacity预留，独立于每generation
预算，create检查capacity*sizeof(entry)溢出。两个额度相加给出保守上界：域已预留的
每binding bridge仍计入generation准入，因此包含重叠收费，不是互斥实际分配量或RSS。
PluginHost自身的projection bridge和每binding执行/错误ledger由其实际实现计算成本H，
同样计入operation_memory_budget_bytes，不复制Graph大小。
当前H为`sizeof(flow_plugin_operation_binding_t)+sizeof(flow_plugin_result_entry_t)`。
准确准入公式为
`required=sum(max_session_bytes + max_result_context_bytes + cost.peak_metadata_bytes + H)`。
在所有工厂之前checked求和并比较预算；恰好required通过，required-1返回ENOSPC；
query或合计溢出返回EINVAL。Graph数据结构与其查询共同维护，PluginHost只消费公开结果。

当前 x64 profile 下，合法 descriptor 的 session/context 各不超过1GiB，域最多1024
bindings、每owner最多1048576 reservations，结合当前Graph公开成本查询及Host实际H，
generation合计不会达到SIZE_MAX。该不可达分支以边界计算和checked算术审查验收；测试
实际执行Graph查询的SIZE_MAX溢出及catalog超上限拒绝，不篡改不可变snapshot制造非法
generation输入。32位平台、上述上限或布局改变时须重新评估；所有checked加乘仍必须保留。

budget是host栈上状态，charge每次steps>=1，在执行对应步骤之前调用；检查
steps<=max_steps-used，成功才增加used。超额ENOSPC粘住，之后不能继续步骤；DLL即便
吞错返回OK，host仍拒绝结果。max_steps为插件明确声明的原生抽象步骤，不是毫秒；
每个native执行步骤必须有观察点；无此能力的引擎preflight ENOTSUP。input/budget/error
输出指针不得保留到execute返回之后。无调用后资源兜底、无任意宿主函数查找。

execute必需返回一个独立结果，不能用NULL代表filter。result_out先NULL；失败若返回
非NULL独立值，host调用已验证destroy_result后abort；alias不销毁；成功NULL为EPROTO。
result数据只依赖result_context和immutable模块metadata，禁止引用session/输入。
clone_result必填，跨worker安全、返回独立值；destroy_result无失败且跨worker安全。
release_session和release_result_context控制线程调用，成功销毁ctx，失败ctx完全保留供重试。
host执行error保留SALTS原始码、engine_status、phase、operation/plugin identity；不得让
不一致error.status覆盖callback失败，message强制终止符。Graph适配层用现有last_error
传播状态；结构化详细错误复制到binding的受锁最近错误槽，只读查询不推进状态。
每binding mutex仅保护固定大小错误结构复制，锁内不调用DLL/分配/I/O。并发错误以最后
完成写入者为最近错误，不能当作所有失败的审计记录。无失败时返回初始化的status=OK。
generation新增只读查询（类型在plugin_generation.h已有定义）：

```c
TURBO_FLOW_C_API int turbo_flow_plugin_generation_operation_error(
  const turbo_flow_plugin_generation_t *generation, size_t binding_index,
  turbo_flow_plugin_operation_error_v3_t *out);
TURBO_FLOW_C_API int turbo_flow_plugin_generation_cleanup_error(
  const turbo_flow_plugin_generation_t *generation, turbo_flow_config_error_t *out);
```

binding_index是resolved配置原数组下标；超界EINVAL，未实例化槽返回初始化OK。
out必须完整初始化；只读复制，绝不重试清理。cleanup_error保存最近一次退役失败的
阶段/状态；控制面串行查询，与create的原始error分离。

## 结果域、generation 与失败事务

声明分别位于plugin_operation.h和plugin_generation.h。capacity单位为binding结果owner数，
默认由调用方给定、范围1..1024；一次域只服务一次已转移Graph的generation，之后不得
重新attach。preflight失败未attach，可原参数重试；消耗后的域必须destroy后新建，避免
重试叠加隐藏容量。无需全局orphan registry或跨worker snapshot refcount。

```c
TURBO_FLOW_C_API int turbo_flow_plugin_result_domain_create(
  turbo_flow_plugin_catalog_snapshot_t *snapshot, size_t capacity,
  turbo_flow_plugin_result_domain_t **domain_out, turbo_flow_plugin_error_t *error);
TURBO_FLOW_C_API int turbo_flow_plugin_result_domain_destroy(
  turbo_flow_plugin_result_domain_t *domain, turbo_flow_plugin_error_t *error);
typedef struct turbo_flow_plugin_result_domain_snapshot_v3_s {
  size_t size; uint32_t abi_major; uint32_t abi_minor;
  uint32_t state;
  size_t capacity, owner_count, outstanding, retained_bytes;
  int last_cleanup_status;
} turbo_flow_plugin_result_domain_snapshot_v3_t;
enum {
  TURBO_FLOW_PLUGIN_RESULT_DOMAIN_READY = 0u,
  TURBO_FLOW_PLUGIN_RESULT_DOMAIN_ATTACHED = 1u,
  TURBO_FLOW_PLUGIN_RESULT_DOMAIN_DETACHED = 2u,
  TURBO_FLOW_PLUGIN_RESULT_DOMAIN_RETIRING = 3u
};
TURBO_FLOW_C_API int turbo_flow_plugin_result_domain_snapshot(
  const turbo_flow_plugin_result_domain_t *domain,
  turbo_flow_plugin_result_domain_snapshot_v3_t *out);
TURBO_FLOW_C_API int turbo_flow_plugin_generation_create(
  turbo_flow_plugin_catalog_snapshot_t *snapshot,
  const turbo_flow_resolved_config_t *resolved, turbo_flow_t **flow_io,
  const turbo_flow_plugin_generation_config_t *config,
  turbo_flow_plugin_result_domain_t *result_domain,
  turbo_flow_plugin_generation_t **generation_out,
  turbo_flow_plugin_generation_t **cleanup_out, turbo_flow_config_error_t *error);
```

domain_create在控制线程reserve完整owner数组并retain一次snapshot，失败out=NULL且不
消耗snapshot。域创建时snapshot必须与generation的snapshot同一个live对象（这里比较
对象identity是生命周期一致性，不是schema类型比较）。非空bindings必须传READY域，
空bindings允许NULL；domain只在所有preflight通过且Graph转移时attach，计数0→1。
attached destroy EBUSY且不stop任何owner。snapshot查询只聚合Graph原子观测计数，不清理。

generation_create的两个out必需且不得别名，进入后都清NULL；flow_io、snapshot、domain
必须live。所有普通参数、catalog/schema/Graph契约、额度、owner容量和provider preflight
通过前，Graph仍由caller拥有，无工厂副作用。为generation、session ledger、result domain
entry和后续清理状态预留空间，然后转移Graph/attach域。凡已创建的ctx先登记在已预留entry，
然后执行下一步；不能因为push失败丢owner。

resolved中重复operation/resource绑定须在工厂前返回EALREADY。
Graph既存或Product materialize期间新增的provider冲突，以公开register_operation_provider
返回的EALREADY为准，保留原错误并进入同一可重试退役流程；该冲突可能在工厂已调用、
Graph/domain已转移后发生。不得新增不可靠的provider查询或穿透Graph私有注册表，亦不得
把冲突降级为成功。其余声明的schema/profile/额度及provider preflight拒绝仍必须在
工厂前完成。

工厂顺序：既有resources/adapters materialize → 对每binding create_result_context →
Graph projection owner create（使用domain所持snapshot bridge）→ create_session → 注册
缓存1:1 provider → compile。context成功非NULL；任何失败返回的非NULL临时context也由
预先静态验证的release函数清理，不能依赖失败工厂返回一个临时vtable。
若result owner create失败，raw context仍在domain entry由release_result_context重试；
若session create失败，非NULL临时session仍在generation entry由release_session重试。
禁止两个工厂context别名；遇alias仅释放一次，标EPROTO，不能release同一地址两次。
result_context也不能等于factory_ctx，session不能等于factory_ctx；不释放借用factory_ctx。
session==result_context时不登记session拥有引用，既有domain entry保留唯一释放权；
此时不运行execute，退役session步骤为空，最终由domain调用一次release_result_context。

构建成功仅generation_out非NULL，cleanup_out=NULL。转移后失败保留原构建错误；进入
FAILED_CLEANUP状态（generation enum追加显式值7，现有0..6不变），调用同一退役函数。
若立即清理成功，两out均NULL；若busy或release失败，仅cleanup_out非NULL，供既有
generation_destroy重试；这是仅供退役的有效句柄，generation_flow返回NULL，start/poll
返回EBUSY，不是半初始化可运行generation。domain仍明确由caller拥有。
所有调用点必须消费cleanup_out，不能只按rc丢句柄；error.path指出构建失败阶段，另由
域/退役查询记录cleanup失败，原始错误不被清理错误覆盖。
cleanup_out只涵盖generation的session/Product/Graph退役；未成功包装或未释放的结果
context始终归预先返回的domain。generation退役成功不隐式destroy域；即便create返回
错误且cleanup_out=NULL，caller仍必须destroy原domain并处理其busy/release失败。

正常/失败退役共用顺序：关闭invocation admission → 等待inflight归零和已发起worker API
返回（现有run/generation lease由控制面持有到join）→ quiesce既有Product owners → stop
Graph → drain Product owners → 逆序release_session（失败停在当前游标）→ shutdown
Product owners → destroy Graph → destroy Product owners → detach域 → 释放generation
snapshot/free generation。构建失败Graph从未start，也必须先撤销callback可达性再清session。
成功步骤不重复调用；任一失败generation/ctx/snapshot仍可诊断重试。禁止调用旧void rollback。

Product materialize 返回失败时，仍由 provider 清理部分状态并留下空 owner_out。
成功输出先严格校验 owner size/ABI，再读取尾字段：未知 size/version 返回 EINVAL，
不能调用任何 owner callback；精确 ABI3 但生命周期语义无效返回 EPROTO。后者若具有
ctx 和 destroy，仅登记不可执行的 destroy-only 终态条目，Graph 销毁后才 destroy 一次。
缺少 ctx 或 destroy、或未知布局均没有可验证的清理契约：保留 FAILED_CLEANUP、Graph、
域 ATTACHED 和 snapshot pin，cleanup 查询及每次 destroy 均返回 EINVAL，不推进任何
callback。此 native DLL 契约违规终态无法通过现有 API 修复，可能保留资源直到进程退出；
不可声称重试必然成功，不提供强制卸载或静默释放。故意触发该终态的测试以独立 CTest
进程隔离；正常及可安全清理的失败路径仍须验证恰好一次释放和最终模块卸载。

generation detach后，domain result owners保持accepting，故输出clone可以越过generation。
caller随后destroy domain：转RETIRING，stop全部result owner，等待外部caller保证不再有
新API进入且worker已join；outstanding非零返回EBUSY。计数0不代替join。逆序逐个destroy
owner（或release尚未包装的raw ctx），成功清entry，失败保留entry和snapshot。最后释放
domain snapshot/free。domain归caller从未转移给generation，不允许caller提前free。
generation失败留下的owner也在同域中退役，不回收供新的generation复用。

| 对象/状态 | owner、线程 | 上限/结束条件 |
|---|---|---|
| catalog/snapshot | PluginHost控制线程 | 65536 operations，snapshot普通引用只在控制面修改 |
| result-domain READY→ATTACHED→DETACHED→RETIRING | caller控制线程 | 1..1024 entries；attached拒绝destroy |
| session active→releasing→released | generation控制线程创建销毁；thread_safe worker执行 | 每binding一个；失败release保留 |
| invocation accepted→returned | 当前worker，input借用 | atomic max_inflight；没有异步或cancel中间态 |
| result claim→committed/aborted | exclusive message worker | reserve先于DLL；每claim一个terminal |
| result/clone | message；domain owns其Graph owner | C和R，最后destroy后归还；不依赖session |
| failed generation | cleanup_out caller控制线程 | 不可run；join+退役完成后释放 |
| descriptor/payload/settlement | 原message和Source/Sink既有owner | result提交不修改所有权或事实 |

## 实施与验证映射

实施计划：[runtime plan](../superpowers/plans/2026-09-09-typed-operation-abi3-runtime.md)。
ABI和全插件消费者→Task1；schema和catalog来源→Task2；原子结果及clone→Task3；
domain独立寿命及retry→Task4；generation/preflight/steps→Task5；安装与双profile→Task6。
Task6 的安装测试从测试专用暂存安装目录加载唯一导出
`turbo_flow_plugin_get_api` 的真实fixture，验证输入7得到结果14、generation退役后clone仍
有效、清空结果并销毁domain后模块才可卸载；C和C++消费者都从安装头及导入target编译链接。
HIGH兼容风险：generation_create新增参数必须同提交迁移全部调用方；共享插件ABI拒绝旧
版本要求同步重编译。新增Graph APIs为加法，原projection可继续使用，但没有可信metadata
的值不能进入typed operation。MED限制：一个结果槽、固定无指针schema子集、inline-only、
无权限服务；其余请求拒绝，#93/#73不因本profile通过而关闭。
设计不修改业务配置格式、不迁移持久化数据、不添加执行器、不安装引擎、不删除旧Core入口。
回滚部署整个已验证旧二进制/配置集合；新Host不自动加载旧ABI或legacy引擎。
已知边界：未知Product owner ABI或缺少可验证ctx/destroy的成功publication没有安全的
回收协议，其失败cleanup句柄会永久pin Graph、domain与模块直到进程退出；其他可验证的
release失败保留原对象并允许重试。generation admission 的每binding预算 H 保守同时计入
binding ledger与result-domain entry bridge，二者可能与domain容量预留重叠，因此不能把
预算值解释为互斥allocation或实际RSS。
