# 最终集中修复报告

状态：MED 启动失败后 generation 无法销毁已修复；限定验证通过，待 root 限定复审。

## 修复与状态事实

- 事实：原 client、server、WebSocket 启动错误一律进入 FAILED。provider owner_idle 排除 FAILED，quiesce 返回 ESHUTDOWN，Graph 尚未记录失败 stage 为 active，故无法通过 generation teardown 到达 detach。
- 最小修复：三类原 adapter 在启动错误且 native impl 为空时使用既有 STOPPED；last_status 保留启动错误。server/WS 从 native impl 是否存在推导 http_initialized，不再从启动返回值推导所有权。
- server/WS 原来忽略的启动 cleanup stop/destroy 返回值现在明确检查。端口读取失败后的 stop 使用已配置 stop_timeout_ms，cleanup 失败返回其具体错误、保留 native storage 与 FAILED。
- 状态事实源仍是原 adapter/native owner，插件没有第二套 admission 或生命周期状态；provider owner_idle 无需变化，未放宽任意 FAILED。
- stop-failed 仍保持 native callback storage；既有显式 stop 重试、Flow claim 终结和迟到 callback exact-once 逻辑未修改。计数为零不作为 FAILED 可释放的证据。
- client 仍只有 REGISTERED 可 start；原 stop fault 测试包含 STOPPED 后 start 返回 EALREADY。server/WS 保持原有 STOPPED 启动入口。

## RED / GREEN

所有命令的工作目录为本 worktree，统一 Windows 前缀：

```powershell
cmd.exe /d /c 'call "C:\Program Files\Microsoft Visual Studio\2022\Professional\Common7\Tools\VsDevCmd.bat" -no_logo -arch=amd64 -host_arch=amd64 && <command>'
```

RED 命令：

```text
cmake --build --preset win-dev-user --target test_chttp_plugin
ctest --preset win-dev-user -R test_chttp_plugin$ --output-on-failure
```

事实：生产修改前，原有 19 项通过，新 3 项失败；client、server、websocket 均输出
`start failure destroy: -4047 $.adapters.<kind>.owner.quiesce Product owner callback failed`。
三项均断言 generation destroy 应返回 SALTS_OK，实际为 SALTS_ESHUTDOWN。
server/WS 使用仍存活的真实 native listener 占用端口，不使用“选空闲端口后关闭”的竞争窗口。
client 使用独立 delivery fixture 5 编译真实 client adapter，仅替换 native init 为确定 ENOMEM；
经过真实 plugin/provider/materialize/start 路径，生产 DLL 不含注入代码。fixture 还核验 owner/root allocator 配对。

格式化后 GREEN 命令：

```text
cmake --build --preset win-dev-user --target test_chttp_plugin test_chttp_adapter test_chttp_server_adapter test_chttp_websocket_adapter test_chttp_client_stop_fault
ctest --preset win-dev-user -R "test_chttp_(plugin|adapter|server_adapter|websocket_adapter|client_stop_fault)$" --output-on-failure
git diff --check
```

事实：构建 exit 0，CTest 5/5，7.02 秒，diff check 无错误。
LastTest.log：plugin 22/22（758 assertions）；client adapter 10/10；
server adapter 18/18；WebSocket adapter 11/11；client stop fault 11/11。
保留并通过先前真实 owner quiesce 失败保留/重试、client stop timeout 显式重试及迟到 callback exact-once 测试。

## 接口影响与剩余风险

- 无新增公开 API、结构字段、枚举值、配置格式或依赖；Graph core 和插件公开操作未修改。
- snapshot 的已清理启动失败由 FAILED 改为 STOPPED，last_status 仍报告错误；README 已解释该资源状态语义。
- 本轮未新增启动 cleanup 自身失败的确定注入；失败保留依据 native impl 和明确 cleanup 返回值，既有正常 stop-failed 回归继续覆盖存储保留。
- 完整 Debug/Release、重复运行和 install gate 按任务边界由 root 统一执行；本报告不宣称这些门禁已在本轮重跑。
- CHTTP/memory 技能约束了所有权单一事实源与 teardown 边界；TDD 和 verification 技能要求先观察三条真实 RED，再以 fresh focused 输出交付。
