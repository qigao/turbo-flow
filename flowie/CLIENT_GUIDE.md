# Flowie 客户端开发指南

Flowie Client 是回调驱动的 C MQTT client。它支持 MQTT 3.1、3.1.1、5，以及 TCP、TLS、WS、WSS。
client 自己拥有 CoroNet context、worker thread、接收循环和有界 command queue；调用方不需要驱动 event loop。

## 1. CMake 接入

安装 TurboFlow 后：

```cmake
cmake_minimum_required(VERSION 3.25)
project(flowie_client_example C)

find_package(TurboFlow CONFIG REQUIRED)

add_executable(flowie_client_example main.c)
target_link_libraries(flowie_client_example PRIVATE
    TurboFlow::FlowieClient
    TurboUtils::Core
)
```

仓库内构建目标：

```powershell
cmake --build --preset win-release-user --target flowie_client
```

公开头文件是 `flowie_mqtt_client.h`。所有配置结构必须从对应 `*_INIT` 宏初始化。

## 2. 完整最小示例

下面程序连接 MQTT 5 broker，订阅 `demo/#`，发布一条 QoS 1 消息，并在主线程等待用户退出。

```c
#include "flowie_mqtt_client.h"
#include "turbo_error.h"

#include <stdio.h>

static int on_message(flowie_mqtt_client_t *client,
                      const flowie_mqtt_publish_view_t *message,
                      void *user_data) {
  (void)client;
  (void)user_data;
  printf("message topic=%.*s payload=%.*s\n",
         (int)message->topic.size, (const char *)message->topic.data,
         (int)message->payload.size, (const char *)message->payload.data);
  return TURBO_OK;
}

static void on_complete(flowie_mqtt_client_t *client, int status,
                        const flowie_mqtt_control_packet_view_t *response,
                        void *user_data) {
  (void)client;
  (void)response;
  (void)user_data;
  if (status != TURBO_OK) fprintf(stderr, "MQTT operation failed: %d\n", status);
}

static void on_error(flowie_mqtt_client_t *client, int status, void *user_data) {
  (void)client;
  (void)user_data;
  fprintf(stderr, "MQTT background error: %d\n", status);
}

int main(void) {
  static uint8_t client_id[] = "flowie-client-1";
  static uint8_t filter[] = "demo/#";
  static uint8_t topic[] = "demo/hello";
  static uint8_t payload[] = "hello from Flowie";
  flowie_mqtt_client_topic_handler_t handler = {0};
  flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
  flowie_mqtt_connect_packet_t connect = FLOWIE_MQTT_CONNECT_PACKET_INIT;
  flowie_mqtt_subscription_t subscription = {0};
  flowie_mqtt_subscribe_packet_t subscribe = FLOWIE_MQTT_SUBSCRIBE_PACKET_INIT;
  flowie_mqtt_client_publish_topic_t publish_topic = {0};
  flowie_mqtt_client_publish_topic_vec_t publish =
      FLOWIE_MQTT_CLIENT_PUBLISH_TOPIC_VEC_INIT;
  flowie_mqtt_client_t *client = NULL;
  int rc;

  handler.filter = (flowie_mqtt_span_t){filter, sizeof(filter) - 1u};
  handler.on_message = on_message;
  config.host = "127.0.0.1";
  config.port = 1883;
  config.transport = FLOWIE_MQTT_CLIENT_TRANSPORT_TCP;
  config.topic_handlers =
      (flowie_mqtt_client_topic_handler_map_t){&handler, 1u};
  config.on_connect = on_complete;
  config.on_subscribe = on_complete;
  config.on_publish = on_complete;
  config.on_disconnect = on_complete;
  config.on_error = on_error;

  rc = flowie_mqtt_client_create(&config, &client);
  if (rc != TURBO_OK) return 1;

  connect.version = FLOWIE_MQTT_VERSION_5;
  connect.clean_start = 1u;
  connect.keep_alive = 30u;
  connect.client_id =
      (flowie_mqtt_span_t){client_id, sizeof(client_id) - 1u};
  subscription.filter = (flowie_mqtt_span_t){filter, sizeof(filter) - 1u};
  subscription.qos = 1u;
  subscribe.version = FLOWIE_MQTT_VERSION_5;
  subscribe.subscriptions = &subscription;
  subscribe.subscription_count = 1u;
  publish_topic.qos = 1u;
  publish_topic.topic = (flowie_mqtt_span_t){topic, sizeof(topic) - 1u};
  publish_topic.payload = (flowie_mqtt_span_t){payload, sizeof(payload) - 1u};
  publish.version = FLOWIE_MQTT_VERSION_5;
  publish.data = &publish_topic;
  publish.count = 1u;

  rc = flowie_mqtt_client_connect(client, &connect);
  if (rc == TURBO_OK) rc = flowie_mqtt_client_subscribe(client, &subscribe);
  if (rc == TURBO_OK) rc = flowie_mqtt_client_publish(client, &publish);
  if (rc != TURBO_OK) {
    fprintf(stderr, "command admission failed: %d\n", rc);
    flowie_mqtt_client_destroy(client);
    return 1;
  }

  puts("Press Enter to stop...");
  (void)getchar();
  (void)flowie_mqtt_client_disconnect(client, 0u, (flowie_mqtt_span_t){0});
  flowie_mqtt_client_destroy(client);
  return 0;
}
```

CONNECT、SUBSCRIBE、PUBLISH 会按有界 command queue 的顺序执行。API 返回 `TURBO_OK` 只表示命令已被
client 接管；协议或网络结果通过对应 callback 返回。

## 3. MQTT 版本

通过 packet 的 `version` 字段选择：

- `FLOWIE_MQTT_VERSION_3_1`
- `FLOWIE_MQTT_VERSION_3_1_1`
- `FLOWIE_MQTT_VERSION_5`

同一个 client connection 只能使用一个协商版本。MQTT 3.x 不支持 MQTT 5 properties、AUTH 和 reason
code；不要把 MQTT 5 packet 字段直接复用于 3.x。

## 4. TLS、WSS 与客户端 mTLS

TLS：

```c
flowie_mqtt_client_config_t config = FLOWIE_MQTT_CLIENT_CONFIG_INIT;
config.host = "broker.example.com";
config.port = 8883;
config.transport = FLOWIE_MQTT_CLIENT_TRANSPORT_TLS;
config.tls.ca_file = "broker-ca.pem";
config.tls.cert_file = "client.pem";
config.tls.key_file = "client-key.pem";
config.tls.key_password = key_provider_password;
```

WSS 使用相同 TLS 配置，并设置：

```c
config.transport = FLOWIE_MQTT_CLIENT_TRANSPORT_WSS;
config.port = 443;
config.path = "/mqtt";
```

安全边界：

- peer verification 始终开启，API 不允许关闭。
- `cert_file` 与 `key_file` 必须成对出现。
- 配置字符串在 `create` 时复制；调用方随后可以释放原字符串。
- client 销毁时会擦除内部私钥密码副本。
- 不要把私钥密码写进源码、日志或普通 YAML；从进程 secret provider 获取后临时传入。

## 5. Callback 与线程边界

所有 callback 在 client 自己的 worker thread 上执行：

- packet view、topic、payload、properties 和 response 只借用到 callback 返回。
- 需要延迟处理时必须复制数据，不能保存 view 指针。
- callback 内可以提交新命令。
- callback 内不能销毁同一个 client，也不要阻塞等待同一 client 的 callback。
- `flowie_mqtt_client_destroy()` 必须从 callback 外调用；它会停止 admission、取消 I/O、完成已接管命令并 join worker。

Topic handler 使用 MQTT filter；多个 filter 匹配时按配置顺序调用，首个非 `TURBO_OK` 返回会停止后续 handler。

## 6. 命令与错误语义

公开操作包括 CONNECT、PUBLISH、SUBSCRIBE、UNSUBSCRIBE、PING 和 DISCONNECT。

- 立即返回错误：参数无效、状态不允许、缺少对应 callback、queue 已满或正在 shutdown。
- completion callback 错误：命令已接管后发生的 encode、协议、网络、timeout 或 broker 拒绝。
- error callback：不属于某个已接管命令的后台错误，例如 broker 非预期断开。

PUBLISH vector 是原子 admission：要么全部 topic 被接管，要么一个都不接管。每个 topic 按输入顺序产生
一次 `on_publish` callback。

## 7. 容量建议

默认 command queue 为 64 条、4 MiB owned bytes，默认最大 packet 为 1 MiB。生产环境应按峰值请求和可接受
内存设置 `command_queue_capacity`、`command_queue_max_bytes`、`max_packet_size` 和
`max_inbound_qos2`，并将 queue-full 作为显式背压处理，不能无限重试。

真实 client 回归参考 [test_flowie_mqtt_client.c](client/tests/test_flowie_mqtt_client.c)。
