#include "rules_forge.h"
#include "turbo_flow.h"
#include "turbo_flow_codec.h"
#include "turbo_flow_http_client.h"
#include "turbo_flow_rulesforge.h"

#include <stdio.h>
#include <string.h>

#define TODO_URL "https://jsonplaceholder.typicode.com/todos/1"
#define TODO_HTTP_ADAPTER "http.public.todo"
#define TODO_CODEC_ADAPTER "codec.todo"
#define TODO_RULE_PROVIDER "rules.todo"
#define TODO_MATCHED_FLAG (1u << 1)
#define TODO_HTTP_TIMEOUT_MS 5000
#define TODO_MAX_RESPONSE_SIZE (64u * 1024u)
#define TODO_MAX_PUMP_ITERATIONS 20000
#define TODO_MAX_RULES 16
#define TODO_DISRUPTOR_CAPACITY 128
#define TODO_STRINGIFY_VALUE(value) #value
#define TODO_STRINGIFY(value) TODO_STRINGIFY_VALUE(value)

static const char k_todo_flow[] =
    "source trigger\n"
    "stage fetch adapter " TODO_HTTP_ADAPTER
    " operation http.client.request resource " TODO_HTTP_ADAPTER "\n"
    "stage decode adapter " TODO_CODEC_ADAPTER "\n"
    "stage rules operation rulesforge.apply resource " TODO_RULE_PROVIDER "\n"
    "stage dispatch worker 1 capacity "
    TODO_STRINGIFY(TODO_DISRUPTOR_CAPACITY) "\n"
    "stage open_sink\n"
    "stage completed_sink\n"
    "stage main {\n"
    "  trigger -> fetch -> decode -> rules -> dispatch\n"
    "  route dispatch -> open_sink when msg.rule_matched\n"
    "  route dispatch -> completed_sink when not msg.rule_matched\n"
    "}\n";

typedef struct todo_run {
  int open_count;
  int completed_count;
} todo_run_t;

static int todo_dispatch(turbo_flow_msg_t *msg, void *ctx) {
  (void)msg;
  (void)ctx;
  return TURBO_OK;
}

static int todo_open_sink(turbo_flow_msg_t *msg, void *ctx) {
  todo_run_t *run = (todo_run_t *)ctx;

  ++run->open_count;
  printf("OPEN todo matched RulesForge: %.*s\n", (int)msg->payload.len,
         msg->payload.data);
  return TURBO_OK;
}

static int todo_completed_sink(turbo_flow_msg_t *msg, void *ctx) {
  todo_run_t *run = (todo_run_t *)ctx;

  ++run->completed_count;
  printf("COMPLETED todo bypassed open rule: %.*s\n", (int)msg->payload.len,
         msg->payload.data);
  return TURBO_OK;
}

static int todo_report_failure(const char *operation, int status) {
  fprintf(stderr, "%s failed: status=%d\n", operation, status);
  return status == TURBO_OK ? 1 : status;
}

int main(void) {
  const char *base_dirs[] = {TODO_ASSET_DIR};
  turbo_flow_http_client_config_t http_config;
  turbo_flow_codec_databind_config_t codec_config;
  turbo_flow_rulesforge_databind_provider_t rules_config =
      TURBO_FLOW_RULEFORGE_DATABIND_PROVIDER_INIT;
  ruleforge_knowledge_base_t knowledge_base = NULL;
  turbo_flow_t *flow = NULL;
  turbo_flow_msg_t trigger;
  todo_run_t run = {0};
  int ruleforge_ready = 0;
  int flow_started = 0;
  int message_initialized = 0;
  int status;

  status = ruleforge_init();
  if (status != RULES_FORGE_OK) {
    return todo_report_failure("ruleforge_init", status);
  }
  ruleforge_ready = 1;

  status = ruleforge_kb_create(&knowledge_base);
  if (status != RULES_FORGE_OK) {
    todo_report_failure("ruleforge_kb_create", status);
    goto cleanup;
  }
  status = ruleforge_kb_load_drl_file(knowledge_base, TODO_RULE_PATH, base_dirs,
                                      1);
  if (status != RULES_FORGE_OK) {
    todo_report_failure("ruleforge_kb_load_drl_file", status);
    goto cleanup;
  }

  flow = turbo_flow_create();
  if (flow == NULL) {
    status = TURBO_ENOMEM;
    todo_report_failure("turbo_flow_create", status);
    goto cleanup;
  }

  memset(&http_config, 0, sizeof(http_config));
  http_config.url = TODO_URL;
  http_config.method = TURBO_FLOW_HTTP_GET;
  http_config.timeout_ms = TODO_HTTP_TIMEOUT_MS;
  http_config.max_response_size = TODO_MAX_RESPONSE_SIZE;
  http_config.max_pump_iterations = TODO_MAX_PUMP_ITERATIONS;
  http_config.success_status_min = 200;
  http_config.success_status_max = 299;
  status = turbo_flow_http_register_client_adapter(
      flow, TODO_HTTP_ADAPTER, &http_config);
  if (status != TURBO_OK) {
    todo_report_failure("register HTTP adapter", status);
    goto cleanup;
  }

  memset(&codec_config, 0, sizeof(codec_config));
  codec_config.schema_path = TODO_ASSET_DIR "/public_todo.schema";
  codec_config.type_name = "Todo";
  codec_config.input_format = TURBO_FLOW_CODEC_DATABIND_JSON;
  codec_config.max_payload_size = TODO_MAX_RESPONSE_SIZE;
  status = turbo_flow_codec_register_databind_adapter(
      flow, TODO_CODEC_ADAPTER, &codec_config);
  if (status != TURBO_OK) {
    todo_report_failure("register DataBind codec", status);
    goto cleanup;
  }

  rules_config.knowledge_base = knowledge_base;
  rules_config.matched_flag = TODO_MATCHED_FLAG;
  rules_config.max_rules = TODO_MAX_RULES;
  status = turbo_flow_rulesforge_register_databind_provider(
      flow, TODO_RULE_PROVIDER, &rules_config);
  if (status != TURBO_OK) {
    todo_report_failure("register RulesForge provider", status);
    goto cleanup;
  }

  status = turbo_flow_register_stage_ex(flow, "dispatch", todo_dispatch, &run,
                                        NULL);
  if (status == TURBO_OK) {
    status = turbo_flow_register_stage_ex(flow, "open_sink", todo_open_sink,
                                          &run, NULL);
  }
  if (status == TURBO_OK) {
    status = turbo_flow_register_stage_ex(flow, "completed_sink",
                                          todo_completed_sink, &run, NULL);
  }
  if (status != TURBO_OK) {
    todo_report_failure("register graph stages", status);
    goto cleanup;
  }

  status = turbo_flow_parse_string(flow, k_todo_flow,
                                   sizeof(k_todo_flow) - 1u);
  if (status == TURBO_OK) {
    status = turbo_flow_compile(flow);
  }
  if (status == TURBO_OK) {
    status = turbo_flow_start(flow);
  }
  if (status != TURBO_OK) {
    todo_report_failure("start graph", status);
    goto cleanup;
  }
  flow_started = 1;

  turbo_flow_msg_init(&trigger);
  message_initialized = 1;
  status = turbo_flow_publish(flow, "trigger", &trigger);
  if (status != TURBO_OK) {
    todo_report_failure("publish trigger", status);
    goto cleanup;
  }

  status = turbo_flow_stop(flow);
  flow_started = 0;
  if (status != TURBO_OK) {
    todo_report_failure("stop graph", status);
    goto cleanup;
  }
  status = run.open_count == 1 && run.completed_count == 0 ? TURBO_OK
                                                           : TURBO_EPROTO;
  if (status != TURBO_OK) {
    todo_report_failure("validate routed result", status);
  }

cleanup:
  if (message_initialized) {
    turbo_flow_msg_cleanup(&trigger);
  }
  if (flow_started) {
    const int stop_status = turbo_flow_stop(flow);
    if (status == TURBO_OK && stop_status != TURBO_OK) {
      status = stop_status;
    }
  }
  if (flow != NULL) {
    turbo_flow_destroy(flow);
  }
  if (knowledge_base != NULL) {
    const int destroy_status = ruleforge_kb_destroy(knowledge_base);
    if (status == TURBO_OK && destroy_status != RULES_FORGE_OK) {
      status = destroy_status;
    }
  }
  if (ruleforge_ready) {
    ruleforge_cleanup();
  }
  return status == TURBO_OK ? 0 : 1;
}
