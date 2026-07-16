#include "turbo_flow_email.h"

#include "turbo_error.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

int main(void) {
  static const char *dsl = "source input\n"
                           "stage smtp_out adapter smtp4dev\n"
                           "stage main {\n"
                           "  input -> smtp_out\n"
                           "}\n";
  const char *host = getenv("TURBO_FLOW_SMTP4DEV_HOST");
  const char *port_text = getenv("TURBO_FLOW_SMTP4DEV_PORT");
  turbo_flow_email_smtp_config_t config;
  turbo_flow_msg_t msg;
  turbo_flow_t *flow;
  long port = 25;
  int rc;

  if (!host || host[0] == '\0') host = "127.0.0.1";
  if (port_text && port_text[0] != '\0') {
    char *end = NULL;
    port = strtol(port_text, &end, 10);
    if (!end || *end != '\0' || port < 1 || port > 65535) return 2;
  }

  memset(&config, 0, sizeof(config));
  config.host = host;
  config.port = (int)port;
  config.from_name = "TurboFlow Integration";
  config.from_email = "turboflow@example.test";
  config.to_name = "smtp4dev Inbox";
  config.to_email = "inbox@example.test";
  config.subject = "TurboFlow CoroNet primitive integration";
  config.timeout_ms = 2000;

  flow = turbo_flow_create();
  if (!flow) return 3;
  rc = turbo_flow_email_register_smtp_sink_adapter(flow, "smtp4dev", &config);
  if (rc == TURBO_OK) rc = turbo_flow_parse_string(flow, dsl, strlen(dsl));
  if (rc == TURBO_OK) rc = turbo_flow_compile(flow);
  if (rc == TURBO_OK) rc = turbo_flow_start(flow);
  turbo_flow_msg_init(&msg);
  if (rc == TURBO_OK) {
    msg.owned_payload = tstr_dup("SMTP delivery driven by TurboFlow DSL and CoroNet primitives.");
    if (!msg.owned_payload) {
      rc = TURBO_ENOMEM;
    } else {
      msg.payload = tstr_to_v(msg.owned_payload);
      rc = turbo_flow_publish(flow, "input", &msg);
    }
  }
  turbo_flow_msg_cleanup(&msg);
  if (turbo_flow_state(flow) == TURBO_FLOW_STATE_STARTED) (void)turbo_flow_stop(flow);
  turbo_flow_destroy(flow);
  if (rc != TURBO_OK) {
    fprintf(stderr, "smtp4dev integration failed: %d\n", rc);
    return 1;
  }
  printf("sent via smtp://%s:%ld\n", host, port);
  return 0;
}
