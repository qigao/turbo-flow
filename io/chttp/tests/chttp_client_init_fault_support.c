/* Isolated DLL substitution at the native allocation boundary; no production fault hooks. */
#include <chttp/chttp.h>
static int delivery_client_init(chttp_async_client *client, const chttp_client_config *config) {
  if (!client || client->impl || !config) return SALTS_EINVAL;
  return SALTS_ENOMEM;
}
#define chttp_async_client_init delivery_client_init
#include "../src/turbo_flow_chttp.c"
