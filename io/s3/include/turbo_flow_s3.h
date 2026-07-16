#ifndef TURBO_FLOW_S3_H
#define TURBO_FLOW_S3_H

#include "turbo_flow.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef enum turbo_flow_s3_credentials_e {
  TURBO_FLOW_S3_CREDENTIALS_STATIC = 0,
  TURBO_FLOW_S3_CREDENTIALS_AWS_ENV,
  TURBO_FLOW_S3_CREDENTIALS_MINIO_ENV
} turbo_flow_s3_credentials_t;

typedef struct turbo_flow_s3_config_s {
  const char *host;
  uint16_t port;
  int use_https;
  const char *region;
  int virtual_style;
  const char *bucket;
  const char *object;
  /** PutObject media type. GetObject sources use Content-Type from StatObject instead. */
  const char *content_type;
  turbo_flow_s3_credentials_t credentials;
  const char *access_key;
  const char *secret_key;
  const char *session_token;
  /** Non-zero registers a periodic GET source; zero registers a PUT sink. */
  uint32_t poll_interval_ms;
  uint32_t max_pump_iterations;
  /**
   * Optional host registry and trusted object schema selector.
   * The registry must outlive a periodic source because each StatObject result is resolved at
   * runtime. Sink mode resolves the configured PutObject media type during registration.
   */
  const turbo_flow_content_binding_t *content_binding;
} turbo_flow_s3_config_t;

/**
 * Register a fixed-object S3 client.
 *
 * Sink mode validates input content before uploading it with PutObject. Periodic source mode
 * obtains actual object Content-Type with StatObject, then downloads and publishes the bytes.
 */
CXX_C_API int turbo_flow_s3_register_client_adapter(turbo_flow_t *flow, const char *name,
                                                    const turbo_flow_s3_config_t *config);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_S3_H */
