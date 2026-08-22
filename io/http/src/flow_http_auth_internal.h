#ifndef FLOW_HTTP_AUTH_INTERNAL_H
#define FLOW_HTTP_AUTH_INTERNAL_H

#include "turbo_flow_security.h"

TURBO_FLOW_C_API int flow_http_auth_decode_response(const char *body, size_t body_size, const char *method,
                                             turbo_flow_security_principal_t *principal_out);

#endif
