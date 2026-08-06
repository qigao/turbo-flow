#ifndef FLOW_HTTP_ACL_INTERNAL_H
#define FLOW_HTTP_ACL_INTERNAL_H

#include "turbo_flow_security.h"

CXX_C_API int flow_http_acl_encode_check_request(const turbo_flow_security_request_t *request,
                                                 char **body_out, size_t *body_size_out);
CXX_C_API int flow_http_acl_decode_check_response(
    const char *body, size_t body_size, turbo_flow_security_decision_t *decision_out);

#endif
