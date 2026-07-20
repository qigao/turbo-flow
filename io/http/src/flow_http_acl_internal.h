#ifndef FLOW_HTTP_ACL_INTERNAL_H
#define FLOW_HTTP_ACL_INTERNAL_H

#include "turbo_flow_security.h"

CXX_C_API int flow_http_acl_decode_response(const char *body, size_t body_size, size_t max_rules,
                                            turbo_flow_security_policy_bundle_t *bundle_out);
CXX_C_API void flow_http_acl_decoded_cleanup(turbo_flow_security_policy_bundle_t *bundle);

#endif
