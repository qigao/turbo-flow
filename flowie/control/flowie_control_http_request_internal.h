#ifndef FLOWIE_CONTROL_HTTP_REQUEST_INTERNAL_H
#define FLOWIE_CONTROL_HTTP_REQUEST_INTERNAL_H

#include "iris/router.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Resolve exactly one HTTP header using RFC case-insensitive field-name matching. */
int flowie_control_http_header_exact(const Req *request, const char *name,
                                     const char **value_out);

/** Resolve zero or one HTTP header; duplicate field names are rejected. */
int flowie_control_http_header_optional_exact(const Req *request, const char *name,
                                              const char **value_out);

/** Resolve exactly one non-empty cookie into caller-owned bounded storage. */
int flowie_control_http_cookie_exact(const Req *request, const char *name, char *value_out,
                                     size_t value_capacity);

#ifdef __cplusplus
}
#endif

#endif
