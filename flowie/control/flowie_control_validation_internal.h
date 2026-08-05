#ifndef FLOWIE_CONTROL_VALIDATION_INTERNAL_H
#define FLOWIE_CONTROL_VALIDATION_INTERNAL_H

#include "turbo_flow_security.h"
#include "flowie_control_acl_internal.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Accept one non-empty, bounded text field without ASCII control bytes. */
int flowie_control_text_valid(const char *value, size_t limit);

/** Parse one canonical user ACL document and require every topic to remain in domain_id. */
int flowie_control_acl_document_syntax_validate(const char *domain_id, const char *document_text,
                                                size_t document_size,
                                                flowie_control_acl_document_t *document_out);

#ifdef __cplusplus
}
#endif

#endif
