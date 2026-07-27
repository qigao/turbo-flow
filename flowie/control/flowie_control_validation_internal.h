#ifndef FLOWIE_CONTROL_VALIDATION_INTERNAL_H
#define FLOWIE_CONTROL_VALIDATION_INTERNAL_H

#include "turbo_flow_security.h"

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

/** Accept one non-empty, bounded text field without ASCII control bytes. */
int flowie_control_text_valid(const char *value, size_t limit);

/**
 * Parse one canonical ACL rule and validate the format-neutral protocol constraints.
 *
 * Subject existence and enabled state belong to the selected fact-store transaction and are
 * intentionally not checked here.
 */
int flowie_control_policy_rule_syntax_validate(const char *root_group_id, const char *rule_line,
                                               size_t rule_line_size,
                                               turbo_flow_security_rule_t *rule_out);

#ifdef __cplusplus
}
#endif

#endif
