#include "flowie_control_validation_internal.h"

#include "turbo_error.h"

#include <string.h>

int flowie_control_text_valid(const char *value, size_t limit) {
  size_t length;
  if (!value || limit == 0u) return 0;
  length = strnlen(value, limit + 1u);
  if (length == 0u || length > limit) return 0;
  for (size_t index = 0u; index < length; ++index) {
    const unsigned char byte = (unsigned char)value[index];
    if (byte < 0x20u || byte == 0x7fu) return 0;
  }
  return 1;
}

int flowie_control_acl_document_syntax_validate(const char *domain_id, const char *document_text,
                                                size_t document_size,
                                                flowie_control_acl_document_t *document_out) {
  flowie_control_acl_document_t document = FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
  char canonical[FLOWIE_CONTROL_ACL_DOCUMENT_MAX + 1u];
  size_t canonical_size = 0u;
  size_t domain_size;
  int rc;
  if (!flowie_control_text_valid(domain_id, TURBO_FLOW_SECURITY_ID_MAX) || !document_text ||
      document_size == 0u || document_size > FLOWIE_CONTROL_ACL_DOCUMENT_MAX ||
      memchr(document_text, '\0', document_size))
    return TURBO_EINVAL;
  rc = flowie_control_acl_parse(document_text, document_size, &document);
  if (rc != TURBO_OK) return rc;
  domain_size = strlen(domain_id);
  for (size_t index = 0u; index < document.entry_count; ++index) {
    const char *topic = document.entries[index].topic;
    if (strncmp(topic, domain_id, domain_size) != 0 || topic[domain_size] != '/')
      return TURBO_EPROTO;
  }
  rc = flowie_control_acl_format(&document, canonical, sizeof(canonical), &canonical_size);
  if (rc != TURBO_OK || canonical_size != document_size ||
      memcmp(canonical, document_text, document_size) != 0)
    return TURBO_EPROTO;
  if (document_out) *document_out = document;
  return TURBO_OK;
}
