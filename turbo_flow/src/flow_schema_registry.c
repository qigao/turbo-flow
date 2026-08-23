#include "turbo_flow.h"

#include "turbo_thread.h"
#include "turbo_flow_stl_error_internal.h"

#include <stdlib.h>
#include <string.h>

static int flow_content_ascii_iequal_n(const char *left, const char *right, size_t len) {
  for (size_t i = 0; i < len; ++i) {
    unsigned char a = (unsigned char)left[i];
    unsigned char b = (unsigned char)right[i];
    if (a >= 'A' && a <= 'Z') a = (unsigned char)(a + ('a' - 'A'));
    if (b >= 'A' && b <= 'Z') b = (unsigned char)(b + ('a' - 'A'));
    if (a != b) return 0;
  }
  return 1;
}

static int flow_content_media_equal(const char *value, size_t len, const char *expected) {
  size_t expected_len = strlen(expected);
  return len == expected_len && flow_content_ascii_iequal_n(value, expected, len);
}

int turbo_flow_content_media_type_normalize(const char *media_type,
                                            turbo_flow_data_encoding_t *encoding_out,
                                            const char **normalized_media_type_out) {
  const char *begin;
  const char *end;
  size_t len;
  if (encoding_out) *encoding_out = TURBO_FLOW_DATA_ENCODING_OPAQUE;
  if (normalized_media_type_out) *normalized_media_type_out = NULL;
  if (!media_type || !encoding_out || !normalized_media_type_out) return TURBO_EINVAL;
  begin = media_type;
  while (*begin == ' ' || *begin == '\t')
    ++begin;
  end = begin;
  while (*end && *end != ';')
    ++end;
  while (end > begin && (end[-1] == ' ' || end[-1] == '\t'))
    --end;
  len = (size_t)(end - begin);
  if (flow_content_media_equal(begin, len, "application/json")) {
    *encoding_out = TURBO_FLOW_DATA_ENCODING_JSON;
    *normalized_media_type_out = "application/json";
  } else if (flow_content_media_equal(begin, len, "text/csv")) {
    *encoding_out = TURBO_FLOW_DATA_ENCODING_CSV;
    *normalized_media_type_out = "text/csv";
  } else if (flow_content_media_equal(begin, len, "application/xml") ||
             flow_content_media_equal(begin, len, "text/xml")) {
    *encoding_out = TURBO_FLOW_DATA_ENCODING_XML;
    *normalized_media_type_out = "application/xml";
  } else if (flow_content_media_equal(begin, len, "text/plain")) {
    *encoding_out = TURBO_FLOW_DATA_ENCODING_UTF8;
    *normalized_media_type_out = "text/plain";
  } else if (flow_content_media_equal(begin, len, "application/octet-stream")) {
    *encoding_out = TURBO_FLOW_DATA_ENCODING_OPAQUE;
    *normalized_media_type_out = "application/octet-stream";
  } else if (flow_content_media_equal(begin, len, "application/vnd.tbe")) {
    *encoding_out = TURBO_FLOW_DATA_ENCODING_TBE;
    *normalized_media_type_out = "application/vnd.tbe";
  } else if (flow_content_media_equal(begin, len, "application/vnd.turboflow.pg-rowset+json")) {
    *encoding_out = TURBO_FLOW_DATA_ENCODING_JSON;
    *normalized_media_type_out = "application/vnd.turboflow.pg-rowset+json";
  } else if (flow_content_media_equal(begin, len, "application/vnd.turboflow.pg-command+json")) {
    *encoding_out = TURBO_FLOW_DATA_ENCODING_JSON;
    *normalized_media_type_out = "application/vnd.turboflow.pg-command+json";
  } else {
    return TURBO_ENOENT;
  }
  return TURBO_OK;
}

static int flow_content_copy(char *dst, size_t capacity, const char *src) {
  size_t len;
  if (!dst || capacity == 0u) return TURBO_EINVAL;
  if (!src) {
    dst[0] = '\0';
    return TURBO_OK;
  }
  len = strlen(src);
  if (len >= capacity) return TURBO_ENOSPC;
  memcpy(dst, src, len + 1u);
  return TURBO_OK;
}

int turbo_flow_content_descriptor_init(turbo_flow_content_descriptor_t *descriptor,
                                       turbo_flow_domain_t domain,
                                       turbo_flow_content_profile_t profile,
                                       turbo_flow_data_encoding_t encoding, const char *media_type,
                                       const char *identity) {
  int rc;
  if (!descriptor || domain == TURBO_FLOW_DOMAIN_NONE ||
      profile < TURBO_FLOW_CONTENT_PROFILE_GENERIC ||
      profile > TURBO_FLOW_CONTENT_PROFILE_MQTT_CONTROL ||
      encoding < TURBO_FLOW_DATA_ENCODING_TBE || encoding > TURBO_FLOW_DATA_ENCODING_OPAQUE) {
    return TURBO_EINVAL;
  }
  memset(descriptor, 0, sizeof(*descriptor));
  descriptor->size = sizeof(*descriptor);
  descriptor->domain = domain;
  descriptor->profile = profile;
  descriptor->encoding = encoding;
  rc = flow_content_copy(descriptor->media_type, sizeof(descriptor->media_type), media_type);
  if (rc != TURBO_OK) return rc;
  rc = flow_content_copy(descriptor->identity, sizeof(descriptor->identity), identity);
  if (rc != TURBO_OK) return rc;
  return turbo_flow_content_descriptor_check(descriptor);
}

int turbo_flow_content_descriptor_declare_schema(turbo_flow_content_descriptor_t *descriptor,
                                                 const char *schema_name, const char *type_name,
                                                 uint32_t schema_version) {
  int rc;
  if (turbo_flow_content_descriptor_check(descriptor) != TURBO_OK || !schema_name ||
      !schema_name[0] || !type_name || !type_name[0] || schema_version == 0u) {
    return TURBO_EINVAL;
  }
  if ((descriptor->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) != 0u) {
    return descriptor->schema_version == schema_version &&
                   strcmp(descriptor->schema_name, schema_name) == 0 &&
                   strcmp(descriptor->type_name, type_name) == 0
               ? TURBO_OK
               : TURBO_EPROTO;
  }
  rc = flow_content_copy(descriptor->schema_name, sizeof(descriptor->schema_name), schema_name);
  if (rc != TURBO_OK) return rc;
  rc = flow_content_copy(descriptor->type_name, sizeof(descriptor->type_name), type_name);
  if (rc != TURBO_OK) {
    descriptor->schema_name[0] = '\0';
    return rc;
  }
  descriptor->schema_version = schema_version;
  descriptor->flags |= TURBO_FLOW_CONTENT_SCHEMA_DECLARED;
  return TURBO_OK;
}

typedef struct flow_schema_registry_entry_s {
  turbo_flow_content_descriptor_t match;
  turbo_flow_data_schema_t schema;
  tstr schema_name;
  tstr type_name;
  tstr projection_type;
} flow_schema_registry_entry_t;

struct turbo_flow_schema_registry_s {
  vec_t entries;
  turbo_mutex_t lock;
  int lock_initialized;
};

static void flow_schema_registry_entry_destroy(flow_schema_registry_entry_t *entry) {
  if (!entry) return;
  tstr_freep(&entry->schema_name);
  tstr_freep(&entry->type_name);
  tstr_freep(&entry->projection_type);
  free(entry);
}

static int flow_content_profile_domain_valid(turbo_flow_domain_t domain,
                                             turbo_flow_content_profile_t profile) {
  switch (profile) {
  case TURBO_FLOW_CONTENT_PROFILE_GENERIC:
    return domain != TURBO_FLOW_DOMAIN_NONE;
  case TURBO_FLOW_CONTENT_PROFILE_HTTP_REQUEST_BODY:
  case TURBO_FLOW_CONTENT_PROFILE_HTTP_RESPONSE_BODY:
    return domain == TURBO_FLOW_DOMAIN_IO_TRANSPORT;
  case TURBO_FLOW_CONTENT_PROFILE_S3_OBJECT:
  case TURBO_FLOW_CONTENT_PROFILE_DATABASE_PARAMETERS:
  case TURBO_FLOW_CONTENT_PROFILE_DATABASE_ROWSET:
  case TURBO_FLOW_CONTENT_PROFILE_DATABASE_COMMAND_RESULT:
    return domain == TURBO_FLOW_DOMAIN_BUFFER_PERSISTENCE;
  case TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_DATA:
  case TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_CONTROL:
  case TURBO_FLOW_CONTENT_PROFILE_MQTT_APPLICATION:
  case TURBO_FLOW_CONTENT_PROFILE_MQTT_CONTROL:
    return domain == TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN;
  default:
    return 0;
  }
}

int turbo_flow_content_descriptor_check(const turbo_flow_content_descriptor_t *descriptor) {
  const uint32_t known_flags = TURBO_FLOW_CONTENT_SCHEMA_DECLARED | TURBO_FLOW_CONTENT_BATCH |
                               TURBO_FLOW_CONTENT_PROTOCOL_CONTROL;
  turbo_flow_data_encoding_t normalized_encoding = TURBO_FLOW_DATA_ENCODING_OPAQUE;
  const char *normalized_media_type = NULL;
  int media_rc;
  if (!descriptor || descriptor->size < sizeof(*descriptor) ||
      descriptor->domain == TURBO_FLOW_DOMAIN_NONE ||
      descriptor->profile < TURBO_FLOW_CONTENT_PROFILE_GENERIC ||
      descriptor->profile > TURBO_FLOW_CONTENT_PROFILE_MQTT_CONTROL ||
      descriptor->encoding < TURBO_FLOW_DATA_ENCODING_TBE ||
      descriptor->encoding > TURBO_FLOW_DATA_ENCODING_OPAQUE) {
    return TURBO_EINVAL;
  }
  if (!flow_content_profile_domain_valid(descriptor->domain, descriptor->profile) ||
      (descriptor->flags & ~known_flags) != 0u) {
    return TURBO_EINVAL;
  }
  if (descriptor->media_type[TURBO_FLOW_CONTENT_MEDIA_TYPE_MAX] != '\0' ||
      descriptor->schema_name[TURBO_FLOW_CONTENT_SCHEMA_NAME_MAX] != '\0' ||
      descriptor->type_name[TURBO_FLOW_CONTENT_TYPE_NAME_MAX] != '\0' ||
      descriptor->identity[TURBO_FLOW_CONTENT_IDENTITY_MAX] != '\0') {
    return TURBO_EINVAL;
  }
  if (descriptor->profile != TURBO_FLOW_CONTENT_PROFILE_GENERIC &&
      descriptor->media_type[0] == '\0') {
    return TURBO_EINVAL;
  }
  if ((descriptor->flags & TURBO_FLOW_CONTENT_PROTOCOL_CONTROL) != 0u &&
      descriptor->profile != TURBO_FLOW_CONTENT_PROFILE_PROTOCOL_CONTROL &&
      descriptor->profile != TURBO_FLOW_CONTENT_PROFILE_MQTT_CONTROL) {
    return TURBO_EINVAL;
  }
  if (descriptor->media_type[0] != '\0') {
    media_rc = turbo_flow_content_media_type_normalize(descriptor->media_type, &normalized_encoding,
                                                       &normalized_media_type);
    if (media_rc == TURBO_OK && (descriptor->encoding != normalized_encoding ||
                                 strcmp(descriptor->media_type, normalized_media_type) != 0)) {
      return TURBO_EINVAL;
    }
    if (media_rc != TURBO_OK && media_rc != TURBO_ENOENT) return media_rc;
  }
  if ((descriptor->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) != 0u) {
    return descriptor->schema_name[0] != '\0' && descriptor->type_name[0] != '\0' &&
                   descriptor->schema_version != 0u
               ? TURBO_OK
               : TURBO_EINVAL;
  }
  return descriptor->schema_name[0] == '\0' && descriptor->type_name[0] == '\0' &&
                 descriptor->schema_version == 0u
             ? TURBO_OK
             : TURBO_EINVAL;
}

static int flow_schema_registry_key_equal(const flow_schema_registry_entry_t *entry,
                                          const turbo_flow_content_descriptor_t *match,
                                          const turbo_flow_data_schema_t *schema) {
  return entry->match.domain == match->domain && entry->match.profile == match->profile &&
         strcmp(entry->match.media_type, match->media_type) == 0 &&
         strcmp(entry->schema.schema_name, schema->schema_name) == 0 &&
         strcmp(entry->schema.type_name, schema->type_name) == 0 &&
         entry->schema.schema_version == schema->schema_version;
}

static int flow_schema_registry_definition_equal(const flow_schema_registry_entry_t *entry,
                                                 const turbo_flow_data_schema_t *schema) {
  return entry->schema.domain == schema->domain && entry->schema.encoding == schema->encoding &&
         entry->schema.schema_id == schema->schema_id &&
         strcmp(entry->schema.projection_type, schema->projection_type) == 0;
}

static int flow_schema_registry_matches(const flow_schema_registry_entry_t *entry,
                                        const turbo_flow_content_descriptor_t *descriptor) {
  if (entry->match.domain != descriptor->domain || entry->match.profile != descriptor->profile ||
      strcmp(entry->match.media_type, descriptor->media_type) != 0) {
    return 0;
  }
  if ((descriptor->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) == 0u) return 0;
  return descriptor->schema_version == entry->schema.schema_version &&
         strcmp(descriptor->schema_name, entry->schema.schema_name) == 0 &&
         strcmp(descriptor->type_name, entry->schema.type_name) == 0;
}

turbo_flow_schema_registry_t *turbo_flow_schema_registry_create(void) {
  turbo_flow_schema_registry_t *registry =
      (turbo_flow_schema_registry_t *)calloc(1, sizeof(*registry));
  if (!registry) return NULL;
  if (turbo_flow_stl_error(vec_init_bytes(&registry->entries, sizeof(flow_schema_registry_entry_t *), _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != TURBO_OK) {
    free(registry);
    return NULL;
  }
  turbo_mutex_init(&registry->lock);
  registry->lock_initialized = 1;
  return registry;
}

void turbo_flow_schema_registry_destroy(turbo_flow_schema_registry_t *registry) {
  if (!registry) return;
  if (registry->lock_initialized) turbo_mutex_lock(&registry->lock);
  for (size_t i = 0; i < vec_size(&registry->entries); ++i) {
    flow_schema_registry_entry_t **entry =
        (flow_schema_registry_entry_t **)vec_at(&registry->entries, i);
    if (entry) flow_schema_registry_entry_destroy(*entry);
  }
  vec_destroy(&registry->entries);
  if (registry->lock_initialized) {
    turbo_mutex_unlock(&registry->lock);
    turbo_mutex_destroy(&registry->lock);
  }
  free(registry);
}

int turbo_flow_schema_registry_register(turbo_flow_schema_registry_t *registry,
                                        const turbo_flow_content_descriptor_t *match,
                                        const turbo_flow_data_schema_t *schema) {
  flow_schema_registry_entry_t *entry;
  int rc;
  if (!registry || turbo_flow_content_descriptor_check(match) != TURBO_OK || !schema ||
      schema->size < sizeof(*schema) || schema->domain != match->domain || !schema->schema_name ||
      !schema->schema_name[0] || !schema->type_name || !schema->type_name[0] ||
      !schema->projection_type || !schema->projection_type[0] || schema->schema_version == 0u ||
      schema->encoding != match->encoding || schema->schema_text) {
    return TURBO_EINVAL;
  }
  if ((match->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) != 0u &&
      (match->schema_version != schema->schema_version ||
       strcmp(match->schema_name, schema->schema_name) != 0 ||
       strcmp(match->type_name, schema->type_name) != 0)) {
    return TURBO_EPROTO;
  }
  turbo_mutex_lock(&registry->lock);
  for (size_t i = 0; i < vec_size(&registry->entries); ++i) {
    flow_schema_registry_entry_t *const *current =
        (flow_schema_registry_entry_t *const *)vec_at_const(&registry->entries, i);
    if (current && flow_schema_registry_key_equal(*current, match, schema)) {
      rc = flow_schema_registry_definition_equal(*current, schema) ? TURBO_EALREADY : TURBO_EPROTO;
      turbo_mutex_unlock(&registry->lock);
      return rc;
    }
  }
  entry = (flow_schema_registry_entry_t *)calloc(1, sizeof(*entry));
  if (!entry) {
    turbo_mutex_unlock(&registry->lock);
    return TURBO_ENOMEM;
  }
  entry->match = *match;
  entry->match.size = sizeof(entry->match);
  entry->schema_name = tstr_dup(schema->schema_name);
  entry->type_name = tstr_dup(schema->type_name);
  entry->projection_type = tstr_dup(schema->projection_type);
  if (!entry->schema_name || !entry->type_name || !entry->projection_type) {
    flow_schema_registry_entry_destroy(entry);
    turbo_mutex_unlock(&registry->lock);
    return TURBO_ENOMEM;
  }
  entry->schema = *schema;
  entry->schema.size = sizeof(entry->schema);
  entry->schema.schema_name = entry->schema_name;
  entry->schema.type_name = entry->type_name;
  entry->schema.projection_type = entry->projection_type;
  entry->schema.schema_text = NULL;
  rc = turbo_flow_stl_error(vec_push(&registry->entries, &entry));
  if (rc != TURBO_OK) flow_schema_registry_entry_destroy(entry);
  turbo_mutex_unlock(&registry->lock);
  return rc;
}

int turbo_flow_schema_registry_resolve(const turbo_flow_schema_registry_t *registry,
                                       const turbo_flow_content_descriptor_t *descriptor,
                                       const turbo_flow_data_schema_t **schema_out) {
  turbo_flow_schema_registry_t *mutable_registry = (turbo_flow_schema_registry_t *)registry;
  if (schema_out) *schema_out = NULL;
  if (!registry || !schema_out || turbo_flow_content_descriptor_check(descriptor) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  const turbo_flow_data_schema_t *resolved = NULL;
  turbo_mutex_lock(&mutable_registry->lock);
  for (size_t i = 0; i < vec_size(&registry->entries); ++i) {
    flow_schema_registry_entry_t *const *entry =
        (flow_schema_registry_entry_t *const *)vec_at_const(&registry->entries, i);
    if (entry && flow_schema_registry_matches(*entry, descriptor)) {
      resolved = &(*entry)->schema;
    }
  }
  if (resolved) {
    *schema_out = resolved;
    turbo_mutex_unlock(&mutable_registry->lock);
    return TURBO_OK;
  }
  turbo_mutex_unlock(&mutable_registry->lock);
  return TURBO_ENOENT;
}

int turbo_flow_content_descriptor_resolve(turbo_flow_content_descriptor_t *descriptor,
                                          const turbo_flow_schema_registry_t *registry,
                                          const turbo_flow_content_schema_ref_t *selector) {
  const turbo_flow_data_schema_t *schema = NULL;
  int explicit_selector = selector != NULL;
  int rc;
  if (turbo_flow_content_descriptor_check(descriptor) != TURBO_OK) return TURBO_EINVAL;
  if (selector) {
    if (selector->size < sizeof(*selector) || !selector->schema_name || !selector->schema_name[0] ||
        !selector->type_name || !selector->type_name[0] || selector->schema_version == 0u ||
        !registry) {
      return TURBO_EINVAL;
    }
    rc = turbo_flow_content_descriptor_declare_schema(
        descriptor, selector->schema_name, selector->type_name, selector->schema_version);
    if (rc != TURBO_OK) return rc;
  }
  if (!registry) return TURBO_OK;
  rc = turbo_flow_schema_registry_resolve(registry, descriptor, &schema);
  if (rc == TURBO_ENOENT) return explicit_selector ? TURBO_EPROTO : TURBO_OK;
  if (rc != TURBO_OK) return rc;
  if (!schema) return TURBO_EPROTO;
  if ((descriptor->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) != 0u) {
    return descriptor->schema_version == schema->schema_version &&
                   strcmp(descriptor->schema_name, schema->schema_name) == 0 &&
                   strcmp(descriptor->type_name, schema->type_name) == 0
               ? TURBO_OK
               : TURBO_EPROTO;
  }
  return turbo_flow_content_descriptor_declare_schema(descriptor, schema->schema_name,
                                                      schema->type_name, schema->schema_version);
}

int turbo_flow_content_descriptor_from_media(turbo_flow_content_descriptor_t *descriptor,
                                             turbo_flow_domain_t domain,
                                             turbo_flow_content_profile_t profile,
                                             const char *media_type, const char *identity,
                                             const turbo_flow_content_binding_t *binding) {
  const turbo_flow_content_schema_ref_t *selector = NULL;
  turbo_flow_data_encoding_t encoding;
  const char *normalized_media_type;
  int schema_fields = 0;
  int rc;
  if (!descriptor || !media_type) return TURBO_EINVAL;
  if (binding) {
    if (binding->size < sizeof(*binding) || binding->schema.size < sizeof(binding->schema)) {
      return TURBO_EINVAL;
    }
    schema_fields = (binding->schema.schema_name && binding->schema.schema_name[0]) +
                    (binding->schema.type_name && binding->schema.type_name[0]) +
                    (binding->schema.schema_version != 0u);
    if (schema_fields != 0 && schema_fields != 3) return TURBO_EINVAL;
    if (schema_fields == 3) selector = &binding->schema;
  }
  rc = turbo_flow_content_media_type_normalize(media_type, &encoding, &normalized_media_type);
  if (rc != TURBO_OK) return rc;
  rc = turbo_flow_content_descriptor_init(descriptor, domain, profile, encoding,
                                          normalized_media_type, identity);
  if (rc != TURBO_OK) return rc;
  return turbo_flow_content_descriptor_resolve(descriptor, binding ? binding->registry : NULL,
                                               selector);
}

int turbo_flow_content_descriptor_validate(const turbo_flow_content_descriptor_t *expected,
                                           const turbo_flow_content_descriptor_t *actual) {
  const int expected_declared = (expected->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) != 0u;
  const int actual_declared = (actual->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) != 0u;
  if (turbo_flow_content_descriptor_check(expected) != TURBO_OK ||
      turbo_flow_content_descriptor_check(actual) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  if (expected->domain != actual->domain || expected->profile != actual->profile ||
      expected->encoding != actual->encoding ||
      strcmp(expected->media_type, actual->media_type) != 0) {
    return TURBO_EPROTO;
  }
  if (expected_declared != actual_declared ||
      (expected_declared && (expected->schema_version != actual->schema_version ||
                             strcmp(expected->schema_name, actual->schema_name) != 0 ||
                             strcmp(expected->type_name, actual->type_name) != 0))) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

int turbo_flow_content_descriptor_validate_payload(const turbo_flow_content_descriptor_t *expected,
                                                   const turbo_flow_content_descriptor_t *actual) {
  if (turbo_flow_content_descriptor_check(expected) != TURBO_OK ||
      turbo_flow_content_descriptor_check(actual) != TURBO_OK) {
    return TURBO_EINVAL;
  }
  if (expected->encoding != actual->encoding ||
      strcmp(expected->media_type, actual->media_type) != 0) {
    return TURBO_EPROTO;
  }
  if ((expected->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) != 0u &&
      ((actual->flags & TURBO_FLOW_CONTENT_SCHEMA_DECLARED) == 0u ||
       expected->schema_version != actual->schema_version ||
       strcmp(expected->schema_name, actual->schema_name) != 0 ||
       strcmp(expected->type_name, actual->type_name) != 0)) {
    return TURBO_EPROTO;
  }
  return TURBO_OK;
}

int turbo_flow_msg_resolve_schema(const turbo_flow_msg_t *msg,
                                  const turbo_flow_schema_registry_t *registry,
                                  const turbo_flow_data_schema_t **schema_out) {
  const turbo_flow_content_descriptor_t *descriptor = turbo_flow_msg_content_descriptor(msg);
  if (!descriptor) {
    if (schema_out) *schema_out = NULL;
    return TURBO_ENOENT;
  }
  return turbo_flow_schema_registry_resolve(registry, descriptor, schema_out);
}
