#include "turbo_flow_email.h"

#include "turbo_buffer.h"
#include "turbo_flow_stl_error_internal.h"

#include <stdlib.h>
#include <string.h>

#define FLOW_EMAIL_MIME_DEFAULT_POOL_SIZE 16384u
#define FLOW_EMAIL_MIME_DEFAULT_MAX_PAYLOAD (16u * 1024u * 1024u)
#define FLOW_EMAIL_MIME_DEFAULT_MAX_HEADERS 1024u
#define FLOW_EMAIL_MIME_DEFAULT_MAX_PARTS 256u
#define FLOW_EMAIL_MIME_DEFAULT_MAX_DECODED (32u * 1024u * 1024u)
#define FLOW_EMAIL_MIME_SCHEMA_ID 0x454d494du

static const char FLOW_EMAIL_MIME_PROJECTION_TYPE[] = "TurboFlow.EmailMimeMessage";
static const turbo_flow_data_schema_t FLOW_EMAIL_MIME_DATA_SCHEMA = {
    sizeof(turbo_flow_data_schema_t), TURBO_FLOW_DOMAIN_PROTOCOL_PATTERN,
    TURBO_FLOW_DATA_ENCODING_OPAQUE, "TurboFlowEmailMime", "MimeMessage",
    FLOW_EMAIL_MIME_PROJECTION_TYPE, FLOW_EMAIL_MIME_SCHEMA_ID, 1u, NULL};

typedef struct flow_email_mime_s {
  mime_settings_t settings;
  void *user_data;
  size_t pool_size;
} flow_email_mime_t;

typedef struct flow_email_mime_header_s {
  tstr name;
  tstr value;
} flow_email_mime_header_t;

struct turbo_flow_email_mime_entity_s {
  vec_t headers;
  tstr body;
  tstr content_type;
  tstr charset;
  tstr filename;
  mime_encoding_t encoding;
  mime_disposition_type_t disposition;
};

struct turbo_flow_email_mime_message_s {
  turbo_flow_email_mime_entity_t root;
  vec_t parts;
};

typedef struct flow_email_mime_extract_s {
  size_t max_payload_size;
  size_t max_headers;
  size_t max_parts;
  size_t max_decoded_bytes;
  size_t pool_size;
} flow_email_mime_extract_t;

typedef struct flow_email_mime_build_s {
  turbo_flow_email_mime_message_t *message;
  tstr pending_header;
  size_t current_part;
  size_t header_count;
  size_t max_headers;
  size_t max_parts;
  int status;
} flow_email_mime_build_t;

static const turbo_flow_option_field_t FLOW_EMAIL_MIME_FIELDS[] = {
    {"callbacks", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0},
    {"pool_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0}};
static const turbo_flow_adapter_schema_t FLOW_EMAIL_MIME_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_EMAIL,
    TURBO_FLOW_ADAPTER_TRANSFORM,
    TURBO_FLOW_ADAPTER_INPUT,
    FLOW_EMAIL_MIME_FIELDS,
    sizeof(FLOW_EMAIL_MIME_FIELDS) / sizeof(FLOW_EMAIL_MIME_FIELDS[0])};

static const turbo_flow_option_field_t FLOW_EMAIL_MIME_EXTRACT_FIELDS[] = {
    {"max_payload_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"max_headers", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"max_parts", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"max_decoded_bytes", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"pool_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0}};
static const turbo_flow_adapter_schema_t FLOW_EMAIL_MIME_EXTRACT_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_EMAIL,
    TURBO_FLOW_ADAPTER_TRANSFORM,
    TURBO_FLOW_ADAPTER_INPUT,
    FLOW_EMAIL_MIME_EXTRACT_FIELDS,
    sizeof(FLOW_EMAIL_MIME_EXTRACT_FIELDS) / sizeof(FLOW_EMAIL_MIME_EXTRACT_FIELDS[0])};

static vstr flow_email_mime_empty_view(void) { return vstr_from_buf("", 0); }

static void flow_email_mime_entity_cleanup(turbo_flow_email_mime_entity_t *entity) {
  size_t i;
  if (!entity) return;
  for (i = 0; i < vec_size(&entity->headers); ++i) {
    flow_email_mime_header_t *header =
        (flow_email_mime_header_t *)vec_at(&entity->headers, i);
    if (header) {
      tstr_freep(&header->name);
      tstr_freep(&header->value);
    }
  }
  vec_destroy(&entity->headers);
  tstr_freep(&entity->body);
  tstr_freep(&entity->content_type);
  tstr_freep(&entity->charset);
  tstr_freep(&entity->filename);
  memset(entity, 0, sizeof(*entity));
}

static int flow_email_mime_entity_init(turbo_flow_email_mime_entity_t *entity) {
  if (!entity) return TURBO_EINVAL;
  memset(entity, 0, sizeof(*entity));
  entity->encoding = MIME_ENCODING_7BIT;
  entity->disposition = MIME_DISPOSITION_UNKNOWN;
  return turbo_flow_stl_error(vec_init_bytes(&entity->headers, sizeof(flow_email_mime_header_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX));
}

static void flow_email_mime_message_destroy(void *ptr, void *ctx) {
  turbo_flow_email_mime_message_t *message = (turbo_flow_email_mime_message_t *)ptr;
  size_t i;
  (void)ctx;
  if (!message) return;
  flow_email_mime_entity_cleanup(&message->root);
  for (i = 0; i < vec_size(&message->parts); ++i) {
    flow_email_mime_entity_cleanup(
        (turbo_flow_email_mime_entity_t *)vec_at(&message->parts, i));
  }
  vec_destroy(&message->parts);
  free(message);
}

static turbo_flow_email_mime_message_t *flow_email_mime_message_create(void) {
  turbo_flow_email_mime_message_t *message =
      (turbo_flow_email_mime_message_t *)calloc(1, sizeof(*message));
  if (!message) return NULL;
  if (flow_email_mime_entity_init(&message->root) != TURBO_OK ||
      turbo_flow_stl_error(vec_init_bytes(&message->parts, sizeof(turbo_flow_email_mime_entity_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX)) != TURBO_OK) {
    flow_email_mime_message_destroy(message, NULL);
    return NULL;
  }
  return message;
}

static turbo_flow_email_mime_entity_t *
flow_email_mime_current_entity(flow_email_mime_build_t *build) {
  if (!build || !build->message) return NULL;
  if (build->current_part == SIZE_MAX) return &build->message->root;
  return (turbo_flow_email_mime_entity_t *)vec_at(&build->message->parts,
                                                        build->current_part);
}

static int flow_email_mime_set_string(tstr *target, const char *data, size_t len) {
  tstr value;
  if (!target || (len > 0 && !data)) return TURBO_EINVAL;
  value = tstr_new_len(data, len);
  if (!value) return TURBO_ENOMEM;
  tstr_freep(target);
  *target = value;
  return TURBO_OK;
}

static int flow_email_mime_header_is(tstr name, const char *expected) {
  size_t expected_len = strlen(expected);
  return name && tstr_len(name) == expected_len && tstr_ncasecmp(name, expected, expected_len) == 0;
}

static int flow_email_mime_apply_header(mime_parser_t *parser,
                                        turbo_flow_email_mime_entity_t *entity,
                                        const flow_email_mime_header_t *header) {
  if (flow_email_mime_header_is(header->name, "Content-Type")) {
    mime_content_type_t parsed;
    int rc =
        flow_email_mime_set_string(&entity->content_type, header->value, tstr_len(header->value));
    if (rc != TURBO_OK) return rc;
    if (mime_parse_content_type(header->value, tstr_len(header->value), &parsed) != 0) {
      return TURBO_EPROTO;
    }
    tstr_freep(&entity->charset);
    if (parsed.charset) {
      rc = flow_email_mime_set_string(&entity->charset, parsed.charset, parsed.charset_len);
      if (rc != TURBO_OK) return rc;
    }
  } else if (flow_email_mime_header_is(header->name, "Content-Transfer-Encoding")) {
    entity->encoding = mime_parse_encoding(header->value, tstr_len(header->value));
    if (entity->encoding == MIME_ENCODING_UNKNOWN) return TURBO_EPROTO;
  } else if (flow_email_mime_header_is(header->name, "Content-Disposition")) {
    mime_content_disposition_t parsed;
    if (mime_parse_content_disposition(header->value, tstr_len(header->value), &parsed) != 0) {
      return TURBO_EPROTO;
    }
    entity->disposition = parsed.type;
    tstr_freep(&entity->filename);
    if (parsed.filename) {
      char *filename = mime_disposition_get_filename(parser->pool, &parsed);
      if (!filename) return TURBO_ENOMEM;
      return flow_email_mime_set_string(&entity->filename, filename, strlen(filename));
    }
  }
  return TURBO_OK;
}

static int flow_email_mime_on_header_field(mime_parser_t *parser, const char *data, size_t len) {
  flow_email_mime_build_t *build = (flow_email_mime_build_t *)parser->data;
  if (!build || build->status != TURBO_OK || build->pending_header) return -1;
  build->pending_header = tstr_new_len(data, len);
  if (!build->pending_header) {
    build->status = TURBO_ENOMEM;
    return -1;
  }
  return 0;
}

static int flow_email_mime_on_header_value(mime_parser_t *parser, const char *data, size_t len) {
  flow_email_mime_build_t *build = (flow_email_mime_build_t *)parser->data;
  turbo_flow_email_mime_entity_t *entity;
  flow_email_mime_header_t header;
  int rc;
  if (!build || build->status != TURBO_OK || !build->pending_header) return -1;
  if (build->header_count >= build->max_headers) {
    build->status = TURBO_EFBIG;
    return -1;
  }
  entity = flow_email_mime_current_entity(build);
  if (!entity) {
    build->status = TURBO_EPROTO;
    return -1;
  }
  memset(&header, 0, sizeof(header));
  header.name = tstr_move(&build->pending_header);
  header.value = tstr_new_len(data, len);
  if (!header.value) {
    tstr_freep(&header.name);
    build->status = TURBO_ENOMEM;
    return -1;
  }
  rc = flow_email_mime_apply_header(parser, entity, &header);
  if (rc == TURBO_OK) rc = turbo_flow_stl_error(vec_push(&entity->headers, &header));
  if (rc != TURBO_OK) {
    tstr_freep(&header.name);
    tstr_freep(&header.value);
    build->status = rc;
    return -1;
  }
  build->header_count++;
  return 0;
}

static int flow_email_mime_on_part_begin(mime_parser_t *parser) {
  flow_email_mime_build_t *build = (flow_email_mime_build_t *)parser->data;
  turbo_flow_email_mime_entity_t entity;
  int rc;
  if (!build || build->status != TURBO_OK) return -1;
  if (vec_size(&build->message->parts) >= build->max_parts) {
    build->status = TURBO_EFBIG;
    return -1;
  }
  rc = flow_email_mime_entity_init(&entity);
  if (rc == TURBO_OK) rc = turbo_flow_stl_error(vec_push(&build->message->parts, &entity));
  if (rc != TURBO_OK) {
    flow_email_mime_entity_cleanup(&entity);
    build->status = rc;
    return -1;
  }
  build->current_part = vec_size(&build->message->parts) - 1u;
  return 0;
}

static int flow_email_mime_on_body(mime_parser_t *parser, const char *data, size_t len) {
  flow_email_mime_build_t *build = (flow_email_mime_build_t *)parser->data;
  turbo_flow_email_mime_entity_t *entity;
  tstr body;
  if (!build || build->status != TURBO_OK) return -1;
  entity = flow_email_mime_current_entity(build);
  if (!entity) {
    build->status = TURBO_EPROTO;
    return -1;
  }
  if (!entity->body) {
    entity->body = tstr_new_len(data, len);
    if (!entity->body) {
      build->status = TURBO_ENOMEM;
      return -1;
    }
    return 0;
  }
  body = tstr_cat_len(entity->body, data, len);
  if (!body) {
    build->status = TURBO_ENOMEM;
    return -1;
  }
  entity->body = body;
  return 0;
}

static int flow_email_mime_decode_entity(turbo_flow_email_mime_entity_t *entity, size_t pool_size,
                                         size_t max_decoded, size_t *decoded_total) {
  mem_pool_t pool;
  char *decoded = NULL;
  size_t decoded_len = 0;
  size_t encoded_len;
  size_t decode_pool_size;
  tstr owned;
  const char *encoded;
  if (!entity || !decoded_total) return TURBO_EINVAL;
  encoded = entity->body ? entity->body : "";
  encoded_len = entity->body ? tstr_len(entity->body) : 0u;
  decode_pool_size = encoded_len + 1u > pool_size ? encoded_len + 1u : pool_size;
  if (mem_init(&pool, decode_pool_size) != TURBO_OK) return TURBO_ENOMEM;
  if (mime_decode_body(&pool, encoded, encoded_len, entity->encoding, &decoded, &decoded_len) !=
      0) {
    mem_destroy(&pool);
    return TURBO_EPROTO;
  }
  if (decoded_len > max_decoded - *decoded_total) {
    mem_destroy(&pool);
    return TURBO_EFBIG;
  }
  owned = tstr_new_len(decoded, decoded_len);
  mem_destroy(&pool);
  if (!owned) return TURBO_ENOMEM;
  tstr_freep(&entity->body);
  entity->body = owned;
  *decoded_total += decoded_len;
  return TURBO_OK;
}

static int flow_email_mime_decode_message(turbo_flow_email_mime_message_t *message,
                                          const flow_email_mime_extract_t *adapter) {
  size_t decoded_total = 0;
  size_t i;
  int rc = flow_email_mime_decode_entity(&message->root, adapter->pool_size,
                                         adapter->max_decoded_bytes, &decoded_total);
  if (rc != TURBO_OK) return rc;
  for (i = 0; i < vec_size(&message->parts); ++i) {
    rc = flow_email_mime_decode_entity(
        (turbo_flow_email_mime_entity_t *)vec_at(&message->parts, i), adapter->pool_size,
        adapter->max_decoded_bytes, &decoded_total);
    if (rc != TURBO_OK) return rc;
  }
  return TURBO_OK;
}

static int flow_email_mime_extract_consume(void *ctx, turbo_flow_t *flow,
                                           const turbo_flow_stage_plan_t *stage,
                                           turbo_flow_msg_t *msg) {
  flow_email_mime_extract_t *adapter = (flow_email_mime_extract_t *)ctx;
  turbo_flow_email_mime_message_t *message;
  flow_email_mime_build_t build;
  mime_settings_t settings;
  mime_parser_t parser;
  mem_pool_t pool;
  mime_errno_t error;
  int rc;
  (void)flow;
  (void)stage;
  if (!adapter || !msg || (msg->payload.len > 0 && !msg->payload.data)) return TURBO_EINVAL;
  if (msg->payload.len > adapter->max_payload_size) return TURBO_EFBIG;
  message = flow_email_mime_message_create();
  if (!message) return TURBO_ENOMEM;
  memset(&build, 0, sizeof(build));
  build.message = message;
  build.current_part = SIZE_MAX;
  build.max_headers = adapter->max_headers;
  build.max_parts = adapter->max_parts;
  build.status = TURBO_OK;
  memset(&settings, 0, sizeof(settings));
  settings.on_header_field = flow_email_mime_on_header_field;
  settings.on_header_value = flow_email_mime_on_header_value;
  settings.on_body = flow_email_mime_on_body;
  settings.on_part_begin = flow_email_mime_on_part_begin;
  if (mem_init(&pool, adapter->pool_size) != TURBO_OK) {
    flow_email_mime_message_destroy(message, NULL);
    return TURBO_ENOMEM;
  }
  mime_parser_init(&parser, &settings, &pool);
  parser.data = &build;
  error = mime_parse(&parser, msg->payload.data ? msg->payload.data : "", msg->payload.len);
  tstr_freep(&build.pending_header);
  mem_destroy(&pool);
  if (error != MIME_OK || build.status != TURBO_OK || parser.state != MIME_STATE_COMPLETE) {
    rc = build.status != TURBO_OK     ? build.status
         : error == MIME_ERROR_MEMORY ? TURBO_ENOMEM
                                      : TURBO_EPROTO;
    flow_email_mime_message_destroy(message, NULL);
    return rc;
  }
  rc = flow_email_mime_decode_message(message, adapter);
  if (rc != TURBO_OK) {
    flow_email_mime_message_destroy(message, NULL);
    return rc;
  }
  turbo_flow_msg_clear_content(msg);
  rc = turbo_flow_msg_bind_projection(msg, &FLOW_EMAIL_MIME_DATA_SCHEMA, message, NULL,
                                      flow_email_mime_message_destroy, NULL);
  if (rc != TURBO_OK) flow_email_mime_message_destroy(message, NULL);
  return rc;
}

static int flow_email_mime_consume(void *ctx, turbo_flow_t *flow,
                                   const turbo_flow_stage_plan_t *stage, turbo_flow_msg_t *msg) {
  flow_email_mime_t *adapter = (flow_email_mime_t *)ctx;
  mem_pool_t pool;
  mime_parser_t parser;
  mime_errno_t error;
  (void)flow;
  (void)stage;
  if (!adapter || !msg || (msg->payload.len > 0 && !msg->payload.data)) return TURBO_EINVAL;
  if (mem_init(&pool, adapter->pool_size) != TURBO_OK) return TURBO_ENOMEM;
  mime_parser_init(&parser, &adapter->settings, &pool);
  parser.data = adapter->user_data;
  error = mime_parse(&parser, msg->payload.data ? msg->payload.data : "", msg->payload.len);
  mem_destroy(&pool);
  return error == MIME_OK ? TURBO_OK : error == MIME_ERROR_MEMORY ? TURBO_ENOMEM : TURBO_EPROTO;
}

static void flow_email_mime_shutdown(void *ctx) { free(ctx); }

int turbo_flow_email_register_mime_parser_adapter(turbo_flow_t *flow, const char *name,
                                                  const turbo_flow_email_mime_config_t *config) {
  flow_email_mime_t *adapter;
  turbo_flow_adapter_ops_t ops;
  int rc;
  if (!flow || !name || !*name || !config) return TURBO_EINVAL;
  adapter = (flow_email_mime_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  adapter->settings = config->settings;
  adapter->user_data = config->user_data;
  adapter->pool_size = config->pool_size ? config->pool_size : FLOW_EMAIL_MIME_DEFAULT_POOL_SIZE;
  memset(&ops, 0, sizeof(ops));
  ops.consume = flow_email_mime_consume;
  ops.shutdown = flow_email_mime_shutdown;
  rc = turbo_flow_register_adapter_ex(flow, name, &ops, adapter, &FLOW_EMAIL_MIME_SCHEMA);
  if (rc != TURBO_OK) flow_email_mime_shutdown(adapter);
  return rc;
}

int turbo_flow_email_register_mime_extract_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_email_mime_extract_config_t *config) {
  flow_email_mime_extract_t *adapter;
  turbo_flow_adapter_ops_t ops;
  int rc;
  if (!flow || !name || !*name) return TURBO_EINVAL;
  adapter = (flow_email_mime_extract_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  adapter->max_payload_size = config && config->max_payload_size
                                  ? config->max_payload_size
                                  : FLOW_EMAIL_MIME_DEFAULT_MAX_PAYLOAD;
  adapter->max_headers =
      config && config->max_headers ? config->max_headers : FLOW_EMAIL_MIME_DEFAULT_MAX_HEADERS;
  adapter->max_parts =
      config && config->max_parts ? config->max_parts : FLOW_EMAIL_MIME_DEFAULT_MAX_PARTS;
  adapter->max_decoded_bytes = config && config->max_decoded_bytes
                                   ? config->max_decoded_bytes
                                   : FLOW_EMAIL_MIME_DEFAULT_MAX_DECODED;
  adapter->pool_size =
      config && config->pool_size ? config->pool_size : FLOW_EMAIL_MIME_DEFAULT_POOL_SIZE;
  memset(&ops, 0, sizeof(ops));
  ops.consume = flow_email_mime_extract_consume;
  ops.shutdown = flow_email_mime_shutdown;
  rc = turbo_flow_register_adapter_ex(flow, name, &ops, adapter, &FLOW_EMAIL_MIME_EXTRACT_SCHEMA);
  if (rc != TURBO_OK) flow_email_mime_shutdown(adapter);
  return rc;
}

const turbo_flow_email_mime_message_t *turbo_flow_email_msg_mime(const turbo_flow_msg_t *msg) {
  const turbo_flow_data_schema_t *schema = NULL;
  const void *projection = turbo_flow_msg_projection(msg, &schema);
  if (!projection || !schema || !schema->projection_type ||
      strcmp(schema->projection_type, FLOW_EMAIL_MIME_PROJECTION_TYPE) != 0) {
    return NULL;
  }
  return (const turbo_flow_email_mime_message_t *)projection;
}

const turbo_flow_email_mime_entity_t *
turbo_flow_email_mime_root(const turbo_flow_email_mime_message_t *message) {
  return message ? &message->root : NULL;
}

size_t turbo_flow_email_mime_part_count(const turbo_flow_email_mime_message_t *message) {
  return message ? vec_size(&message->parts) : 0u;
}

const turbo_flow_email_mime_entity_t *
turbo_flow_email_mime_part_at(const turbo_flow_email_mime_message_t *message, size_t index) {
  return message
             ? (const turbo_flow_email_mime_entity_t *)vec_at_const(&message->parts, index)
             : NULL;
}

size_t turbo_flow_email_mime_entity_header_count(const turbo_flow_email_mime_entity_t *entity) {
  return entity ? vec_size(&entity->headers) : 0u;
}

vstr turbo_flow_email_mime_entity_header_name(const turbo_flow_email_mime_entity_t *entity,
                                                size_t index) {
  const flow_email_mime_header_t *header =
      entity ? (const flow_email_mime_header_t *)vec_at_const(&entity->headers, index) : NULL;
  return header && header->name ? tstr_to_v(header->name) : flow_email_mime_empty_view();
}

vstr turbo_flow_email_mime_entity_header_value(const turbo_flow_email_mime_entity_t *entity,
                                                 size_t index) {
  const flow_email_mime_header_t *header =
      entity ? (const flow_email_mime_header_t *)vec_at_const(&entity->headers, index) : NULL;
  return header && header->value ? tstr_to_v(header->value) : flow_email_mime_empty_view();
}

vstr turbo_flow_email_mime_entity_content_type(const turbo_flow_email_mime_entity_t *entity) {
  return entity && entity->content_type ? tstr_to_v(entity->content_type)
                                        : flow_email_mime_empty_view();
}

vstr turbo_flow_email_mime_entity_charset(const turbo_flow_email_mime_entity_t *entity) {
  return entity && entity->charset ? tstr_to_v(entity->charset) : flow_email_mime_empty_view();
}

mime_encoding_t
turbo_flow_email_mime_entity_encoding(const turbo_flow_email_mime_entity_t *entity) {
  return entity ? entity->encoding : MIME_ENCODING_UNKNOWN;
}

mime_disposition_type_t
turbo_flow_email_mime_entity_disposition(const turbo_flow_email_mime_entity_t *entity) {
  return entity ? entity->disposition : MIME_DISPOSITION_UNKNOWN;
}

vstr turbo_flow_email_mime_entity_filename(const turbo_flow_email_mime_entity_t *entity) {
  return entity && entity->filename ? tstr_to_v(entity->filename) : flow_email_mime_empty_view();
}

vstr turbo_flow_email_mime_entity_body(const turbo_flow_email_mime_entity_t *entity) {
  return entity && entity->body ? tstr_to_v(entity->body) : flow_email_mime_empty_view();
}
