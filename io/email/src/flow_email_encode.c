#include "turbo_flow_email.h"

#include "email/email_message.h"
#include "mime_mhtml.h"
#include "turbo_vec.h"

#include <ctype.h>
#include <stdlib.h>
#include <string.h>

#define FLOW_EMAIL_ENCODE_DEFAULT_MAX_PAYLOAD (16u * 1024u * 1024u)
#define FLOW_EMAIL_ENCODE_DEFAULT_MAX_OUTPUT (64u * 1024u * 1024u)
#define FLOW_EMAIL_ENCODE_DEFAULT_POOL_SIZE 16384u
#define FLOW_EMAIL_ENCODE_MAX_ITEMS 1024u

typedef struct flow_email_attachment_s {
  tstr_t filename;
  tstr_t content_type;
  tstr_t data;
  int inline_attachment;
  tstr_t content_id;
} flow_email_attachment_t;

typedef struct flow_email_mime_encode_s {
  tstr_t from_name;
  tstr_t from_email;
  tstr_t to_name;
  tstr_t to_email;
  tstr_t subject;
  tstr_t alternative_text;
  int html_body;
  int priority;
  turbo_vec_t attachments;
  size_t max_payload_size;
  size_t max_output_size;
  size_t pool_size;
} flow_email_mime_encode_t;

typedef struct flow_email_mhtml_resource_s {
  tstr_t content_type;
  tstr_t content_location;
  tstr_t content_id;
  tstr_t data;
} flow_email_mhtml_resource_t;

typedef struct flow_email_mhtml_encode_s {
  tstr_t charset;
  turbo_vec_t resources;
  size_t max_payload_size;
  size_t max_output_size;
  size_t pool_size;
} flow_email_mhtml_encode_t;

static const turbo_flow_option_field_t FLOW_EMAIL_MIME_ENCODE_FIELDS[] = {
    {"from_name", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"from_email", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"to_name", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"to_email", TURBO_FLOW_OPTION_STRING, TURBO_FLOW_OPTION_REQUIRED, 0, 0, NULL, 0},
    {"subject", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"html_body", TURBO_FLOW_OPTION_BOOL, 0, 0, 0, NULL, 0},
    {"alternative_text", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"priority", TURBO_FLOW_OPTION_U32, TURBO_FLOW_OPTION_HAS_MAX, 0, 2, NULL, 0},
    {"attachments", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL,
     0},
    {"attachment_count", TURBO_FLOW_OPTION_SIZE, TURBO_FLOW_OPTION_HAS_MAX, 0,
     FLOW_EMAIL_ENCODE_MAX_ITEMS, NULL, 0},
    {"max_payload_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"max_output_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"pool_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0}};

static const turbo_flow_adapter_schema_t FLOW_EMAIL_MIME_ENCODE_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_EMAIL,
    TURBO_FLOW_ADAPTER_TRANSFORM,
    TURBO_FLOW_ADAPTER_INPUT,
    FLOW_EMAIL_MIME_ENCODE_FIELDS,
    sizeof(FLOW_EMAIL_MIME_ENCODE_FIELDS) / sizeof(FLOW_EMAIL_MIME_ENCODE_FIELDS[0])};

static const turbo_flow_option_field_t FLOW_EMAIL_MHTML_ENCODE_FIELDS[] = {
    {"charset", TURBO_FLOW_OPTION_STRING, 0, 0, 0, NULL, 0},
    {"resources", TURBO_FLOW_OPTION_HOST_OBJECT, TURBO_FLOW_OPTION_NOT_SERIALIZABLE, 0, 0, NULL, 0},
    {"resource_count", TURBO_FLOW_OPTION_SIZE, TURBO_FLOW_OPTION_HAS_MAX, 0,
     FLOW_EMAIL_ENCODE_MAX_ITEMS, NULL, 0},
    {"max_payload_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"max_output_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0},
    {"pool_size", TURBO_FLOW_OPTION_SIZE, 0, 0, 0, NULL, 0}};

static const turbo_flow_adapter_schema_t FLOW_EMAIL_MHTML_ENCODE_SCHEMA = {
    NULL,
    TURBO_FLOW_ADAPTER_KIND_EMAIL,
    TURBO_FLOW_ADAPTER_TRANSFORM,
    TURBO_FLOW_ADAPTER_INPUT,
    FLOW_EMAIL_MHTML_ENCODE_FIELDS,
    sizeof(FLOW_EMAIL_MHTML_ENCODE_FIELDS) / sizeof(FLOW_EMAIL_MHTML_ENCODE_FIELDS[0])};

static int flow_email_encode_copy(tstr_t *dst, const char *src, size_t len) {
  if (!dst || (len > 0 && !src)) return TURBO_EINVAL;
  if (!src) return TURBO_OK;
  *dst = tstr_new_len(src, len ? len : strlen(src));
  return *dst ? TURBO_OK : TURBO_ENOMEM;
}

static int flow_email_encode_safe_header(const char *value) {
  return !value || (!strchr(value, '\r') && !strchr(value, '\n'));
}

static int flow_email_encode_safe_display(const char *value) {
  return flow_email_encode_safe_header(value) &&
         (!value || (!strchr(value, '"') && !strchr(value, '\\')));
}

static int flow_email_encode_valid_charset(const char *value) {
  const unsigned char *cursor = (const unsigned char *)value;
  if (!value || !*value) return 0;
  while (*cursor) {
    if (!isalnum(*cursor) && *cursor != '-' && *cursor != '_' && *cursor != '.') return 0;
    ++cursor;
  }
  return 1;
}

static int flow_email_encode_valid_content_type(const char *value) {
  mime_content_type_t parsed;
  return value && *value && flow_email_encode_safe_header(value) &&
         mime_parse_content_type(value, strlen(value), &parsed) == 0;
}

static int flow_email_encode_valid_address(const char *value) {
  const char *at;
  if (!value || !*value || !flow_email_encode_safe_header(value) || strchr(value, ' ') ||
      strchr(value, '<') || strchr(value, '>')) {
    return 0;
  }
  at = strchr(value, '@');
  return at && at != value && at[1] != '\0' && !strchr(at + 1, '@');
}

static int flow_email_encode_add_size(size_t *total, size_t value, size_t limit) {
  if (!total || value > limit - *total) return TURBO_EFBIG;
  *total += value;
  return TURBO_OK;
}

static void flow_email_encode_adopt_payload(turbo_flow_msg_t *msg, tstr_t payload) {
  turbo_flow_msg_clear_content(msg);
  tstr_freep(&msg->owned_payload);
  mem_buffer_release(msg->buffer);
  msg->buffer = NULL;
  msg->owned_payload = payload;
  msg->payload = tstr_to_v(payload);
}

static void flow_email_attachment_cleanup(flow_email_attachment_t *attachment) {
  if (!attachment) return;
  tstr_freep(&attachment->filename);
  tstr_freep(&attachment->content_type);
  tstr_freep(&attachment->data);
  tstr_freep(&attachment->content_id);
}

static void flow_email_mime_encode_shutdown(void *ctx) {
  flow_email_mime_encode_t *adapter = (flow_email_mime_encode_t *)ctx;
  size_t i;
  if (!adapter) return;
  for (i = 0; i < turbo_vec_size(&adapter->attachments); ++i) {
    flow_email_attachment_cleanup(
        (flow_email_attachment_t *)turbo_vec_at(&adapter->attachments, i));
  }
  turbo_vec_destroy(&adapter->attachments);
  tstr_freep(&adapter->from_name);
  tstr_freep(&adapter->from_email);
  tstr_freep(&adapter->to_name);
  tstr_freep(&adapter->to_email);
  tstr_freep(&adapter->subject);
  tstr_freep(&adapter->alternative_text);
  free(adapter);
}

static int flow_email_mime_encode_consume(void *ctx, turbo_flow_t *flow,
                                          const turbo_flow_stage_plan_t *stage,
                                          turbo_flow_msg_t *msg) {
  flow_email_mime_encode_t *adapter = (flow_email_mime_encode_t *)ctx;
  email_message_t *email;
  mem_pool_t pool;
  tstr_t body = NULL;
  tstr_t output = NULL;
  size_t i;
  int rc = TURBO_ENOMEM;
  (void)flow;
  (void)stage;
  if (!adapter || !msg || (msg->payload.len > 0 && !msg->payload.data)) return TURBO_EINVAL;
  if (msg->payload.len > adapter->max_payload_size) return TURBO_EFBIG;
  if (msg->payload.len > 0 && memchr(msg->payload.data, '\0', msg->payload.len))
    return TURBO_EINVAL;
  body = tstr_new_len(msg->payload.data ? msg->payload.data : "", msg->payload.len);
  if (!body || mem_init(&pool, adapter->pool_size) != TURBO_OK) goto cleanup;
  email = email_message_create(&pool);
  if (!email || email_message_set_from(email, adapter->from_name, adapter->from_email) != 0 ||
      email_message_add_to(email, adapter->to_name, adapter->to_email) != 0 ||
      (adapter->subject && email_message_set_subject(email, adapter->subject) != 0)) {
    goto cleanup_pool;
  }
  if (adapter->html_body) {
    if (email_message_set_html_body(email, body) != 0 ||
        (adapter->alternative_text &&
         email_message_set_text_body(email, adapter->alternative_text) != 0)) {
      goto cleanup_email;
    }
  } else if (email_message_set_text_body(email, body) != 0) {
    goto cleanup_email;
  }
  email_message_set_priority(email, (email_priority_t)adapter->priority);
  for (i = 0; i < turbo_vec_size(&adapter->attachments); ++i) {
    const flow_email_attachment_t *attachment =
        (const flow_email_attachment_t *)turbo_vec_at_const(&adapter->attachments, i);
    int add_rc =
        attachment->inline_attachment
            ? email_message_add_inline_attachment(email, attachment->content_id,
                                                  attachment->filename, attachment->content_type,
                                                  attachment->data, tstr_len(attachment->data))
            : email_message_add_attachment(email, attachment->filename, attachment->content_type,
                                           attachment->data, tstr_len(attachment->data));
    if (add_rc != 0) goto cleanup_email;
  }
  output = email_message_to_string(email);
  if (!output) goto cleanup_email;
  if (tstr_len(output) > adapter->max_output_size) {
    rc = TURBO_EFBIG;
    goto cleanup_email;
  }
  flow_email_encode_adopt_payload(msg, output);
  output = NULL;
  rc = TURBO_OK;

cleanup_email:
  if (email && email->message_id) {
    free(email->message_id);
    email->message_id = NULL;
  }
  email_message_free(email);
cleanup_pool:
  mem_destroy(&pool);
cleanup:
  tstr_freep(&output);
  tstr_freep(&body);
  return rc;
}

static int flow_email_copy_attachment(flow_email_mime_encode_t *adapter,
                                      const turbo_flow_email_attachment_config_t *src) {
  flow_email_attachment_t dst;
  int rc;
  if (!adapter || !src || !src->filename || !*src->filename ||
      !flow_email_encode_valid_content_type(src->content_type) ||
      (src->data_len > 0 && !src->data) || !flow_email_encode_safe_header(src->filename) ||
      strchr(src->filename, '"') ||
      (src->inline_attachment &&
       (!src->content_id || !*src->content_id || !flow_email_encode_safe_header(src->content_id) ||
        strchr(src->content_id, '<') || strchr(src->content_id, '>')))) {
    return TURBO_EINVAL;
  }
  memset(&dst, 0, sizeof(dst));
  dst.inline_attachment = src->inline_attachment != 0;
  rc = flow_email_encode_copy(&dst.filename, src->filename, 0);
  if (rc == TURBO_OK) rc = flow_email_encode_copy(&dst.content_type, src->content_type, 0);
  if (rc == TURBO_OK)
    rc = flow_email_encode_copy(&dst.data, src->data ? src->data : "", src->data_len);
  if (rc == TURBO_OK && src->content_id)
    rc = flow_email_encode_copy(&dst.content_id, src->content_id, 0);
  if (rc == TURBO_OK) rc = turbo_vec_push(&adapter->attachments, &dst);
  if (rc != TURBO_OK) flow_email_attachment_cleanup(&dst);
  return rc;
}

static void flow_email_mhtml_resource_cleanup(flow_email_mhtml_resource_t *resource) {
  if (!resource) return;
  tstr_freep(&resource->content_type);
  tstr_freep(&resource->content_location);
  tstr_freep(&resource->content_id);
  tstr_freep(&resource->data);
}

static void flow_email_mhtml_encode_shutdown(void *ctx) {
  flow_email_mhtml_encode_t *adapter = (flow_email_mhtml_encode_t *)ctx;
  size_t i;
  if (!adapter) return;
  for (i = 0; i < turbo_vec_size(&adapter->resources); ++i) {
    flow_email_mhtml_resource_cleanup(
        (flow_email_mhtml_resource_t *)turbo_vec_at(&adapter->resources, i));
  }
  turbo_vec_destroy(&adapter->resources);
  tstr_freep(&adapter->charset);
  free(adapter);
}

static int flow_email_mhtml_encode_consume(void *ctx, turbo_flow_t *flow,
                                           const turbo_flow_stage_plan_t *stage,
                                           turbo_flow_msg_t *msg) {
  flow_email_mhtml_encode_t *adapter = (flow_email_mhtml_encode_t *)ctx;
  mime_mhtml_document_t *document = NULL;
  mem_pool_t pool;
  char *serialized = NULL;
  tstr_t output = NULL;
  size_t output_len = 0;
  size_t i;
  int rc = TURBO_ENOMEM;
  (void)flow;
  (void)stage;
  if (!adapter || !msg || (msg->payload.len > 0 && !msg->payload.data)) return TURBO_EINVAL;
  if (msg->payload.len > adapter->max_payload_size) return TURBO_EFBIG;
  if (msg->payload.len > 0 && memchr(msg->payload.data, '\0', msg->payload.len))
    return TURBO_EINVAL;
  if (mem_init(&pool, adapter->pool_size) != TURBO_OK) return TURBO_ENOMEM;
  document = mime_mhtml_document_create(&pool);
  if (!document || mime_mhtml_set_html(document, msg->payload.data ? msg->payload.data : "",
                                       msg->payload.len, adapter->charset) != 0) {
    goto cleanup;
  }
  for (i = 0; i < turbo_vec_size(&adapter->resources); ++i) {
    const flow_email_mhtml_resource_t *resource =
        (const flow_email_mhtml_resource_t *)turbo_vec_at_const(&adapter->resources, i);
    if (mime_mhtml_add_resource(document, resource->content_type, resource->content_location,
                                resource->content_id, resource->data, tstr_len(resource->data),
                                0) != 0) {
      goto cleanup;
    }
  }
  serialized = mime_mhtml_serialize(document, &output_len);
  if (!serialized) goto cleanup;
  if (output_len > adapter->max_output_size) {
    rc = TURBO_EFBIG;
    goto cleanup;
  }
  output = tstr_new_len(serialized, output_len);
  if (!output) goto cleanup;
  flow_email_encode_adopt_payload(msg, output);
  output = NULL;
  rc = TURBO_OK;

cleanup:
  tstr_freep(&output);
  free(serialized);
  mime_mhtml_document_free(document);
  mem_destroy(&pool);
  return rc;
}

static int flow_email_copy_mhtml_resource(flow_email_mhtml_encode_t *adapter,
                                          const turbo_flow_email_mhtml_resource_config_t *src) {
  flow_email_mhtml_resource_t dst;
  int rc;
  if (!adapter || !src || !flow_email_encode_valid_content_type(src->content_type) ||
      ((!src->content_location || !*src->content_location) &&
       (!src->content_id || !*src->content_id)) ||
      (src->data_len > 0 && !src->data) || !flow_email_encode_safe_header(src->content_location) ||
      !flow_email_encode_safe_header(src->content_id) ||
      (src->content_id && (strchr(src->content_id, '<') || strchr(src->content_id, '>')))) {
    return TURBO_EINVAL;
  }
  memset(&dst, 0, sizeof(dst));
  rc = flow_email_encode_copy(&dst.content_type, src->content_type, 0);
  if (rc == TURBO_OK && src->content_location) {
    rc = flow_email_encode_copy(&dst.content_location, src->content_location, 0);
  }
  if (rc == TURBO_OK && src->content_id)
    rc = flow_email_encode_copy(&dst.content_id, src->content_id, 0);
  if (rc == TURBO_OK)
    rc = flow_email_encode_copy(&dst.data, src->data ? src->data : "", src->data_len);
  if (rc == TURBO_OK) rc = turbo_vec_push(&adapter->resources, &dst);
  if (rc != TURBO_OK) flow_email_mhtml_resource_cleanup(&dst);
  return rc;
}

int turbo_flow_email_register_mime_encode_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_email_mime_encode_config_t *config) {
  flow_email_mime_encode_t *adapter;
  turbo_flow_adapter_ops_t ops;
  size_t configured_bytes = 0;
  size_t i;
  int rc;
  if (!flow || !name || !*name || !config || !flow_email_encode_valid_address(config->from_email) ||
      !flow_email_encode_valid_address(config->to_email) ||
      !flow_email_encode_safe_display(config->from_name) ||
      !flow_email_encode_safe_display(config->to_name) ||
      !flow_email_encode_safe_header(config->subject) || config->priority < 0 ||
      config->priority > 2 || (!config->html_body && config->alternative_text) ||
      config->attachment_count > FLOW_EMAIL_ENCODE_MAX_ITEMS ||
      (config->attachment_count > 0 && !config->attachments)) {
    return TURBO_EINVAL;
  }
  adapter = (flow_email_mime_encode_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  adapter->max_payload_size =
      config->max_payload_size ? config->max_payload_size : FLOW_EMAIL_ENCODE_DEFAULT_MAX_PAYLOAD;
  adapter->max_output_size =
      config->max_output_size ? config->max_output_size : FLOW_EMAIL_ENCODE_DEFAULT_MAX_OUTPUT;
  adapter->pool_size = config->pool_size ? config->pool_size : FLOW_EMAIL_ENCODE_DEFAULT_POOL_SIZE;
  adapter->html_body = config->html_body != 0;
  adapter->priority = config->priority;
  rc = turbo_vec_init(&adapter->attachments, sizeof(flow_email_attachment_t));
  if (rc == TURBO_OK && config->from_name)
    rc = flow_email_encode_copy(&adapter->from_name, config->from_name, 0);
  if (rc == TURBO_OK) rc = flow_email_encode_copy(&adapter->from_email, config->from_email, 0);
  if (rc == TURBO_OK && config->to_name)
    rc = flow_email_encode_copy(&adapter->to_name, config->to_name, 0);
  if (rc == TURBO_OK) rc = flow_email_encode_copy(&adapter->to_email, config->to_email, 0);
  if (rc == TURBO_OK && config->subject)
    rc = flow_email_encode_copy(&adapter->subject, config->subject, 0);
  if (rc == TURBO_OK && config->alternative_text) {
    rc = flow_email_encode_copy(&adapter->alternative_text, config->alternative_text, 0);
  }
  for (i = 0; rc == TURBO_OK && i < config->attachment_count; ++i) {
    rc = flow_email_encode_add_size(&configured_bytes, config->attachments[i].data_len,
                                    adapter->max_output_size);
    if (rc == TURBO_OK) rc = flow_email_copy_attachment(adapter, &config->attachments[i]);
  }
  if (rc != TURBO_OK) {
    flow_email_mime_encode_shutdown(adapter);
    return rc;
  }
  memset(&ops, 0, sizeof(ops));
  ops.consume = flow_email_mime_encode_consume;
  ops.shutdown = flow_email_mime_encode_shutdown;
  rc = turbo_flow_register_adapter_ex(flow, name, &ops, adapter, &FLOW_EMAIL_MIME_ENCODE_SCHEMA);
  if (rc != TURBO_OK) flow_email_mime_encode_shutdown(adapter);
  return rc;
}

int turbo_flow_email_register_mhtml_encode_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_email_mhtml_encode_config_t *config) {
  flow_email_mhtml_encode_t *adapter;
  turbo_flow_adapter_ops_t ops;
  size_t configured_bytes = 0;
  size_t i;
  int rc;
  if (!flow || !name || !*name || !config ||
      (config->charset && !flow_email_encode_valid_charset(config->charset)) ||
      config->resource_count > FLOW_EMAIL_ENCODE_MAX_ITEMS ||
      (config->resource_count > 0 && !config->resources)) {
    return TURBO_EINVAL;
  }
  adapter = (flow_email_mhtml_encode_t *)calloc(1, sizeof(*adapter));
  if (!adapter) return TURBO_ENOMEM;
  adapter->max_payload_size =
      config->max_payload_size ? config->max_payload_size : FLOW_EMAIL_ENCODE_DEFAULT_MAX_PAYLOAD;
  adapter->max_output_size =
      config->max_output_size ? config->max_output_size : FLOW_EMAIL_ENCODE_DEFAULT_MAX_OUTPUT;
  adapter->pool_size = config->pool_size ? config->pool_size : FLOW_EMAIL_ENCODE_DEFAULT_POOL_SIZE;
  rc = turbo_vec_init(&adapter->resources, sizeof(flow_email_mhtml_resource_t));
  if (rc == TURBO_OK) {
    rc = flow_email_encode_copy(&adapter->charset, config->charset ? config->charset : "utf-8", 0);
  }
  for (i = 0; rc == TURBO_OK && i < config->resource_count; ++i) {
    rc = flow_email_encode_add_size(&configured_bytes, config->resources[i].data_len,
                                    adapter->max_output_size);
    if (rc == TURBO_OK) rc = flow_email_copy_mhtml_resource(adapter, &config->resources[i]);
  }
  if (rc != TURBO_OK) {
    flow_email_mhtml_encode_shutdown(adapter);
    return rc;
  }
  memset(&ops, 0, sizeof(ops));
  ops.consume = flow_email_mhtml_encode_consume;
  ops.shutdown = flow_email_mhtml_encode_shutdown;
  rc = turbo_flow_register_adapter_ex(flow, name, &ops, adapter, &FLOW_EMAIL_MHTML_ENCODE_SCHEMA);
  if (rc != TURBO_OK) flow_email_mhtml_encode_shutdown(adapter);
  return rc;
}
