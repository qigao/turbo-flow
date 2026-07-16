#ifndef TURBO_FLOW_EMAIL_H
#define TURBO_FLOW_EMAIL_H

#include "CoroNet/turbo_coro_context.h"
#include "mime_content_disposition.h"
#include "mime_parser.h"
#include "mime_utils.h"
#include "turbo_flow.h"

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct turbo_flow_email_smtp_config_s {
  /** SMTP server host. Required for configured sink adapters. */
  const char *host;
  /** SMTP server port in 1..65535. Required for configured sink adapters. */
  int port;
  /** Use implicit TLS, e.g. SMTPS on port 465. Mutually exclusive with STARTTLS. */
  int use_tls;
  /** Use STARTTLS after plain connect, e.g. port 587. Mutually exclusive with implicit TLS. */
  int use_starttls;
  /** SMTP_AUTH_NONE, SMTP_AUTH_PLAIN, SMTP_AUTH_LOGIN, or SMTP_AUTH_CRAM_MD5 from email_smtp.h. */
  int auth_method;
  /** Optional SMTP username. */
  const char *username;
  /** Optional SMTP password. */
  const char *password;
  /** SMTP operation timeout in milliseconds; must be non-negative and 0 uses the module default. */
  int timeout_ms;
  /** Sender display name; NULL omits the display name. */
  const char *from_name;
  /** Sender address. Required for configured sink adapters. */
  const char *from_email;
  /** Recipient display name; NULL omits the display name. */
  const char *to_name;
  /** Recipient address. Required for configured sink adapters. */
  const char *to_email;
  /** Message subject; NULL uses an empty subject. */
  const char *subject;
  /** Non-zero sends flow payload as HTML body; zero sends it as plain text. */
  int html_body;
  /** Maximum context pump iterations for synchronous sends; 0 uses adapter default. */
  uint32_t max_pump_iterations;
  /**
   * CoroNet event-loop context used by the SMTP client.
   *
   * When NULL, the adapter creates and owns a context. When non-NULL and
   * `take_context_ownership` is 0, the caller owns the context. When ownership
   * is transferred, the adapter destroys the context during shutdown.
   */
  coro_context_t *context;
  int take_context_ownership;
} turbo_flow_email_smtp_config_t;

typedef struct turbo_flow_email_pop3_config_s {
  /** POP3 server host. */
  const char *host;
  /** POP3 server port in 1..65535, commonly 110 or 995. */
  int port;
  /** Use implicit TLS. Mutually exclusive with STLS. */
  int use_tls;
  /** Upgrade a plain connection with STLS. Mutually exclusive with implicit TLS. */
  int use_stls;
  /** Optional USER/PASS credentials. */
  const char *username;
  const char *password;
  /** POP3 operation timeout in milliseconds; must be non-negative and 0 uses the module default. */
  int timeout_ms;
  /** Delay between mailbox polls; must be greater than zero. */
  uint32_t poll_interval_ms;
} turbo_flow_email_pop3_config_t;

typedef struct turbo_flow_email_mime_config_s {
  mime_settings_t settings;
  /** Callback data and parser views are valid only during adapter consume. */
  void *user_data;
  /** Per-message parser pool size; zero uses 16 KiB. */
  size_t pool_size;
} turbo_flow_email_mime_config_t;

typedef struct turbo_flow_email_mime_extract_config_s {
  /** Maximum raw RFC MIME payload size; zero uses 16 MiB. */
  size_t max_payload_size;
  /** Maximum total header count across the root and all parts; zero uses 1024. */
  size_t max_headers;
  /** Maximum multipart entity count; zero uses 256. */
  size_t max_parts;
  /** Maximum combined decoded root/part body bytes; zero uses 32 MiB. */
  size_t max_decoded_bytes;
  /** Initial TurboNet parser pool size; zero uses 16 KiB. */
  size_t pool_size;
} turbo_flow_email_mime_extract_config_t;

typedef struct turbo_flow_email_attachment_config_s {
  const char *filename;
  const char *content_type;
  const char *data;
  size_t data_len;
  /** Non-zero emits an inline attachment and requires `content_id`. */
  int inline_attachment;
  const char *content_id;
} turbo_flow_email_attachment_config_t;

typedef struct turbo_flow_email_mime_encode_config_s {
  const char *from_name;
  const char *from_email;
  const char *to_name;
  const char *to_email;
  const char *subject;
  /** Non-zero maps input payload to HTML; zero maps it to plain text. */
  int html_body;
  /** Optional plain-text alternative used only when `html_body` is non-zero. */
  const char *alternative_text;
  /** 0 normal, 1 high, 2 low. */
  int priority;
  const turbo_flow_email_attachment_config_t *attachments;
  size_t attachment_count;
  /** Zero uses 16 MiB. */
  size_t max_payload_size;
  /** Zero uses 64 MiB. */
  size_t max_output_size;
  /** Initial builder pool size; zero uses 16 KiB. */
  size_t pool_size;
} turbo_flow_email_mime_encode_config_t;

typedef struct turbo_flow_email_mhtml_resource_config_s {
  const char *content_type;
  const char *content_location;
  const char *content_id;
  const char *data;
  size_t data_len;
} turbo_flow_email_mhtml_resource_config_t;

typedef struct turbo_flow_email_mhtml_encode_config_s {
  /** Root HTML charset; NULL uses UTF-8. */
  const char *charset;
  const turbo_flow_email_mhtml_resource_config_t *resources;
  size_t resource_count;
  /** Zero uses 16 MiB. */
  size_t max_payload_size;
  /** Zero uses 64 MiB. */
  size_t max_output_size;
  /** Initial builder pool size; zero uses 16 KiB. */
  size_t pool_size;
} turbo_flow_email_mhtml_encode_config_t;

typedef struct turbo_flow_email_mime_message_s turbo_flow_email_mime_message_t;
typedef struct turbo_flow_email_mime_entity_s turbo_flow_email_mime_entity_t;

/**
 * Register an SMTP sink adapter for DSL `adapter "<name>"`.
 *
 * The adapter consumes a flow message and sends its payload as one email body.
 * It uses the repository email module (`email/include/email`) for RFC 2822/MIME
 * message construction and SMTP protocol I/O. It does not poll mailboxes,
 * retry delivery, manage queues, map arbitrary headers, or own product-level
 * email workflow semantics.
 */
CXX_C_API int
turbo_flow_email_register_smtp_sink_adapter(turbo_flow_t *flow, const char *name,
                                            const turbo_flow_email_smtp_config_t *config);

/**
 * Register a POP3 polling source that publishes raw RFC messages.
 *
 * The source publishes the newest message when its UIDL changes. It never
 * deletes server messages; UIDL de-duplication is local to the adapter
 * lifetime and resets when the adapter is recreated.
 */
CXX_C_API int
turbo_flow_email_register_pop3_source_adapter(turbo_flow_t *flow, const char *name,
                                              const turbo_flow_email_pop3_config_t *config);

/** Register an RFC MIME callback transform backed by TurboNet::MimeParser. */
CXX_C_API int
turbo_flow_email_register_mime_parser_adapter(turbo_flow_t *flow, const char *name,
                                              const turbo_flow_email_mime_config_t *config);

/** Register an owned RFC MIME decode/extract transform backed by TurboNet::MimeParser. */
CXX_C_API int turbo_flow_email_register_mime_extract_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_email_mime_extract_config_t *config);

/** Register a payload-to-RFC-2822/MIME transform backed by TurboNet::Email. */
CXX_C_API int
turbo_flow_email_register_mime_encode_adapter(turbo_flow_t *flow, const char *name,
                                              const turbo_flow_email_mime_encode_config_t *config);

/** Register a payload-to-RFC-2557-MHTML transform backed by TurboNet::MimeParser. */
CXX_C_API int turbo_flow_email_register_mhtml_encode_adapter(
    turbo_flow_t *flow, const char *name, const turbo_flow_email_mhtml_encode_config_t *config);

/** Return the message-owned MIME projection, or NULL for another/opaque content representation. */
CXX_C_API const turbo_flow_email_mime_message_t *
turbo_flow_email_msg_mime(const turbo_flow_msg_t *msg);

CXX_C_API const turbo_flow_email_mime_entity_t *
turbo_flow_email_mime_root(const turbo_flow_email_mime_message_t *message);
CXX_C_API size_t turbo_flow_email_mime_part_count(const turbo_flow_email_mime_message_t *message);
CXX_C_API const turbo_flow_email_mime_entity_t *
turbo_flow_email_mime_part_at(const turbo_flow_email_mime_message_t *message, size_t index);

CXX_C_API size_t
turbo_flow_email_mime_entity_header_count(const turbo_flow_email_mime_entity_t *entity);
CXX_C_API tstr_v turbo_flow_email_mime_entity_header_name(
    const turbo_flow_email_mime_entity_t *entity, size_t index);
CXX_C_API tstr_v turbo_flow_email_mime_entity_header_value(
    const turbo_flow_email_mime_entity_t *entity, size_t index);
CXX_C_API tstr_v
turbo_flow_email_mime_entity_content_type(const turbo_flow_email_mime_entity_t *entity);
CXX_C_API tstr_v turbo_flow_email_mime_entity_charset(const turbo_flow_email_mime_entity_t *entity);
CXX_C_API mime_encoding_t
turbo_flow_email_mime_entity_encoding(const turbo_flow_email_mime_entity_t *entity);
CXX_C_API mime_disposition_type_t
turbo_flow_email_mime_entity_disposition(const turbo_flow_email_mime_entity_t *entity);
CXX_C_API tstr_v
turbo_flow_email_mime_entity_filename(const turbo_flow_email_mime_entity_t *entity);
CXX_C_API tstr_v turbo_flow_email_mime_entity_body(const turbo_flow_email_mime_entity_t *entity);

#ifdef __cplusplus
}
#endif

#endif /* TURBO_FLOW_EMAIL_H */
