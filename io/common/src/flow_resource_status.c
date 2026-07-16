#include "flow_resource_status.h"

#include "fmt.h"
#include "turbo_error.h"
#include "turbo_str.h"

#include <stdio.h>
#include <string.h>

static int tf_resource_identity_set(turbo_flow_resource_metadata_t *metadata, const char *uid,
                                    const char *owner_name) {
  int written;
  written = snprintf(metadata->uid, sizeof(metadata->uid), "%s", uid);
  if (written < 0 || (size_t)written >= sizeof(metadata->uid)) return TURBO_ENAMETOOLONG;
  written = snprintf(metadata->owner_name, sizeof(metadata->owner_name), "%s", owner_name);
  if (written < 0 || (size_t)written >= sizeof(metadata->owner_name)) return TURBO_ENAMETOOLONG;
  return TURBO_OK;
}

int tf_connection_resource_metadata(const tf_connection_state_t *connection, const char *uid,
                                    const char *owner_name, turbo_flow_domain_t domain,
                                    turbo_flow_resource_metadata_t *out) {
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  int rc;
  if (!connection || !uid || !*uid || !owner_name || !*owner_name || !out ||
      out->size < sizeof(*out) || domain <= TURBO_FLOW_DOMAIN_NONE ||
      domain > TURBO_FLOW_DOMAIN_MANAGEMENT) {
    return TURBO_EINVAL;
  }
  metadata.domain = domain;
  metadata.kind = TURBO_FLOW_RESOURCE_CONNECTION;
  metadata.generation = 1u;
  metadata.observed_generation = 1u;
  rc = tf_resource_identity_set(&metadata, uid, owner_name);
  if (rc != TURBO_OK) return rc;
  memcpy(out, &metadata, sizeof(metadata));
  return TURBO_OK;
}

int tf_connection_status_document(const tf_connection_state_t *connection, const char *uid,
                                  const char *owner_name, turbo_flow_domain_t domain,
                                  const turbo_flow_resource_schema_t *schema,
                                  turbo_flow_resource_document_kind_t document_kind,
                                  turbo_flow_resource_document_t *out) {
  turbo_flow_resource_metadata_t metadata = TURBO_FLOW_RESOURCE_METADATA_INIT;
  turbo_flow_connection_snapshot_t snapshot;
  tstr_t payload;
  int rc;
  if (document_kind != TURBO_FLOW_RESOURCE_DOCUMENT_STATUS) return TURBO_ENOTSUP;
  rc = tf_connection_resource_metadata(connection, uid, owner_name, domain, &metadata);
  if (rc != TURBO_OK) return rc;
  memset(&snapshot, 0, sizeof(snapshot));
  rc = tf_connection_snapshot(connection, &snapshot);
  if (rc != TURBO_OK) return rc;
  payload = tstr_format(
      "{\"state\":{},\"last_status\":{},\"connections_current\":\"{}\","
      "\"connection_limit\":\"{}\",\"in_flight_messages\":\"{}\","
      "\"in_flight_bytes\":\"{}\"}",
      (unsigned)snapshot.state, snapshot.last_status, snapshot.connections_current,
      snapshot.connection_limit, snapshot.in_flight_messages, snapshot.in_flight_bytes);
  if (!payload) return TURBO_ENOMEM;
  rc = turbo_flow_resource_document_set_payload_copy(out, &metadata, schema, payload,
                                                     tstr_len(payload));
  tstr_free(payload);
  return rc;
}
