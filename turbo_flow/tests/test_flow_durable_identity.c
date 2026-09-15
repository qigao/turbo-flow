#include "tinytest.h"
#include "turbo_flow_durable_buffer.h"
#include "turbo_flow_projection.h"
#include "../src/flow_projection_owner_internal.h"

#include <string.h>

static void check_identity_eq(const turbo_flow_msg_t *msg, const char *source,
                              const char *admission, const char *correlation,
                              uint64_t source_sequence) {
  turbo_flow_durable_identity_t out = TURBO_FLOW_DURABLE_IDENTITY_INIT;
  size_t correlation_len = correlation ? strlen(correlation) : 0u;

  check_equal(turbo_flow_msg_durable_identity(msg, &out), SALTS_OK);
  check_equal(out.source_id.len, strlen(source));
  check_true(memcmp(out.source_id.data, source, out.source_id.len) == 0);
  check_equal(out.admission_id.len, strlen(admission));
  check_true(memcmp(out.admission_id.data, admission, out.admission_id.len) == 0);
  check_equal(out.correlation.len, correlation_len);
  if (correlation_len != 0u) {
    check_true(memcmp(out.correlation.data, correlation, correlation_len) == 0);
  }
  check_equal(out.source_sequence, source_sequence);
}

static turbo_flow_durable_identity_t make_identity(char *source, char *admission,
                                                    char *correlation,
                                                    uint64_t source_sequence) {
  turbo_flow_durable_identity_t identity = TURBO_FLOW_DURABLE_IDENTITY_INIT;
  identity.source_id = vstr_from_buf(source, strlen(source));
  identity.admission_id = vstr_from_buf(admission, strlen(admission));
  identity.correlation = correlation ? vstr_from_buf(correlation, strlen(correlation))
                                     : vstr_from_buf(NULL, 0u);
  identity.source_sequence = source_sequence;
  return identity;
}

spec("Graph durable message identity") {
  it("copies caller identity bytes into message-owned storage") {
    char source[] = "telemetry";
    char admission[] = "frame-42";
    char correlation[] = "device-7";
    turbo_flow_durable_identity_t identity =
        make_identity(source, admission, correlation, UINT64_C(42));
    turbo_flow_msg_t msg;

    turbo_flow_msg_init(&msg);
    check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_OK);
    memset(source, 'x', strlen(source));
    memset(admission, 'y', strlen(admission));
    memset(correlation, 'z', strlen(correlation));
    check_identity_eq(&msg, "telemetry", "frame-42", "device-7", UINT64_C(42));
    turbo_flow_msg_cleanup(&msg);
  }

  it("preserves identity across clone retain move and content clears") {
    char source[] = "intake";
    char admission[] = "g7:19";
    char correlation[] = "corr-19";
    turbo_flow_durable_identity_t identity =
        make_identity(source, admission, correlation, UINT64_C(19));
    turbo_flow_msg_t source_msg;
    turbo_flow_msg_t clone;
    turbo_flow_msg_t retained;
    turbo_flow_msg_t moved;

    turbo_flow_msg_init(&source_msg);
    turbo_flow_msg_init(&clone);
    turbo_flow_msg_init(&retained);
    turbo_flow_msg_init(&moved);
    check_equal(turbo_flow_msg_set_durable_identity(&source_msg, &identity), SALTS_OK);

    check_equal(turbo_flow_msg_clone(&clone, &source_msg), SALTS_OK);
    check_identity_eq(&clone, "intake", "g7:19", "corr-19", UINT64_C(19));

    check_equal(turbo_flow_msg_retain_view(&retained, &source_msg), SALTS_OK);
    check_identity_eq(&retained, "intake", "g7:19", "corr-19", UINT64_C(19));

    check_equal(turbo_flow_msg_move(&moved, &retained), SALTS_OK);
    check_identity_eq(&moved, "intake", "g7:19", "corr-19", UINT64_C(19));

    turbo_flow_msg_clear_projection(&source_msg);
    check_identity_eq(&source_msg, "intake", "g7:19", "corr-19", UINT64_C(19));
    turbo_flow_msg_clear_result(&source_msg);
    check_identity_eq(&source_msg, "intake", "g7:19", "corr-19", UINT64_C(19));
    turbo_flow_msg_clear_content(&source_msg);
    check_identity_eq(&source_msg, "intake", "g7:19", "corr-19", UINT64_C(19));

    turbo_flow_msg_cleanup(&moved);
    turbo_flow_msg_cleanup(&retained);
    turbo_flow_msg_cleanup(&clone);
    turbo_flow_msg_cleanup(&source_msg);
  }

  it("rejects empty oversized and incompatible identities") {
    char source[] = "source";
    char admission[] = "admission";
    char long_source[TURBO_FLOW_DURABLE_SOURCE_ID_MAX + 2u];
    char long_admission[TURBO_FLOW_DURABLE_ADMISSION_ID_MAX + 2u];
    char long_correlation[TURBO_FLOW_DURABLE_CORRELATION_MAX + 2u];
    turbo_flow_durable_identity_t identity =
        make_identity(source, admission, NULL, UINT64_C(1));
    turbo_flow_durable_identity_t out = TURBO_FLOW_DURABLE_IDENTITY_INIT;
    turbo_flow_msg_t msg;

    memset(long_source, 's', sizeof(long_source));
    memset(long_admission, 'a', sizeof(long_admission));
    memset(long_correlation, 'c', sizeof(long_correlation));
    turbo_flow_msg_init(&msg);

    check_equal(turbo_flow_msg_durable_identity(&msg, &out), SALTS_ENOENT);

    identity.source_id = vstr_from_buf(NULL, 0u);
    check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_EINVAL);
    identity = make_identity(source, admission, NULL, UINT64_C(1));
    identity.admission_id = vstr_from_buf(NULL, 0u);
    check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_EINVAL);

    identity = make_identity(source, admission, NULL, UINT64_C(1));
    identity.source_id =
        vstr_from_buf(long_source, TURBO_FLOW_DURABLE_SOURCE_ID_MAX + 1u);
    check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_ERANGE);
    identity = make_identity(source, admission, NULL, UINT64_C(1));
    identity.admission_id =
        vstr_from_buf(long_admission, TURBO_FLOW_DURABLE_ADMISSION_ID_MAX + 1u);
    check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_ERANGE);
    identity = make_identity(source, admission, NULL, UINT64_C(1));
    identity.correlation =
        vstr_from_buf(long_correlation, TURBO_FLOW_DURABLE_CORRELATION_MAX + 1u);
    check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_ERANGE);

    identity = make_identity(source, admission, NULL, UINT64_C(1));
    identity.size = sizeof(identity) - 1u;
    check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_EINVAL);
    identity = make_identity(source, admission, NULL, UINT64_C(1));
    identity.version = TURBO_FLOW_DURABLE_BUFFER_API_VERSION + 1u;
    check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_EINVAL);

    check_equal(turbo_flow_msg_set_durable_identity(NULL, &identity), SALTS_EINVAL);
    check_equal(turbo_flow_msg_set_durable_identity(&msg, NULL), SALTS_EINVAL);
    check_equal(turbo_flow_msg_durable_identity(NULL, &out), SALTS_EINVAL);
    check_equal(turbo_flow_msg_durable_identity(&msg, NULL), SALTS_EINVAL);
    turbo_flow_msg_cleanup(&msg);
  }

  it("rejects identity mutation while a result claim owns the message") {
    char source[] = "source";
    char admission[] = "admission";
    turbo_flow_durable_identity_t identity =
        make_identity(source, admission, NULL, UINT64_C(1));
    turbo_flow_msg_t msg;
    flow_msg_projection_t *projection;

    turbo_flow_msg_init(&msg);
    check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_OK);
    projection = (flow_msg_projection_t *)msg._content_handle;
    check_not_null(projection);
    projection->claim_active = 1;
    identity.source_sequence = UINT64_C(2);
    check_equal(turbo_flow_msg_set_durable_identity(&msg, &identity), SALTS_EBUSY);
    projection->claim_active = 0;
    check_identity_eq(&msg, "source", "admission", NULL, UINT64_C(1));
    turbo_flow_msg_cleanup(&msg);
  }
}
