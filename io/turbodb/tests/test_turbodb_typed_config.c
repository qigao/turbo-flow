#include "tinytest.h"
#include "turbo_flow_turbodb_durable_internal.h"

static void fill_valid(DurableTurboDbConfig_t *config) {
  DurableTurboDbConfig_init(config);
  config->schema_version = 2u;
  config->max_message_bytes = 4096u;
  config->max_records = 128u;
  config->max_total_bytes = 1024u * 1024u;
  config->max_record_bytes = 8192u;
  config->max_claims = 32u;
  config->connection_count = 2u;
  config->identity_mode = DurableIdentityMode_Generated;
  config->expected_generation = 0u;
  config->open_mode = InboxOpenMode_Exclusive;
}

spec("TurboDB typed provider config") {
  it("converts validated typed policy without deployment database identity") {
    DurableTurboDbConfig_t typed;
    durable_turbodb_config_t config;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    fill_valid(&typed);
    check_equal(
        durable_turbodb_config_from_typed(
            &typed, "durable_buffer", &config, &error),
        SALTS_OK);
    check_equal(config.max_message_bytes, (size_t)4096u);
    check_equal(config.inbox.max_records, (size_t)128u);
    check_equal(config.inbox.max_total_bytes, (size_t)(1024u * 1024u));
    check_equal(config.inbox.max_record_bytes, (size_t)8192u);
    check_equal(config.inbox.max_claims, (size_t)32u);
    check_equal(config.inbox.connection_count, 2u);
    check_equal(
        config.inbox.open_mode, TURBO_FLOW_TURBODB_INBOX_OPEN_EXCLUSIVE);
    check_equal(config.inbox.expected_generation, UINT64_C(0));
    check_equal(
        config.identity_mode, TURBO_FLOW_DURABLE_IDENTITY_GENERATED);
    check_null(config.inbox.database);
    check_null(config.inbox.namespace_name);

    DurableTurboDbConfig_clear(&typed);
  }

  it("rejects cross-field capacity violations") {
    DurableTurboDbConfig_t typed;
    durable_turbodb_config_t config;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    fill_valid(&typed);
    typed.max_record_bytes = typed.max_total_bytes + 1u;
    check_equal(
        durable_turbodb_config_from_typed(
            &typed, "durable_buffer", &config, &error),
        SALTS_ERANGE);
    check_contains(error.path, "max_record_bytes");

    DurableTurboDbConfig_clear(&typed);
    fill_valid(&typed);
    typed.max_claims = typed.max_records + 1u;
    check_equal(
        durable_turbodb_config_from_typed(
            &typed, "durable_buffer", &config, &error),
        SALTS_ERANGE);
    check_contains(error.path, "max_claims");

    DurableTurboDbConfig_clear(&typed);
  }

  it("enforces explicit generation semantics for takeover") {
    DurableTurboDbConfig_t typed;
    durable_turbodb_config_t config;
    turbo_flow_config_error_t error = TURBO_FLOW_CONFIG_ERROR_INIT;

    fill_valid(&typed);
    typed.open_mode = InboxOpenMode_Takeover;
    typed.expected_generation = 0u;
    check_equal(
        durable_turbodb_config_from_typed(
            &typed, "durable_buffer", &config, &error),
        SALTS_EINVAL);
    check_contains(error.path, "expected_generation");

    typed.expected_generation = 7u;
    check_equal(
        durable_turbodb_config_from_typed(
            &typed, "durable_buffer", &config, &error),
        SALTS_OK);
    check_equal(
        config.inbox.open_mode, TURBO_FLOW_TURBODB_INBOX_OPEN_TAKEOVER);
    check_equal(config.inbox.expected_generation, UINT64_C(7));

    DurableTurboDbConfig_clear(&typed);
  }
}
