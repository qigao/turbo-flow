#include "flowie_cluster_pgsql_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <limits.h>
#include <stdlib.h>
#include <string.h>

static flowie_cluster_pgsql_config_t flowie_cluster_pgsql_test_config(void) {
  flowie_cluster_pgsql_config_t config = FLOWIE_CLUSTER_PGSQL_CONFIG_INIT;
  config.conninfo = "host=127.0.0.1 port=1 connect_timeout=1";
  config.schema_name = "flowie_cluster";
  config.cluster_id = "cluster-a";
  config.listener_id = "mqtt-main";
  config.node_id = "node-a";
  config.advertised_endpoint = "127.0.0.1:7101";
  config.boot_id[0] = 1u;
  config.shard_count = 16u;
  config.lease_ttl_ms = 15000u;
  config.renew_interval_ms = 3000u;
  config.retry_interval_ms = 250u;
  config.worst_case_db_latency_ms = 1000u;
  config.safety_margin_ms = 1000u;
  config.create_schema = 1;
  return config;
}

static flowie_cluster_pgsql_fact_config_t
flowie_cluster_pgsql_test_fact_config(const flowie_cluster_pgsql_config_t *coordinator) {
  flowie_cluster_pgsql_fact_config_t config = FLOWIE_CLUSTER_PGSQL_FACT_CONFIG_INIT;
  config.coordinator = coordinator;
  config.max_key_size = 1024u;
  config.max_value_size = 65536u;
  config.max_batch_size = 8u;
  config.max_fact_records = 1024u;
  config.max_receipts = 1024u;
  config.max_dedupe_records = 1024u;
  config.max_outbox_records = 2048u;
  config.max_outbox_bytes = 1024u * 1024u;
  config.max_event_payload_size = 65536u;
  return config;
}

static flowie_cluster_pgsql_fact_command_t
flowie_cluster_pgsql_test_fact_command(const flowie_cluster_pgsql_config_t *coordinator,
                                       flowie_cluster_pgsql_fact_mutation_t *mutation) {
  static const uint8_t key[] = "client-a";
  static const uint8_t value[] = "session-state";
  static const uint8_t event[] = "session-updated";
  flowie_cluster_pgsql_fact_command_t command = FLOWIE_CLUSTER_PGSQL_FACT_COMMAND_INIT;
  uint32_t shard = 0u;
  mutation->shard_key = key;
  mutation->shard_key_size = sizeof(key) - 1u;
  mutation->record.key = key;
  mutation->record.key_size = sizeof(key) - 1u;
  mutation->record.next_revision = 1u;
  mutation->record.value = value;
  mutation->record.value_size = sizeof(value) - 1u;
  mutation->event_type = 1u;
  mutation->event_payload = event;
  mutation->event_payload_size = sizeof(event) - 1u;
  command.command_id[0] = 1u;
  command.mutations = mutation;
  command.mutation_count = 1u;
  (void)flowie_cluster_shard_for_key(
      coordinator->hash_version, mutation->key_kind, (const uint8_t *)coordinator->cluster_id,
      strlen(coordinator->cluster_id), (const uint8_t *)coordinator->listener_id,
      strlen(coordinator->listener_id), key, sizeof(key) - 1u, coordinator->shard_count, &shard);
  (void)flowie_cluster_owner_token_init(&command.owner, shard, 1u, coordinator->node_id,
                                        strlen(coordinator->node_id), coordinator->boot_id);
  return command;
}

spec("flowie PostgreSQL cluster coordinator") {
  group("configuration") {
    it("accepts bounded identifiers and lease timing") {
      flowie_cluster_pgsql_config_t config = flowie_cluster_pgsql_test_config();
      check_int_eq(flowie_cluster_pgsql_config_validate(&config), TURBO_OK);
    }

    it("rejects unsafe schema names and invalid identity") {
      flowie_cluster_pgsql_config_t config = flowie_cluster_pgsql_test_config();
      config.schema_name = "public;drop schema public";
      check_int_eq(flowie_cluster_pgsql_config_validate(&config), TURBO_EINVAL);
      config = flowie_cluster_pgsql_test_config();
      memset(config.boot_id, 0, sizeof(config.boot_id));
      check_int_eq(flowie_cluster_pgsql_config_validate(&config), TURBO_EINVAL);
      config = flowie_cluster_pgsql_test_config();
      config.node_id = "";
      check_int_eq(flowie_cluster_pgsql_config_validate(&config), TURBO_EINVAL);
      config = flowie_cluster_pgsql_test_config();
      config.advertised_endpoint = "";
      check_int_eq(flowie_cluster_pgsql_config_validate(&config), TURBO_EINVAL);
    }

    it("rejects unbounded or non-conservative lease settings") {
      flowie_cluster_pgsql_config_t config = flowie_cluster_pgsql_test_config();
      config.shard_count = FLOWIE_CLUSTER_SHARD_COUNT_MAX + 1u;
      check_int_eq(flowie_cluster_pgsql_config_validate(&config), TURBO_EINVAL);
      config = flowie_cluster_pgsql_test_config();
      config.safety_margin_ms = config.lease_ttl_ms;
      check_int_eq(flowie_cluster_pgsql_config_validate(&config), TURBO_EINVAL);
      config = flowie_cluster_pgsql_test_config();
      config.renew_interval_ms = 13000u;
      check_int_eq(flowie_cluster_pgsql_config_validate(&config), TURBO_EINVAL);
      config.renew_interval_ms = UINT64_MAX;
      check_int_eq(flowie_cluster_pgsql_config_validate(&config), TURBO_ERANGE);
      config = flowie_cluster_pgsql_test_config();
      config.conninfo = "";
      check_int_eq(flowie_cluster_pgsql_config_validate(&config), TURBO_EINVAL);
    }
  }

  group("connection boundary") {
    it("cleans owned membership snapshots idempotently") {
      flowie_cluster_pgsql_membership_snapshot_t snapshot =
          FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
      snapshot.members =
          (flowie_cluster_pgsql_member_t *)calloc(1u, sizeof(*snapshot.members));
      check_not_null(snapshot.members);
      snapshot.member_count = 1u;
      snapshot.membership_revision = 1u;
      flowie_cluster_pgsql_membership_snapshot_cleanup(&snapshot);
      check_null(snapshot.members);
      check_size_eq(snapshot.member_count, 0u);
      check_uint_eq(snapshot.membership_revision, 0u);
      flowie_cluster_pgsql_membership_snapshot_cleanup(&snapshot);
    }

    it("fails closed and clears output when PostgreSQL is unavailable") {
      flowie_cluster_pgsql_config_t config = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_coordinator_t *coordinator =
          (flowie_cluster_pgsql_coordinator_t *)(uintptr_t)1u;
      check_int_eq(flowie_cluster_pgsql_coordinator_open(&config, &coordinator), TURBO_EIO);
      check_null(coordinator);
      flowie_cluster_pgsql_coordinator_destroy(NULL);
    }

    it("fails closed and clears fact store output when PostgreSQL is unavailable") {
      flowie_cluster_pgsql_config_t coordinator = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_fact_config_t config =
          flowie_cluster_pgsql_test_fact_config(&coordinator);
      flowie_cluster_pgsql_fact_store_t *store = (flowie_cluster_pgsql_fact_store_t *)(uintptr_t)1u;
      check_int_eq(flowie_cluster_pgsql_fact_store_open(&config, &store), TURBO_EIO);
      check_null(store);
      flowie_cluster_pgsql_fact_store_destroy(NULL);
    }

    it("validates bounded fact worker admission before opening PostgreSQL") {
      flowie_cluster_pgsql_config_t coordinator = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_fact_config_t fact = flowie_cluster_pgsql_test_fact_config(&coordinator);
      flowie_cluster_pgsql_fact_worker_config_t config =
          FLOWIE_CLUSTER_PGSQL_FACT_WORKER_CONFIG_INIT;
      flowie_cluster_pgsql_fact_worker_t *worker =
          (flowie_cluster_pgsql_fact_worker_t *)(uintptr_t)1u;
      config.fact = &fact;
      config.max_queue_entries = 8u;
      config.max_queue_bytes = 1024u * 1024u;
      check_int_eq(flowie_cluster_pgsql_fact_worker_config_validate(&config), TURBO_OK);
      check_int_eq(flowie_cluster_pgsql_fact_worker_create(&config, &worker), TURBO_EIO);
      check_null(worker);
      config.max_queue_entries = 0u;
      check_int_eq(flowie_cluster_pgsql_fact_worker_config_validate(&config), TURBO_EINVAL);
      flowie_cluster_pgsql_fact_worker_destroy(NULL);
    }
  }

  group("fenced fact command") {
    it("accepts one bounded command mapped to its owner shard") {
      flowie_cluster_pgsql_config_t coordinator = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_fact_config_t config =
          flowie_cluster_pgsql_test_fact_config(&coordinator);
      flowie_cluster_pgsql_fact_mutation_t mutation = FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
      flowie_cluster_pgsql_fact_command_t command =
          flowie_cluster_pgsql_test_fact_command(&coordinator, &mutation);
      check_int_eq(flowie_cluster_pgsql_fact_config_validate(&config), TURBO_OK);
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_OK);
    }

    it("validates immutable target-delivery dedupe identity") {
      flowie_cluster_pgsql_config_t coordinator = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_fact_config_t config =
          flowie_cluster_pgsql_test_fact_config(&coordinator);
      flowie_cluster_pgsql_fact_mutation_t mutation = FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
      flowie_cluster_pgsql_fact_command_t command =
          flowie_cluster_pgsql_test_fact_command(&coordinator, &mutation);
      flowie_cluster_pgsql_event_dedupe_t dedupe = FLOWIE_CLUSTER_PGSQL_EVENT_DEDUPE_INIT;
      dedupe.source_command_id[0] = 2u;
      dedupe.source_shard_id = command.owner.shard_id;
      dedupe.source_owner_epoch = 3u;
      dedupe.source_fact_revision = 4u;
      dedupe.target_session_id = 5u;
      dedupe.event_digest[0] = 6u;
      command.dedupe = &dedupe;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_OK);
      dedupe.shared_filter = (const uint8_t *)"$share/g/a/#";
      dedupe.shared_filter_size = sizeof("$share/g/a/#") - 1u;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_OK);
      dedupe.shared_filter = NULL;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
      dedupe.shared_filter = (const uint8_t *)"$share/g/a/#";
      dedupe.shared_filter_size = 0u;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
      dedupe.shared_filter = NULL;

      memset(dedupe.source_command_id, 0, sizeof(dedupe.source_command_id));
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
      dedupe.source_command_id[0] = 2u;
      dedupe.source_owner_epoch = 0u;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
      dedupe.source_owner_epoch = 3u;
      dedupe.target_session_id = 0u;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_OK);
      dedupe.target_session_id = 5u;
      memset(dedupe.event_digest, 0, sizeof(dedupe.event_digest));
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
    }

    it("accepts a fenced dedupe-only terminal delivery decision") {
      flowie_cluster_pgsql_config_t coordinator = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_fact_config_t config =
          flowie_cluster_pgsql_test_fact_config(&coordinator);
      flowie_cluster_pgsql_fact_mutation_t mutation = FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
      flowie_cluster_pgsql_fact_command_t command =
          flowie_cluster_pgsql_test_fact_command(&coordinator, &mutation);
      flowie_cluster_pgsql_event_dedupe_t dedupe = FLOWIE_CLUSTER_PGSQL_EVENT_DEDUPE_INIT;
      dedupe.source_command_id[0] = 2u;
      dedupe.source_shard_id = command.owner.shard_id;
      dedupe.source_owner_epoch = 3u;
      dedupe.target_session_id = 5u;
      dedupe.event_digest[0] = 6u;
      command.dedupe = &dedupe;
      command.mutations = NULL;
      command.mutation_count = 0u;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_OK);
      command.dedupe = NULL;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
    }

    it("requires an explicit delivery dedupe capacity bound") {
      flowie_cluster_pgsql_config_t coordinator = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_fact_config_t config =
          flowie_cluster_pgsql_test_fact_config(&coordinator);
      config.max_dedupe_records = 0u;
      check_int_eq(flowie_cluster_pgsql_fact_config_validate(&config), TURBO_EINVAL);
    }

    it("accepts a fenced event-only command without inventing a fact revision") {
      flowie_cluster_pgsql_config_t coordinator = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_fact_config_t config =
          flowie_cluster_pgsql_test_fact_config(&coordinator);
      flowie_cluster_pgsql_fact_mutation_t mutation = FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
      flowie_cluster_pgsql_fact_command_t command =
          flowie_cluster_pgsql_test_fact_command(&coordinator, &mutation);
      mutation.write_kind = FLOWIE_CLUSTER_PGSQL_EVENT_ONLY;
      mutation.record.next_revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
      mutation.record.value = NULL;
      mutation.record.value_size = 0u;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_OK);
      mutation.record.next_revision = 1u;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
      mutation.record.next_revision = TURBO_FLOW_RECORD_REVISION_ABSENT;
      mutation.write_kind = (flowie_cluster_pgsql_fact_write_kind_t)0;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
    }

    it("accepts a fact-only offline delivery without inventing an outbox intent") {
      flowie_cluster_pgsql_config_t coordinator = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_fact_config_t config =
          flowie_cluster_pgsql_test_fact_config(&coordinator);
      flowie_cluster_pgsql_fact_mutation_t mutation = FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
      flowie_cluster_pgsql_fact_command_t command =
          flowie_cluster_pgsql_test_fact_command(&coordinator, &mutation);
      mutation.write_kind = FLOWIE_CLUSTER_PGSQL_FACT_ONLY;
      mutation.event_type = 0u;
      mutation.event_payload = NULL;
      mutation.event_payload_size = 0u;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_OK);
      mutation.event_type = 1u;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
      mutation.event_type = 0u;
      mutation.event_payload = mutation.record.value;
      mutation.event_payload_size = mutation.record.value_size;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
    }

    it("rejects unstable IDs, stale owners and duplicate fact keys") {
      flowie_cluster_pgsql_config_t coordinator = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_fact_config_t config =
          flowie_cluster_pgsql_test_fact_config(&coordinator);
      flowie_cluster_pgsql_fact_mutation_t mutations[2] = {FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT,
                                                           FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT};
      flowie_cluster_pgsql_fact_command_t command =
          flowie_cluster_pgsql_test_fact_command(&coordinator, &mutations[0]);
      memset(command.command_id, 0, sizeof(command.command_id));
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
      command.command_id[0] = 1u;
      command.owner.node_id[0] = 'x';
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EBUSY);
      command.owner.node_id[0] = coordinator.node_id[0];
      mutations[1] = mutations[0];
      command.mutations = mutations;
      command.mutation_count = 2u;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_EINVAL);
    }

    it("enforces event byte capacity before database I/O") {
      flowie_cluster_pgsql_config_t coordinator = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_fact_config_t config =
          flowie_cluster_pgsql_test_fact_config(&coordinator);
      flowie_cluster_pgsql_fact_mutation_t mutation = FLOWIE_CLUSTER_PGSQL_FACT_MUTATION_INIT;
      flowie_cluster_pgsql_fact_command_t command =
          flowie_cluster_pgsql_test_fact_command(&coordinator, &mutation);
      config.max_outbox_bytes = mutation.event_payload_size - 1u;
      check_int_eq(flowie_cluster_pgsql_fact_command_validate(&config, &command), TURBO_ENOSPC);
    }

    it("validates a bounded shard scan against the exact configured owner") {
      flowie_cluster_pgsql_config_t coordinator = flowie_cluster_pgsql_test_config();
      flowie_cluster_pgsql_fact_config_t config =
          flowie_cluster_pgsql_test_fact_config(&coordinator);
      flowie_cluster_pgsql_fact_scan_t scan = FLOWIE_CLUSTER_PGSQL_FACT_SCAN_INIT;
      check_int_eq(flowie_cluster_owner_token_init(&scan.owner, 0u, 1u, coordinator.node_id,
                                                   strlen(coordinator.node_id),
                                                   coordinator.boot_id),
                   TURBO_OK);
      scan.max_records = 4u;
      check_int_eq(flowie_cluster_pgsql_fact_scan_validate(&config, &scan), TURBO_OK);
      scan.owner.node_id[0] = 'x';
      check_int_eq(flowie_cluster_pgsql_fact_scan_validate(&config, &scan), TURBO_EBUSY);
      scan.owner.node_id[0] = coordinator.node_id[0];
      scan.max_records = 0u;
      check_int_eq(flowie_cluster_pgsql_fact_scan_validate(&config, &scan), TURBO_EINVAL);
      scan.max_records = config.max_fact_records + 1u;
      check_int_eq(flowie_cluster_pgsql_fact_scan_validate(&config, &scan), TURBO_EINVAL);
      scan.max_records = (size_t)INT_MAX;
      check_int_eq(flowie_cluster_pgsql_fact_scan_validate(&config, &scan), TURBO_EINVAL);
      check_int_eq(flowie_cluster_pgsql_fact_scan(NULL, &scan, NULL, NULL), TURBO_EINVAL);
    }
  }
}
