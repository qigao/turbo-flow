#include "flowie_cluster_pgsql_internal.h"

#include "libpq-fe.h"
#include "turbo_error.h"
#include "turbo_str.h"
#include "turbo_thread.h"

#include <errno.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#define FLOWIE_CLUSTER_PGSQL_SCHEMA_VERSION 3u
#define FLOWIE_CLUSTER_PGSQL_OID_BYTEA 17u
#define FLOWIE_CLUSTER_PGSQL_OID_TEXT 25u

struct flowie_cluster_pgsql_coordinator_s {
  PGconn *connection;
  tstr_t conninfo;
  tstr_t schema_name;
  tstr_t cluster_id;
  tstr_t listener_id;
  tstr_t node_id;
  tstr_t advertised_endpoint;
  uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE];
  uint32_t shard_count;
  uint64_t lease_ttl_ms;
  uint64_t worst_case_db_latency_ms;
  uint64_t safety_margin_ms;
  tstr_t node_claim_sql;
  tstr_t shard_ensure_sql;
  tstr_t claim_sql;
  tstr_t node_renew_sql;
  tstr_t renew_sql;
  tstr_t require_sql;
  tstr_t release_sql;
};

static int flowie_cluster_pgsql_identifier_valid(const char *name, size_t maximum) {
  size_t length;
  if (!name) return 0;
  length = strnlen(name, maximum + 1u);
  if (length == 0u || length > maximum || !((name[0] >= 'a' && name[0] <= 'z') || name[0] == '_'))
    return 0;
  for (size_t index = 1u; index < length; ++index) {
    const char byte = name[index];
    if (!((byte >= 'a' && byte <= 'z') || (byte >= '0' && byte <= '9') || byte == '_')) return 0;
  }
  return 1;
}

static int
flowie_cluster_pgsql_nonzero_boot_id(const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  uint8_t combined = 0u;
  if (!boot_id) return 0;
  for (size_t index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index)
    combined |= boot_id[index];
  return combined != 0u;
}

int flowie_cluster_pgsql_config_validate(const flowie_cluster_pgsql_config_t *config) {
  size_t cluster_id_size;
  size_t listener_id_size;
  size_t node_id_size;
  uint64_t renewal_budget;
  if (!config || config->size < sizeof(*config) ||
      config->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 || !config->conninfo ||
      config->conninfo[0] == '\0' ||
      strnlen(config->conninfo, FLOWIE_CLUSTER_PGSQL_CONNINFO_MAX + 1u) >
          FLOWIE_CLUSTER_PGSQL_CONNINFO_MAX ||
      !flowie_cluster_pgsql_identifier_valid(config->schema_name,
                                             FLOWIE_CLUSTER_PGSQL_SCHEMA_NAME_MAX) ||
      !config->cluster_id || !config->listener_id || !config->node_id ||
      !config->advertised_endpoint || config->advertised_endpoint[0] == '\0' ||
      strnlen(config->advertised_endpoint, FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX + 1u) >
          FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX ||
      config->hash_version != FLOWIE_CLUSTER_HASH_VERSION_1 || config->shard_count == 0u ||
      config->shard_count > FLOWIE_CLUSTER_SHARD_COUNT_MAX || config->lease_ttl_ms == 0u ||
      config->lease_ttl_ms > INT64_MAX || config->renew_interval_ms == 0u ||
      config->retry_interval_ms == 0u || config->worst_case_db_latency_ms == 0u ||
      config->worst_case_db_latency_ms > (uint64_t)INT_MAX * FLOWIE_CLUSTER_PGSQL_MS_PER_SECOND ||
      config->safety_margin_ms == 0u || config->safety_margin_ms >= config->lease_ttl_ms ||
      !flowie_cluster_pgsql_nonzero_boot_id(config->boot_id) ||
      (config->create_schema != 0 && config->create_schema != 1))
    return TURBO_EINVAL;
  cluster_id_size = strnlen(config->cluster_id, FLOWIE_CLUSTER_ID_MAX + 1u);
  listener_id_size = strnlen(config->listener_id, FLOWIE_CLUSTER_LISTENER_ID_MAX + 1u);
  node_id_size = strnlen(config->node_id, FLOWIE_CLUSTER_NODE_ID_MAX + 1u);
  if (cluster_id_size == 0u || cluster_id_size > FLOWIE_CLUSTER_ID_MAX || listener_id_size == 0u ||
      listener_id_size > FLOWIE_CLUSTER_LISTENER_ID_MAX || node_id_size == 0u ||
      node_id_size > FLOWIE_CLUSTER_NODE_ID_MAX)
    return TURBO_EINVAL;
  if (config->renew_interval_ms > UINT64_MAX - config->worst_case_db_latency_ms)
    return TURBO_ERANGE;
  renewal_budget = config->renew_interval_ms + config->worst_case_db_latency_ms;
  if (renewal_budget > UINT64_MAX - config->safety_margin_ms) return TURBO_ERANGE;
  renewal_budget += config->safety_margin_ms;
  if (renewal_budget >= config->lease_ttl_ms) return TURBO_EINVAL;
  return TURBO_OK;
}

static int flowie_cluster_pgsql_sqlstate_status(const char *sqlstate) {
  if (sqlstate && (strcmp(sqlstate, "40001") == 0 || strcmp(sqlstate, "40P01") == 0 ||
                   strcmp(sqlstate, "55P03") == 0))
    return TURBO_EBUSY;
  if (sqlstate && strcmp(sqlstate, "57014") == 0) return TURBO_ETIMEDOUT;
  return TURBO_EIO;
}

static int flowie_cluster_pgsql_result_status(PGresult *result, ExecStatusType expected) {
  if (result && PQresultStatus(result) == expected) return TURBO_OK;
  return flowie_cluster_pgsql_sqlstate_status(result ? PQresultErrorField(result, PG_DIAG_SQLSTATE)
                                                     : NULL);
}

static int flowie_cluster_pgsql_exec(PGconn *connection, const char *sql, ExecStatusType expected) {
  PGresult *result;
  int rc;
  if (!connection || !sql) return TURBO_EINVAL;
  result = PQexec(connection, sql);
  rc = flowie_cluster_pgsql_result_status(result, expected);
  if (result) PQclear(result);
  return rc;
}

static int flowie_cluster_pgsql_rollback(flowie_cluster_pgsql_coordinator_t *coordinator,
                                         int status) {
  if (coordinator && coordinator->connection)
    (void)flowie_cluster_pgsql_exec(coordinator->connection, "ROLLBACK", PGRES_COMMAND_OK);
  return status;
}

static int flowie_cluster_pgsql_parse_u64(const char *text, size_t size, uint64_t *out) {
  char buffer[32];
  char *end = NULL;
  unsigned long long value;
  if (!text || size == 0u || size >= sizeof(buffer) || !out) return TURBO_EPROTO;
  memcpy(buffer, text, size);
  buffer[size] = '\0';
  errno = 0;
  value = strtoull(buffer, &end, 10);
  if (errno != 0 || !end || *end != '\0') return TURBO_EPROTO;
  *out = (uint64_t)value;
  return TURBO_OK;
}

static int flowie_cluster_pgsql_set_search_path(PGconn *connection) {
  static const char sql[] = "SELECT pg_catalog.set_config('search_path','',false)";
  return flowie_cluster_pgsql_exec(connection, sql, PGRES_TUPLES_OK);
}

static PGconn *flowie_cluster_pgsql_connect(const char *conninfo, uint64_t timeout_ms) {
  const char *keywords[3] = {"dbname", "connect_timeout", NULL};
  const char *values[3];
  char timeout_seconds[32];
  uint64_t seconds = timeout_ms / FLOWIE_CLUSTER_PGSQL_MS_PER_SECOND;
  int written;
  if (timeout_ms % FLOWIE_CLUSTER_PGSQL_MS_PER_SECOND != 0u) ++seconds;
  if (seconds == 0u) seconds = 1u;
  written = snprintf(timeout_seconds, sizeof(timeout_seconds), "%llu", (unsigned long long)seconds);
  if (written <= 0 || (size_t)written >= sizeof(timeout_seconds)) return NULL;
  values[0] = conninfo;
  values[1] = timeout_seconds;
  values[2] = NULL;
  return PQconnectdbParams(keywords, values, 1);
}

static int flowie_cluster_pgsql_set_timeouts(PGconn *connection, uint64_t timeout_ms) {
  static const char sql[] = "SELECT pg_catalog.set_config('statement_timeout',$1,false),"
                            "pg_catalog.set_config('lock_timeout',$1,false)";
  char timeout[32];
  const char *values[1] = {timeout};
  PGresult *result;
  int written = snprintf(timeout, sizeof(timeout), "%llu", (unsigned long long)timeout_ms);
  int rc;
  if (written <= 0 || (size_t)written >= sizeof(timeout)) return TURBO_ERANGE;
  result = PQexecParams(connection, sql, 1, NULL, values, NULL, NULL, 0);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 2)) rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static tstr_t flowie_cluster_pgsql_schema_ddl(const char *schema) {
  tstr_t sql = tstr_new();
  tstr_t next;
  if (!sql) return NULL;
#define FLOWIE_CLUSTER_PGSQL_APPEND(...)                                                           \
  do {                                                                                             \
    next = tstr_cat_fmt(sql, __VA_ARGS__);                                                         \
    if (!next) {                                                                                   \
      tstr_free(sql);                                                                              \
      return NULL;                                                                                 \
    }                                                                                              \
    sql = next;                                                                                    \
  } while (0)
  FLOWIE_CLUSTER_PGSQL_APPEND("CREATE SCHEMA IF NOT EXISTS %s;", schema);
  FLOWIE_CLUSTER_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.cluster_config("
      "cluster_id TEXT PRIMARY KEY,schema_version INTEGER NOT NULL CHECK(schema_version>0),"
      "hash_version INTEGER NOT NULL CHECK(hash_version>0),shard_count INTEGER NOT NULL "
      "CHECK(shard_count>0),membership_revision BIGINT NOT NULL CHECK(membership_revision>=0),"
      "created_at TIMESTAMPTZ NOT NULL DEFAULT pg_catalog.clock_timestamp());",
      schema);
  FLOWIE_CLUSTER_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.cluster_node("
      "cluster_id TEXT NOT NULL,node_id TEXT NOT NULL,boot_id BYTEA NOT NULL CHECK(octet_length("
      "boot_id)=%u),state SMALLINT NOT NULL CHECK(state BETWEEN 1 AND %u),"
      "advertised_endpoint TEXT,lease_until TIMESTAMPTZ "
      "NOT NULL,revision BIGINT NOT NULL CHECK(revision>0),PRIMARY KEY(cluster_id,node_id),"
      "CONSTRAINT cluster_node_advertised_endpoint_check CHECK(advertised_endpoint IS NULL OR "
      "(octet_length(advertised_endpoint)>0 AND octet_length(advertised_endpoint)<=%u)),"
      "FOREIGN KEY(cluster_id) REFERENCES %s.cluster_config(cluster_id));",
      schema, FLOWIE_CLUSTER_BOOT_ID_SIZE, FLOWIE_CLUSTER_NODE_EXPIRED,
      FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX, schema);
  FLOWIE_CLUSTER_PGSQL_APPEND(
      "ALTER TABLE %s.cluster_node ADD COLUMN IF NOT EXISTS advertised_endpoint TEXT;",
      schema);
  FLOWIE_CLUSTER_PGSQL_APPEND(
      "CREATE TABLE IF NOT EXISTS %s.cluster_shard("
      "cluster_id TEXT NOT NULL,listener_id TEXT NOT NULL,shard_id INTEGER NOT NULL "
      "CHECK(shard_id>=0),owner_node_id TEXT,owner_boot_id BYTEA CHECK(owner_boot_id IS NULL OR "
      "octet_length(owner_boot_id)=%u),owner_epoch BIGINT NOT NULL CHECK(owner_epoch>=0),"
      "lease_until TIMESTAMPTZ,revision BIGINT NOT NULL CHECK(revision>0),"
      "CHECK((owner_node_id IS NULL)=(owner_boot_id IS NULL)),"
      "CHECK((owner_node_id IS NULL)=(lease_until IS NULL)),"
      "PRIMARY KEY(cluster_id,listener_id,shard_id),"
      "FOREIGN KEY(cluster_id) REFERENCES %s.cluster_config(cluster_id));",
      schema, FLOWIE_CLUSTER_BOOT_ID_SIZE, schema);
#undef FLOWIE_CLUSTER_PGSQL_APPEND
  return sql;
}

static int flowie_cluster_pgsql_schema_prepare(flowie_cluster_pgsql_coordinator_t *coordinator,
                                               int create_schema, uint32_t hash_version,
                                               uint32_t shard_count) {
  static const Oid config_types[3] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                                      FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  tstr_t ddl = NULL;
  tstr_t config_sql = NULL;
  tstr_t next;
  char hash_text[16];
  char shard_text[16];
  const char *values[3];
  PGresult *result = NULL;
  int parameter_count;
  int rc;
  if (create_schema) {
    ddl = flowie_cluster_pgsql_schema_ddl(coordinator->schema_name);
    if (!ddl) return TURBO_ENOMEM;
    rc = flowie_cluster_pgsql_exec(coordinator->connection, ddl, PGRES_COMMAND_OK);
    tstr_free(ddl);
    if (rc != TURBO_OK) return rc;
  }
  config_sql = tstr_new();
  if (!config_sql) return TURBO_ENOMEM;
  if (create_schema) {
    next = tstr_cat_fmt(
        config_sql,
        "WITH upgraded AS (UPDATE %s.cluster_config SET schema_version=%u WHERE cluster_id=$1 AND "
        "schema_version IN (1,2) AND NOT EXISTS(SELECT 1 FROM %s.cluster_node WHERE cluster_id=$1) "
        "RETURNING schema_version,hash_version,shard_count),inserted AS (INSERT INTO "
        "%s.cluster_config(cluster_id,schema_version,hash_version,"
        "shard_count,membership_revision) VALUES($1,%u,$2::integer,$3::integer,0) ON CONFLICT("
        "cluster_id) DO NOTHING RETURNING schema_version,hash_version,shard_count) SELECT "
        "schema_version::text,hash_version::text,shard_count::text FROM upgraded UNION ALL SELECT "
        "schema_version::text,hash_version::text,shard_count::text FROM inserted UNION ALL SELECT "
        "schema_version::text,hash_version::text,shard_count::text FROM %s.cluster_config WHERE "
        "cluster_id=$1 AND NOT EXISTS(SELECT 1 FROM upgraded) AND NOT EXISTS(SELECT 1 FROM inserted)",
        coordinator->schema_name, FLOWIE_CLUSTER_PGSQL_SCHEMA_VERSION, coordinator->schema_name,
        coordinator->schema_name, FLOWIE_CLUSTER_PGSQL_SCHEMA_VERSION, coordinator->schema_name);
    parameter_count = 3;
  } else {
    next = tstr_cat_fmt(config_sql,
                        "SELECT schema_version::text,hash_version::text,shard_count::text FROM "
                        "%s.cluster_config WHERE cluster_id=$1",
                        coordinator->schema_name);
    parameter_count = 1;
  }
  if (!next) {
    tstr_free(config_sql);
    return TURBO_ENOMEM;
  }
  config_sql = next;
  if (snprintf(hash_text, sizeof(hash_text), "%u", hash_version) <= 0 ||
      snprintf(shard_text, sizeof(shard_text), "%u", shard_count) <= 0) {
    tstr_free(config_sql);
    return TURBO_ERANGE;
  }
  values[0] = coordinator->cluster_id;
  values[1] = hash_text;
  values[2] = shard_text;
  result = PQexecParams(coordinator->connection, config_sql, parameter_count, config_types, values,
                        NULL, NULL, 0);
  tstr_free(config_sql);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && (PQntuples(result) != 1 || PQnfields(result) != 3 ||
                         PQgetisnull(result, 0, 0) || PQgetisnull(result, 0, 1) ||
                         PQgetisnull(result, 0, 2) || strcmp(PQgetvalue(result, 0, 0), "2") != 0 ||
                         strcmp(PQgetvalue(result, 0, 1), hash_text) != 0 ||
                         strcmp(PQgetvalue(result, 0, 2), shard_text) != 0))
    rc = TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}

static int flowie_cluster_pgsql_sql_prepare(flowie_cluster_pgsql_coordinator_t *coordinator) {
  const char *schema = coordinator->schema_name;
  tstr_t next;
#define FLOWIE_CLUSTER_PGSQL_PREPARE(field, ...)                                                   \
  do {                                                                                             \
    coordinator->field = tstr_new();                                                               \
    if (!coordinator->field) return TURBO_ENOMEM;                                                  \
    next = tstr_cat_fmt(coordinator->field, __VA_ARGS__);                                          \
    if (!next) return TURBO_ENOMEM;                                                                \
    coordinator->field = next;                                                                     \
  } while (0)
  FLOWIE_CLUSTER_PGSQL_PREPARE(
      node_claim_sql,
      "WITH locked AS MATERIALIZED (SELECT membership_revision FROM %s.cluster_config WHERE "
      "cluster_id=$1 AND schema_version=%u FOR UPDATE),changed AS MATERIALIZED (SELECT "
      "NOT EXISTS(SELECT 1 FROM %s.cluster_node n WHERE n.cluster_id=$1 AND n.node_id=$4) OR "
      "EXISTS(SELECT 1 FROM %s.cluster_node n WHERE n.cluster_id=$1 AND n.node_id=$4 AND "
      "(n.boot_id<>$5 OR n.state NOT IN (%u,%u)) AND (n.boot_id<>$5 OR n.state<>%u OR "
      "n.advertised_endpoint IS DISTINCT FROM $8)) AS value FROM locked),advanced AS (UPDATE "
      "%s.cluster_config SET membership_revision=membership_revision+1 WHERE cluster_id=$1 AND "
      "(SELECT value FROM changed) RETURNING membership_revision),current_revision AS (SELECT "
      "membership_revision FROM advanced UNION ALL SELECT membership_revision FROM locked WHERE "
      "NOT (SELECT value FROM changed)) INSERT INTO %s.cluster_node(cluster_id,node_id,boot_id,"
      "state,advertised_endpoint,lease_until,revision) SELECT $1,$4,$5,%u,$8,"
      "pg_catalog.clock_timestamp()+$7::bigint*interval '1 millisecond',membership_revision FROM "
      "current_revision ON CONFLICT(cluster_id,node_id) DO UPDATE SET boot_id=excluded.boot_id,"
      "state=excluded.state,advertised_endpoint=excluded.advertised_endpoint,"
      "lease_until=excluded.lease_until,revision=excluded.revision WHERE "
      "%s.cluster_node.boot_id<>excluded.boot_id OR %s.cluster_node.state NOT IN (%u,%u)",
      schema, FLOWIE_CLUSTER_PGSQL_SCHEMA_VERSION, schema, schema, FLOWIE_CLUSTER_NODE_OFFLINE,
      FLOWIE_CLUSTER_NODE_EXPIRED, FLOWIE_CLUSTER_NODE_READY, schema, schema,
      FLOWIE_CLUSTER_NODE_READY, schema, schema, FLOWIE_CLUSTER_NODE_OFFLINE,
      FLOWIE_CLUSTER_NODE_EXPIRED);
  FLOWIE_CLUSTER_PGSQL_PREPARE(
      shard_ensure_sql,
      "INSERT INTO %s.cluster_shard(cluster_id,listener_id,shard_id,owner_epoch,revision) "
      "VALUES($1,$2,$3::integer,0,1) ON CONFLICT DO NOTHING",
      schema);
  FLOWIE_CLUSTER_PGSQL_PREPARE(
      claim_sql,
      "UPDATE %s.cluster_shard SET owner_node_id=$4,owner_boot_id=$5,"
      "owner_epoch=owner_epoch+1,lease_until=pg_catalog.clock_timestamp()+$7::bigint*interval "
      "'1 millisecond',revision=revision+1 WHERE cluster_id=$1 AND listener_id=$2 AND "
      "shard_id=$3::integer AND owner_epoch<9223372036854775807 AND "
      "(owner_node_id IS NULL OR lease_until<=pg_catalog.clock_timestamp()) AND EXISTS(SELECT 1 "
      "FROM %s.cluster_node live WHERE live.cluster_id=$1 AND live.node_id=$4 AND "
      "live.boot_id=$5 AND live.state=%u AND live.lease_until>pg_catalog.clock_timestamp()) RETURNING "
      "owner_epoch::text,GREATEST(1,FLOOR(EXTRACT(EPOCH FROM "
      "(LEAST(lease_until,(SELECT n.lease_until FROM %s.cluster_node n WHERE n.cluster_id=$1 AND "
      "n.node_id=$4 AND n.boot_id=$5))-pg_catalog.clock_timestamp()))*1000)::bigint)::text",
      schema, schema, FLOWIE_CLUSTER_NODE_READY, schema);
  FLOWIE_CLUSTER_PGSQL_PREPARE(
      node_renew_sql,
      "UPDATE %s.cluster_node SET lease_until=pg_catalog.clock_timestamp()+$7::bigint*interval "
      "'1 millisecond' WHERE cluster_id=$1 AND node_id=$4 AND boot_id=$5 AND state IN (%u,%u) "
      "AND lease_until>pg_catalog.clock_timestamp() RETURNING 1",
      schema, FLOWIE_CLUSTER_NODE_READY, FLOWIE_CLUSTER_NODE_DRAINING);
  FLOWIE_CLUSTER_PGSQL_PREPARE(
      renew_sql,
      "UPDATE %s.cluster_shard SET lease_until=pg_catalog.clock_timestamp()+$7::bigint*interval "
      "'1 millisecond',revision=revision+1 WHERE cluster_id=$1 AND listener_id=$2 AND "
      "shard_id=$3::integer AND owner_node_id=$4 AND owner_boot_id=$5 AND owner_epoch=$6::bigint "
      "AND lease_until>pg_catalog.clock_timestamp() RETURNING GREATEST(1,FLOOR(EXTRACT(EPOCH FROM "
      "(LEAST(lease_until,(SELECT n.lease_until FROM %s.cluster_node n WHERE n.cluster_id=$1 AND "
      "n.node_id=$4 AND n.boot_id=$5))-pg_catalog.clock_timestamp()))*1000)::bigint)::text",
      schema, schema);
  FLOWIE_CLUSTER_PGSQL_PREPARE(
      require_sql,
      "SELECT GREATEST(1,FLOOR(EXTRACT(EPOCH FROM "
      "(LEAST(s.lease_until,n.lease_until)-pg_catalog.clock_timestamp()))*1000)::bigint)::text "
      "FROM "
      "%s.cluster_shard s "
      "JOIN %s.cluster_node n ON n.cluster_id=s.cluster_id AND n.node_id=s.owner_node_id WHERE "
      "s.cluster_id=$1 AND s.listener_id=$2 AND s.shard_id=$3::integer AND s.owner_node_id=$4 AND "
      "s.owner_boot_id=$5 AND s.owner_epoch=$6::bigint AND "
      "s.lease_until>pg_catalog.clock_timestamp() AND n.boot_id=$5 AND "
      "n.state IN (%u,%u) AND n.lease_until>pg_catalog.clock_timestamp()",
      schema, schema, FLOWIE_CLUSTER_NODE_READY, FLOWIE_CLUSTER_NODE_DRAINING);
  FLOWIE_CLUSTER_PGSQL_PREPARE(
      release_sql,
      "UPDATE %s.cluster_shard SET owner_node_id=NULL,owner_boot_id=NULL,lease_until=NULL,"
      "revision=revision+1 WHERE cluster_id=$1 AND listener_id=$2 AND shard_id=$3::integer AND "
      "owner_node_id=$4 AND owner_boot_id=$5 AND owner_epoch=$6::bigint AND "
      "lease_until>pg_catalog.clock_timestamp() AND EXISTS(SELECT 1 FROM %s.cluster_node n WHERE "
      "n.cluster_id=$1 AND n.node_id=$4 AND n.boot_id=$5 AND "
      "n.lease_until>pg_catalog.clock_timestamp()) RETURNING 1",
      schema, schema);
#undef FLOWIE_CLUSTER_PGSQL_PREPARE
  return TURBO_OK;
}

void flowie_cluster_pgsql_coordinator_destroy(flowie_cluster_pgsql_coordinator_t *coordinator) {
  if (!coordinator) return;
  if (coordinator->connection) PQfinish(coordinator->connection);
  tstr_freep(&coordinator->conninfo);
  tstr_freep(&coordinator->schema_name);
  tstr_freep(&coordinator->cluster_id);
  tstr_freep(&coordinator->listener_id);
  tstr_freep(&coordinator->node_id);
  tstr_freep(&coordinator->advertised_endpoint);
  tstr_freep(&coordinator->node_claim_sql);
  tstr_freep(&coordinator->shard_ensure_sql);
  tstr_freep(&coordinator->claim_sql);
  tstr_freep(&coordinator->node_renew_sql);
  tstr_freep(&coordinator->renew_sql);
  tstr_freep(&coordinator->require_sql);
  tstr_freep(&coordinator->release_sql);
  free(coordinator);
}

int flowie_cluster_pgsql_coordinator_open(const flowie_cluster_pgsql_config_t *config,
                                          flowie_cluster_pgsql_coordinator_t **out) {
  flowie_cluster_pgsql_coordinator_t *coordinator;
  int rc;
  if (out) *out = NULL;
  rc = flowie_cluster_pgsql_config_validate(config);
  if (rc != TURBO_OK || !out) return rc == TURBO_OK ? TURBO_EINVAL : rc;
  coordinator = (flowie_cluster_pgsql_coordinator_t *)calloc(1u, sizeof(*coordinator));
  if (!coordinator) return TURBO_ENOMEM;
  coordinator->conninfo = tstr_dup(config->conninfo);
  coordinator->schema_name = tstr_dup(config->schema_name);
  coordinator->cluster_id = tstr_dup(config->cluster_id);
  coordinator->listener_id = tstr_dup(config->listener_id);
  coordinator->node_id = tstr_dup(config->node_id);
  coordinator->advertised_endpoint = tstr_dup(config->advertised_endpoint);
  coordinator->shard_count = config->shard_count;
  coordinator->lease_ttl_ms = config->lease_ttl_ms;
  coordinator->worst_case_db_latency_ms = config->worst_case_db_latency_ms;
  coordinator->safety_margin_ms = config->safety_margin_ms;
  memcpy(coordinator->boot_id, config->boot_id, sizeof(coordinator->boot_id));
  if (!coordinator->conninfo || !coordinator->schema_name || !coordinator->cluster_id ||
      !coordinator->listener_id || !coordinator->node_id || !coordinator->advertised_endpoint) {
    rc = TURBO_ENOMEM;
    goto fail;
  }
  coordinator->connection =
      flowie_cluster_pgsql_connect(coordinator->conninfo, coordinator->worst_case_db_latency_ms);
  if (!coordinator->connection || PQstatus(coordinator->connection) != CONNECTION_OK) {
    rc = TURBO_EIO;
    goto fail;
  }
  rc = flowie_cluster_pgsql_set_search_path(coordinator->connection);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_set_timeouts(coordinator->connection,
                                           coordinator->worst_case_db_latency_ms);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_schema_prepare(coordinator, config->create_schema,
                                             config->hash_version, config->shard_count);
  if (rc == TURBO_OK) rc = flowie_cluster_pgsql_sql_prepare(coordinator);
  if (rc != TURBO_OK) goto fail;
  *out = coordinator;
  return TURBO_OK;

fail:
  flowie_cluster_pgsql_coordinator_destroy(coordinator);
  return rc;
}

static int flowie_cluster_pgsql_hex_nibble(char value, uint8_t *out) {
  if (!out) return TURBO_EINVAL;
  if (value >= '0' && value <= '9')
    *out = (uint8_t)(value - '0');
  else if (value >= 'a' && value <= 'f')
    *out = (uint8_t)(value - 'a' + 10);
  else if (value >= 'A' && value <= 'F')
    *out = (uint8_t)(value - 'A' + 10);
  else
    return TURBO_EPROTO;
  return TURBO_OK;
}

static int flowie_cluster_pgsql_boot_id_parse(const char *text, size_t size,
                                               uint8_t out[FLOWIE_CLUSTER_BOOT_ID_SIZE]) {
  if (!text || !out || size != FLOWIE_CLUSTER_BOOT_ID_SIZE * 2u) return TURBO_EPROTO;
  for (size_t index = 0u; index < FLOWIE_CLUSTER_BOOT_ID_SIZE; ++index) {
    uint8_t high = 0u;
    uint8_t low = 0u;
    int rc = flowie_cluster_pgsql_hex_nibble(text[index * 2u], &high);
    if (rc == TURBO_OK) rc = flowie_cluster_pgsql_hex_nibble(text[index * 2u + 1u], &low);
    if (rc != TURBO_OK) return rc;
    out[index] = (uint8_t)((high << 4u) | low);
  }
  return flowie_cluster_pgsql_nonzero_boot_id(out) ? TURBO_OK : TURBO_EPROTO;
}

void flowie_cluster_pgsql_membership_snapshot_cleanup(
    flowie_cluster_pgsql_membership_snapshot_t *snapshot) {
  if (!snapshot) return;
  free(snapshot->members);
  *snapshot = (flowie_cluster_pgsql_membership_snapshot_t)
      FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
}

int flowie_cluster_pgsql_member_resolve(
    flowie_cluster_pgsql_coordinator_t *coordinator, tstr_v node_id,
    const uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE], flowie_cluster_pgsql_member_t *out) {
  static const Oid types[3] = {25u, 25u, 17u};
  int lengths[3] = {0, 0, FLOWIE_CLUSTER_BOOT_ID_SIZE};
  int formats[3] = {0, 0, 1};
  const char *values[3];
  tstr_t sql = NULL;
  PGresult *result = NULL;
  uint64_t parsed = 0u;
  size_t endpoint_size;
  int rc;
  if (!coordinator || !coordinator->connection || !node_id.data || node_id.len == 0u ||
      node_id.len > FLOWIE_CLUSTER_NODE_ID_MAX || !boot_id || !out ||
      out->size != sizeof(*out) || out->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1)
    return TURBO_EINVAL;
  values[0] = coordinator->cluster_id;
  values[1] = node_id.data;
  values[2] = (const char *)boot_id;
  sql = tstr_cat_fmt(
      NULL,
      "SELECT node_id,pg_catalog.encode(boot_id,'hex'),state::text,advertised_endpoint,"
      "floor(extract(epoch from lease_until)*1000)::bigint::text,revision::text FROM "
      "%s.cluster_node WHERE cluster_id=$1 AND node_id=$2 AND boot_id=$3",
      coordinator->schema_name);
  if (!sql) return TURBO_ENOMEM;
  result = PQexecParams(coordinator->connection, sql, 3, types, values, lengths, formats, 0);
  tstr_free(sql);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && PQnfields(result) != 6) rc = TURBO_EPROTO;
  if (rc == TURBO_OK && PQntuples(result) == 0) rc = TURBO_ENOENT;
  if (rc == TURBO_OK && PQntuples(result) != 1) rc = TURBO_EPROTO;
  if (rc == TURBO_OK) {
    *out = (flowie_cluster_pgsql_member_t)FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
    if (PQgetisnull(result, 0, 0) || PQgetisnull(result, 0, 1) || PQgetisnull(result, 0, 2) ||
        PQgetisnull(result, 0, 3) || PQgetisnull(result, 0, 4) || PQgetisnull(result, 0, 5))
      rc = TURBO_EPROTO;
    else {
      out->node_id_size = (size_t)PQgetlength(result, 0, 0);
      endpoint_size = (size_t)PQgetlength(result, 0, 3);
      if (out->node_id_size == 0u || out->node_id_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
          endpoint_size == 0u || endpoint_size > FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX)
        rc = TURBO_EPROTO;
      else {
        memcpy(out->node_id, PQgetvalue(result, 0, 0), out->node_id_size);
        out->node_id[out->node_id_size] = '\0';
        memcpy(out->advertised_endpoint, PQgetvalue(result, 0, 3), endpoint_size);
        out->advertised_endpoint[endpoint_size] = '\0';
        out->advertised_endpoint_size = endpoint_size;
        rc = flowie_cluster_pgsql_boot_id_parse(
            PQgetvalue(result, 0, 1), (size_t)PQgetlength(result, 0, 1), out->boot_id);
        if (rc == TURBO_OK)
          rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, 0, 2),
                                              (size_t)PQgetlength(result, 0, 2), &parsed);
        if (rc == TURBO_OK &&
            (parsed < FLOWIE_CLUSTER_NODE_STARTING || parsed > FLOWIE_CLUSTER_NODE_EXPIRED))
          rc = TURBO_EPROTO;
        if (rc == TURBO_OK) out->state = (flowie_cluster_node_state_t)parsed;
        if (rc == TURBO_OK)
          rc = flowie_cluster_pgsql_parse_u64(
              PQgetvalue(result, 0, 4), (size_t)PQgetlength(result, 0, 4),
              &out->lease_deadline_epoch_ms);
        if (rc == TURBO_OK)
          rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, 0, 5),
                                              (size_t)PQgetlength(result, 0, 5), &out->revision);
      }
    }
  }
  if (result) PQclear(result);
  return rc;
}

int flowie_cluster_pgsql_membership_expire(flowie_cluster_pgsql_coordinator_t *coordinator,
                                           uint64_t *out_membership_revision) {
  tstr_t sql = NULL;
  PGresult *result = NULL;
  const char *values[1];
  uint64_t revision = 0u;
  int rc;
  if (out_membership_revision) *out_membership_revision = 0u;
  if (!coordinator || !out_membership_revision || !coordinator->connection ||
      PQstatus(coordinator->connection) != CONNECTION_OK)
    return !coordinator || !out_membership_revision ? TURBO_EINVAL : TURBO_EIO;
  sql = tstr_new();
  if (!sql) return TURBO_ENOMEM;
  sql = tstr_cat_fmt(
      sql,
      "WITH locked AS MATERIALIZED (SELECT membership_revision FROM %s.cluster_config WHERE "
      "cluster_id=$1 AND schema_version=%u FOR UPDATE),advanced AS (UPDATE %s.cluster_config SET "
      "membership_revision=membership_revision+1 WHERE cluster_id=$1 AND EXISTS(SELECT 1 FROM "
      "%s.cluster_node n WHERE n.cluster_id=$1 AND n.state BETWEEN %u AND %u AND "
      "n.lease_until<=pg_catalog.clock_timestamp()) RETURNING membership_revision),expired AS "
      "(UPDATE %s.cluster_node SET state=%u,revision=(SELECT membership_revision FROM advanced) "
      "WHERE cluster_id=$1 AND state BETWEEN %u AND %u AND "
      "lease_until<=pg_catalog.clock_timestamp() AND EXISTS(SELECT 1 FROM advanced) RETURNING 1) "
      "SELECT COALESCE((SELECT membership_revision FROM advanced),(SELECT membership_revision "
      "FROM locked))::text,(SELECT COUNT(*) FROM expired)::text",
      coordinator->schema_name, FLOWIE_CLUSTER_PGSQL_SCHEMA_VERSION, coordinator->schema_name,
      coordinator->schema_name, FLOWIE_CLUSTER_NODE_STARTING, FLOWIE_CLUSTER_NODE_DRAINING,
      coordinator->schema_name, FLOWIE_CLUSTER_NODE_EXPIRED, FLOWIE_CLUSTER_NODE_STARTING,
      FLOWIE_CLUSTER_NODE_DRAINING);
  if (!sql) return TURBO_ENOMEM;
  values[0] = coordinator->cluster_id;
  result = PQexecParams(coordinator->connection, sql, 1, NULL, values, NULL, NULL, 0);
  tstr_free(sql);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK &&
      (PQntuples(result) != 1 || PQnfields(result) != 2 || PQgetisnull(result, 0, 0)))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, 0, 0),
                                        (size_t)PQgetlength(result, 0, 0), &revision);
  if (result) PQclear(result);
  if (rc == TURBO_OK) *out_membership_revision = revision;
  return rc;
}

int flowie_cluster_pgsql_membership_snapshot(
    flowie_cluster_pgsql_coordinator_t *coordinator, size_t max_nodes,
    flowie_cluster_pgsql_membership_snapshot_t *out) {
  static const Oid types[2] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  flowie_cluster_pgsql_membership_snapshot_t snapshot =
      FLOWIE_CLUSTER_PGSQL_MEMBERSHIP_SNAPSHOT_INIT;
  tstr_t sql = NULL;
  tstr_t next = NULL;
  PGresult *result = NULL;
  const char *values[2];
  char limit_text[32];
  uint64_t parsed = 0u;
  int rows;
  int rc;
  if (!coordinator || !out || out->size < sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1 || max_nodes == 0u ||
      max_nodes > FLOWIE_CLUSTER_NODE_COUNT_MAX)
    return TURBO_EINVAL;
  if (!coordinator->connection || PQstatus(coordinator->connection) != CONNECTION_OK)
    return TURBO_EIO;
  *out = snapshot;
  if (snprintf(limit_text, sizeof(limit_text), "%zu", max_nodes + 1u) <= 0)
    return TURBO_ERANGE;
  rc = flowie_cluster_pgsql_exec(coordinator->connection,
                                 "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY",
                                 PGRES_COMMAND_OK);
  if (rc != TURBO_OK) return rc;
  sql = tstr_new();
  if (!sql) return flowie_cluster_pgsql_rollback(coordinator, TURBO_ENOMEM);
  next = tstr_cat_fmt(sql,
                      "SELECT membership_revision::text FROM %s.cluster_config WHERE "
                      "cluster_id=$1 AND schema_version=%u",
                      coordinator->schema_name, FLOWIE_CLUSTER_PGSQL_SCHEMA_VERSION);
  if (!next) {
    tstr_free(sql);
    return flowie_cluster_pgsql_rollback(coordinator, TURBO_ENOMEM);
  }
  sql = next;
  values[0] = coordinator->cluster_id;
  result = PQexecParams(coordinator->connection, sql, 1, types, values, NULL, NULL, 0);
  tstr_free(sql);
  sql = NULL;
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK &&
      (PQntuples(result) != 1 || PQnfields(result) != 1 || PQgetisnull(result, 0, 0)))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, 0, 0),
                                        (size_t)PQgetlength(result, 0, 0),
                                        &snapshot.membership_revision);
  if (result) PQclear(result);
  result = NULL;
  if (rc != TURBO_OK) return flowie_cluster_pgsql_rollback(coordinator, rc);
  sql = tstr_new();
  if (!sql) return flowie_cluster_pgsql_rollback(coordinator, TURBO_ENOMEM);
  next = tstr_cat_fmt(
      sql,
      "SELECT node_id,pg_catalog.encode(boot_id,'hex'),state::text,advertised_endpoint,"
      "floor(extract(epoch from lease_until)*1000)::bigint::text,revision::text FROM "
      "%s.cluster_node WHERE cluster_id=$1 ORDER BY node_id COLLATE \"C\" "
      "LIMIT $2::integer",
      coordinator->schema_name);
  if (!next) {
    tstr_free(sql);
    return flowie_cluster_pgsql_rollback(coordinator, TURBO_ENOMEM);
  }
  sql = next;
  values[1] = limit_text;
  result = PQexecParams(coordinator->connection, sql, 2, types, values, NULL, NULL, 0);
  tstr_free(sql);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  rows = rc == TURBO_OK ? PQntuples(result) : 0;
  if (rc == TURBO_OK && (PQnfields(result) != 6 || rows < 0)) rc = TURBO_EPROTO;
  if (rc == TURBO_OK && (size_t)rows > max_nodes) rc = TURBO_ENOSPC;
  if (rc == TURBO_OK && rows > 0) {
    snapshot.members =
        (flowie_cluster_pgsql_member_t *)calloc((size_t)rows, sizeof(*snapshot.members));
    if (!snapshot.members) rc = TURBO_ENOMEM;
  }
  for (int row = 0; rc == TURBO_OK && row < rows; ++row) {
    flowie_cluster_pgsql_member_t *member = &snapshot.members[row];
    size_t node_size;
    size_t endpoint_size;
    *member = (flowie_cluster_pgsql_member_t)FLOWIE_CLUSTER_PGSQL_MEMBER_INIT;
    if (PQgetisnull(result, row, 0) || PQgetisnull(result, row, 1) ||
        PQgetisnull(result, row, 2) || PQgetisnull(result, row, 3) ||
        PQgetisnull(result, row, 4) || PQgetisnull(result, row, 5)) {
      rc = TURBO_EPROTO;
      break;
    }
    node_size = (size_t)PQgetlength(result, row, 0);
    endpoint_size = (size_t)PQgetlength(result, row, 3);
    if (node_size == 0u || node_size > FLOWIE_CLUSTER_NODE_ID_MAX || endpoint_size == 0u ||
        endpoint_size > FLOWIE_CLUSTER_ADVERTISED_ENDPOINT_MAX ||
        (row > 0 && strcmp(snapshot.members[row - 1].node_id, PQgetvalue(result, row, 0)) >= 0)) {
      rc = TURBO_EPROTO;
      break;
    }
    memcpy(member->node_id, PQgetvalue(result, row, 0), node_size);
    member->node_id[node_size] = '\0';
    member->node_id_size = node_size;
    memcpy(member->advertised_endpoint, PQgetvalue(result, row, 3), endpoint_size);
    member->advertised_endpoint[endpoint_size] = '\0';
    member->advertised_endpoint_size = endpoint_size;
    rc = flowie_cluster_pgsql_boot_id_parse(PQgetvalue(result, row, 1),
                                             (size_t)PQgetlength(result, row, 1), member->boot_id);
    if (rc == TURBO_OK)
      rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, row, 2),
                                          (size_t)PQgetlength(result, row, 2), &parsed);
    if (rc == TURBO_OK &&
        (parsed < FLOWIE_CLUSTER_NODE_STARTING || parsed > FLOWIE_CLUSTER_NODE_EXPIRED))
      rc = TURBO_EPROTO;
    if (rc == TURBO_OK) member->state = (flowie_cluster_node_state_t)parsed;
    if (rc == TURBO_OK)
      rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, row, 4),
                                          (size_t)PQgetlength(result, row, 4),
                                          &member->lease_deadline_epoch_ms);
    if (rc == TURBO_OK)
      rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, row, 5),
                                          (size_t)PQgetlength(result, row, 5), &member->revision);
    if (rc == TURBO_OK &&
        (member->revision == 0u || member->revision > snapshot.membership_revision))
      rc = TURBO_EPROTO;
  }
  if (result) PQclear(result);
  if (rc != TURBO_OK) {
    flowie_cluster_pgsql_membership_snapshot_cleanup(&snapshot);
    return flowie_cluster_pgsql_rollback(coordinator, rc);
  }
  snapshot.member_count = (size_t)rows;
  rc = flowie_cluster_pgsql_exec(coordinator->connection, "COMMIT", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) {
    flowie_cluster_pgsql_membership_snapshot_cleanup(&snapshot);
    return rc;
  }
  *out = snapshot;
  return TURBO_OK;
}

void flowie_cluster_pgsql_shard_owner_snapshot_cleanup(
    flowie_cluster_pgsql_shard_owner_snapshot_t *snapshot) {
  if (!snapshot) return;
  free(snapshot->owners);
  *snapshot = (flowie_cluster_pgsql_shard_owner_snapshot_t)
      FLOWIE_CLUSTER_PGSQL_SHARD_OWNER_SNAPSHOT_INIT;
}

int flowie_cluster_pgsql_shard_owner_snapshot(
    flowie_cluster_pgsql_coordinator_t *coordinator,
    flowie_cluster_pgsql_shard_owner_snapshot_t *out) {
  static const Oid types[3] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  flowie_cluster_pgsql_shard_owner_snapshot_t snapshot =
      FLOWIE_CLUSTER_PGSQL_SHARD_OWNER_SNAPSHOT_INIT;
  tstr_t sql = NULL;
  tstr_t next;
  PGresult *result = NULL;
  const char *values[3];
  char shard_count_text[32];
  uint64_t parsed_shard = 0u;
  uint64_t owner_epoch = 0u;
  uint64_t validity_ms = 0u;
  uint64_t request_start_ns;
  int rows;
  int rc;
  if (!coordinator || !out || out->size < sizeof(*out) ||
      out->abi_version != FLOWIE_CLUSTER_PGSQL_ABI_V1)
    return TURBO_EINVAL;
  if (!coordinator->connection || PQstatus(coordinator->connection) != CONNECTION_OK)
    return TURBO_EIO;
  *out = snapshot;
  if (snprintf(shard_count_text, sizeof(shard_count_text), "%u", coordinator->shard_count) <= 0)
    return TURBO_ERANGE;
  request_start_ns = turbo_hrtime();
  if (request_start_ns == 0u) return TURBO_EIO;
  sql = tstr_new();
  if (!sql) return TURBO_ENOMEM;
  next = tstr_cat_fmt(
      sql,
      "SELECT g.shard_id::text,s.owner_node_id,pg_catalog.encode(s.owner_boot_id,'hex'),"
      "s.owner_epoch::text,CASE WHEN s.lease_until>pg_catalog.clock_timestamp() AND "
      "n.lease_until>pg_catalog.clock_timestamp() AND n.state IN (%u,%u) THEN "
      "GREATEST(1,FLOOR(EXTRACT(EPOCH FROM (LEAST(s.lease_until,n.lease_until)-"
      "pg_catalog.clock_timestamp()))*1000)::bigint)::text END FROM "
      "pg_catalog.generate_series(0,$3::integer-1) AS g(shard_id) LEFT JOIN "
      "%s.cluster_shard s ON s.cluster_id=$1 AND s.listener_id=$2 AND "
      "s.shard_id=g.shard_id LEFT JOIN %s.cluster_node n ON n.cluster_id=s.cluster_id AND "
      "n.node_id=s.owner_node_id AND n.boot_id=s.owner_boot_id ORDER BY g.shard_id",
      FLOWIE_CLUSTER_NODE_READY, FLOWIE_CLUSTER_NODE_DRAINING, coordinator->schema_name,
      coordinator->schema_name);
  if (!next) {
    tstr_free(sql);
    return TURBO_ENOMEM;
  }
  sql = next;
  values[0] = coordinator->cluster_id;
  values[1] = coordinator->listener_id;
  values[2] = shard_count_text;
  result = PQexecParams(coordinator->connection, sql, 3, types, values, NULL, NULL, 0);
  tstr_free(sql);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  rows = rc == TURBO_OK ? PQntuples(result) : 0;
  if (rc == TURBO_OK &&
      (PQnfields(result) != 5 || rows < 0 || (uint32_t)rows != coordinator->shard_count))
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK) {
    snapshot.owners =
        (flowie_cluster_pgsql_shard_owner_t *)calloc((size_t)rows, sizeof(*snapshot.owners));
    if (!snapshot.owners) rc = TURBO_ENOMEM;
  }
  for (int row = 0; rc == TURBO_OK && row < rows; ++row) {
    flowie_cluster_pgsql_shard_owner_t *owner = &snapshot.owners[row];
    uint8_t boot_id[FLOWIE_CLUSTER_BOOT_ID_SIZE] = {0};
    int assigned;
    *owner = (flowie_cluster_pgsql_shard_owner_t)FLOWIE_CLUSTER_PGSQL_SHARD_OWNER_INIT;
    if (PQgetisnull(result, row, 0)) {
      rc = TURBO_EPROTO;
      break;
    }
    rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, row, 0),
                                         (size_t)PQgetlength(result, row, 0), &parsed_shard);
    if (rc != TURBO_OK || parsed_shard != (uint64_t)row) {
      rc = TURBO_EPROTO;
      break;
    }
    owner->shard_id = (uint32_t)row;
    owner->owner.shard_id = (uint32_t)row;
    assigned = !PQgetisnull(result, row, 4);
    if (!assigned) continue;
    if (PQgetisnull(result, row, 1) || PQgetisnull(result, row, 2) ||
        PQgetisnull(result, row, 3)) {
      rc = TURBO_EPROTO;
      break;
    }
    rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, row, 3),
                                         (size_t)PQgetlength(result, row, 3), &owner_epoch);
    if (rc == TURBO_OK)
      rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, row, 4),
                                           (size_t)PQgetlength(result, row, 4), &validity_ms);
    if (rc == TURBO_OK && validity_ms <= coordinator->safety_margin_ms) {
      owner->owner = (flowie_cluster_owner_token_t)FLOWIE_CLUSTER_OWNER_TOKEN_INIT;
      owner->owner.shard_id = (uint32_t)row;
      continue;
    }
    if (rc == TURBO_OK)
      rc = flowie_cluster_pgsql_boot_id_parse(PQgetvalue(result, row, 2),
                                               (size_t)PQgetlength(result, row, 2),
                                               boot_id);
    if (rc == TURBO_OK) {
      size_t node_id_size = (size_t)PQgetlength(result, row, 1);
      if (owner_epoch == 0u || node_id_size == 0u || node_id_size > FLOWIE_CLUSTER_NODE_ID_MAX)
        rc = TURBO_EPROTO;
      else
        rc = flowie_cluster_owner_token_init(
            &owner->owner, (uint32_t)row, owner_epoch, PQgetvalue(result, row, 1), node_id_size,
            boot_id);
    }
    if (rc == TURBO_OK)
      rc = flowie_cluster_lease_deadline_ns(request_start_ns, validity_ms,
                                             coordinator->safety_margin_ms,
                                             &owner->local_deadline_ns);
  }
  if (result) PQclear(result);
  if (rc != TURBO_OK) {
    flowie_cluster_pgsql_shard_owner_snapshot_cleanup(&snapshot);
    return rc;
  }
  snapshot.owner_count = (size_t)rows;
  *out = snapshot;
  return TURBO_OK;
}

static int flowie_cluster_pgsql_parameters(flowie_cluster_pgsql_coordinator_t *coordinator,
                                           const flowie_cluster_owner_token_t *token,
                                           uint32_t shard_id, const char *values[7], int lengths[7],
                                           int formats[7], char shard_text[16], char epoch_text[32],
                                           char ttl_text[32]) {
  if (!coordinator || shard_id >= coordinator->shard_count) return TURBO_EINVAL;
  if (snprintf(shard_text, 16u, "%u", shard_id) <= 0 ||
      snprintf(ttl_text, 32u, "%llu", (unsigned long long)coordinator->lease_ttl_ms) <= 0)
    return TURBO_ERANGE;
  if (token && snprintf(epoch_text, 32u, "%llu", (unsigned long long)token->owner_epoch) <= 0)
    return TURBO_ERANGE;
  values[0] = coordinator->cluster_id;
  values[1] = coordinator->listener_id;
  values[2] = shard_text;
  values[3] = coordinator->node_id;
  values[4] = (const char *)coordinator->boot_id;
  values[5] = token ? epoch_text : "0";
  values[6] = ttl_text;
  for (size_t index = 0u; index < 7u; ++index) {
    lengths[index] = 0;
    formats[index] = 0;
  }
  lengths[4] = FLOWIE_CLUSTER_BOOT_ID_SIZE;
  formats[4] = 1;
  return TURBO_OK;
}

static int flowie_cluster_pgsql_grant_parse(flowie_cluster_pgsql_coordinator_t *coordinator,
                                            PGresult *result, uint32_t shard_id,
                                            uint64_t request_start_ns,
                                            flowie_cluster_owner_token_t *out_token,
                                            uint64_t *out_deadline_ns) {
  uint64_t owner_epoch = 0u;
  uint64_t validity_ms = 0u;
  int rc;
  if (PQntuples(result) != 1 || PQnfields(result) != 2 || PQgetisnull(result, 0, 0) ||
      PQgetisnull(result, 0, 1))
    return PQntuples(result) == 0 ? TURBO_EBUSY : TURBO_EPROTO;
  rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, 0, 0), (size_t)PQgetlength(result, 0, 0),
                                      &owner_epoch);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, 0, 1), (size_t)PQgetlength(result, 0, 1),
                                        &validity_ms);
  if (rc == TURBO_OK)
    rc = flowie_cluster_owner_token_init(out_token, shard_id, owner_epoch, coordinator->node_id,
                                         tstr_len(coordinator->node_id), coordinator->boot_id);
  if (rc == TURBO_OK)
    rc = flowie_cluster_lease_deadline_ns(request_start_ns, validity_ms,
                                          coordinator->safety_margin_ms, out_deadline_ns);
  return rc;
}

int flowie_cluster_pgsql_shard_claim(flowie_cluster_pgsql_coordinator_t *coordinator,
                                     uint32_t shard_id, uint64_t request_start_ns,
                                     flowie_cluster_owner_token_t *out_token,
                                     uint64_t *out_deadline_ns) {
  static const Oid types[8] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  const char *values[7];
  const char *node_values[8];
  int lengths[7];
  int node_lengths[8];
  int formats[7];
  int node_formats[8];
  char shard_text[16];
  char epoch_text[32];
  char ttl_text[32];
  PGresult *result = NULL;
  int rc;
  if (out_token) memset(out_token, 0, sizeof(*out_token));
  if (out_deadline_ns) *out_deadline_ns = 0u;
  if (!coordinator || !out_token || !out_deadline_ns || request_start_ns == 0u) return TURBO_EINVAL;
  if (!coordinator->connection || PQstatus(coordinator->connection) != CONNECTION_OK)
    return TURBO_EIO;
  rc = flowie_cluster_pgsql_parameters(coordinator, NULL, shard_id, values, lengths, formats,
                                       shard_text, epoch_text, ttl_text);
  if (rc != TURBO_OK) return rc;
  for (size_t index = 0u; index < 7u; ++index) {
    node_values[index] = values[index];
    node_lengths[index] = lengths[index];
    node_formats[index] = formats[index];
  }
  node_values[7] = coordinator->advertised_endpoint;
  node_lengths[7] = 0;
  node_formats[7] = 0;
  rc = flowie_cluster_pgsql_exec(coordinator->connection, "BEGIN", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) return rc;
  result = PQexecParams(coordinator->connection, coordinator->node_claim_sql, 8, types, node_values,
                        node_lengths, node_formats, 0);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_COMMAND_OK);
  if (result) PQclear(result);
  result = NULL;
  if (rc != TURBO_OK) return flowie_cluster_pgsql_rollback(coordinator, rc);
  result = PQexecParams(coordinator->connection, coordinator->shard_ensure_sql, 3, types, values,
                        lengths, formats, 0);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_COMMAND_OK);
  if (result) PQclear(result);
  result = NULL;
  if (rc != TURBO_OK) return flowie_cluster_pgsql_rollback(coordinator, rc);
  result = PQexecParams(coordinator->connection, coordinator->claim_sql, 7, types, values, lengths,
                        formats, 0);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK)
    rc = flowie_cluster_pgsql_grant_parse(coordinator, result, shard_id, request_start_ns,
                                          out_token, out_deadline_ns);
  if (result) PQclear(result);
  if (rc != TURBO_OK) return flowie_cluster_pgsql_rollback(coordinator, rc);
  rc = flowie_cluster_pgsql_exec(coordinator->connection, "COMMIT", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) {
    memset(out_token, 0, sizeof(*out_token));
    *out_deadline_ns = 0u;
  }
  return rc;
}

static int flowie_cluster_pgsql_token_parameters(flowie_cluster_pgsql_coordinator_t *coordinator,
                                                 const flowie_cluster_owner_token_t *token,
                                                 const char *values[7], int lengths[7],
                                                 int formats[7], char shard_text[16],
                                                 char epoch_text[32], char ttl_text[32]) {
  if (!coordinator || !token) return TURBO_EINVAL;
  if (!coordinator->connection || PQstatus(coordinator->connection) != CONNECTION_OK)
    return TURBO_EIO;
  if (token->size < sizeof(*token) || token->abi_version != FLOWIE_CLUSTER_INTERNAL_ABI_V1 ||
      token->owner_epoch == 0u || token->node_id_size == 0u ||
      token->node_id_size > FLOWIE_CLUSTER_NODE_ID_MAX ||
      !flowie_cluster_pgsql_nonzero_boot_id(token->boot_id))
    return TURBO_EINVAL;
  if (token->shard_id >= coordinator->shard_count ||
      token->node_id_size != tstr_len(coordinator->node_id) ||
      memcmp(token->node_id, coordinator->node_id, token->node_id_size) != 0 ||
      memcmp(token->boot_id, coordinator->boot_id, sizeof(token->boot_id)) != 0)
    return TURBO_EBUSY;
  return flowie_cluster_pgsql_parameters(coordinator, token, token->shard_id, values, lengths,
                                         formats, shard_text, epoch_text, ttl_text);
}

static int flowie_cluster_pgsql_validity_parse(flowie_cluster_pgsql_coordinator_t *coordinator,
                                               PGresult *result, uint64_t request_start_ns,
                                               uint64_t *out_deadline_ns) {
  uint64_t validity_ms = 0u;
  int rc;
  if (PQntuples(result) != 1 || PQnfields(result) != 1 || PQgetisnull(result, 0, 0))
    return PQntuples(result) == 0 ? TURBO_EBUSY : TURBO_EPROTO;
  rc = flowie_cluster_pgsql_parse_u64(PQgetvalue(result, 0, 0), (size_t)PQgetlength(result, 0, 0),
                                      &validity_ms);
  if (rc == TURBO_OK)
    rc = flowie_cluster_lease_deadline_ns(request_start_ns, validity_ms,
                                          coordinator->safety_margin_ms, out_deadline_ns);
  return rc;
}

int flowie_cluster_pgsql_shard_renew(flowie_cluster_pgsql_coordinator_t *coordinator,
                                     const flowie_cluster_owner_token_t *token,
                                     uint64_t request_start_ns, uint64_t *out_deadline_ns) {
  static const Oid types[7] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  const char *values[7];
  int lengths[7];
  int formats[7];
  char shard_text[16];
  char epoch_text[32];
  char ttl_text[32];
  PGresult *result = NULL;
  int rc;
  if (out_deadline_ns) *out_deadline_ns = 0u;
  if (!out_deadline_ns || request_start_ns == 0u) return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_token_parameters(coordinator, token, values, lengths, formats,
                                             shard_text, epoch_text, ttl_text);
  if (rc != TURBO_OK) return rc;
  rc = flowie_cluster_pgsql_exec(coordinator->connection, "BEGIN", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) return rc;
  result = PQexecParams(coordinator->connection, coordinator->node_renew_sql, 7, types, values,
                        lengths, formats, 0);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && PQntuples(result) != 1)
    rc = PQntuples(result) == 0 ? TURBO_EBUSY : TURBO_EPROTO;
  if (result) PQclear(result);
  result = NULL;
  if (rc != TURBO_OK) return flowie_cluster_pgsql_rollback(coordinator, rc);
  result = PQexecParams(coordinator->connection, coordinator->renew_sql, 7, types, values, lengths,
                        formats, 0);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK)
    rc =
        flowie_cluster_pgsql_validity_parse(coordinator, result, request_start_ns, out_deadline_ns);
  if (result) PQclear(result);
  if (rc != TURBO_OK) return flowie_cluster_pgsql_rollback(coordinator, rc);
  rc = flowie_cluster_pgsql_exec(coordinator->connection, "COMMIT", PGRES_COMMAND_OK);
  if (rc != TURBO_OK) *out_deadline_ns = 0u;
  return rc;
}

int flowie_cluster_pgsql_shard_require(flowie_cluster_pgsql_coordinator_t *coordinator,
                                       const flowie_cluster_owner_token_t *token,
                                       uint64_t request_start_ns, uint64_t *out_deadline_ns) {
  static const Oid types[7] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  const char *values[7];
  int lengths[7];
  int formats[7];
  char shard_text[16];
  char epoch_text[32];
  char ttl_text[32];
  PGresult *result;
  int rc;
  if (out_deadline_ns) *out_deadline_ns = 0u;
  if (!out_deadline_ns || request_start_ns == 0u) return TURBO_EINVAL;
  rc = flowie_cluster_pgsql_token_parameters(coordinator, token, values, lengths, formats,
                                             shard_text, epoch_text, ttl_text);
  if (rc != TURBO_OK) return rc;
  result = PQexecParams(coordinator->connection, coordinator->require_sql, 6, types, values,
                        lengths, formats, 0);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK)
    rc =
        flowie_cluster_pgsql_validity_parse(coordinator, result, request_start_ns, out_deadline_ns);
  if (result) PQclear(result);
  return rc;
}

int flowie_cluster_pgsql_shard_release(flowie_cluster_pgsql_coordinator_t *coordinator,
                                       const flowie_cluster_owner_token_t *token) {
  static const Oid types[7] = {FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT,  FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_BYTEA, FLOWIE_CLUSTER_PGSQL_OID_TEXT,
                               FLOWIE_CLUSTER_PGSQL_OID_TEXT};
  const char *values[7];
  int lengths[7];
  int formats[7];
  char shard_text[16];
  char epoch_text[32];
  char ttl_text[32];
  PGresult *result;
  int rc = flowie_cluster_pgsql_token_parameters(coordinator, token, values, lengths, formats,
                                                 shard_text, epoch_text, ttl_text);
  if (rc != TURBO_OK) return rc;
  result = PQexecParams(coordinator->connection, coordinator->release_sql, 6, types, values,
                        lengths, formats, 0);
  rc = flowie_cluster_pgsql_result_status(result, PGRES_TUPLES_OK);
  if (rc == TURBO_OK && PQntuples(result) != 1)
    rc = PQntuples(result) == 0 ? TURBO_EBUSY : TURBO_EPROTO;
  if (result) PQclear(result);
  return rc;
}
