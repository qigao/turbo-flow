#include "turbo_flow_security_sqlite.h"

#include "cmeta_error.h"
#include "tstr.h"
#include "turbo_flow_stl_error_internal.h"

#include <sqlite3.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

static const char FLOW_SECURITY_SQLITE_SCHEMA[] =
    "PRAGMA journal_mode=WAL;PRAGMA synchronous=FULL;"
    "CREATE TABLE IF NOT EXISTS turbo_flow_acl_bundle_v3("
    "namespace_name TEXT PRIMARY KEY,policy_version INTEGER NOT NULL CHECK(policy_version>0),"
    "expires_at INTEGER NOT NULL CHECK(expires_at>=0));"
    "CREATE TABLE IF NOT EXISTS turbo_flow_acl_rule_v3("
    "namespace_name TEXT NOT NULL,ordinal INTEGER NOT NULL CHECK(ordinal>=0),"
    "rule_line TEXT NOT NULL CHECK(length(rule_line)>0),"
    "PRIMARY KEY(namespace_name,ordinal),"
    "FOREIGN KEY(namespace_name) REFERENCES turbo_flow_acl_bundle_v3(namespace_name)"
    " ON DELETE CASCADE) WITHOUT ROWID;";

typedef struct flow_security_sqlite_loaded_s {
  vec_t rules;
} flow_security_sqlite_loaded_t;

struct turbo_flow_security_sqlite_provider_s {
  tstr database_path;
  tstr namespace_name;
  int busy_timeout_ms;
  size_t max_rules;
  turbo_flow_security_policy_provider_t interface;
};

static int flow_security_sqlite_status(int status) {
  if (status == SQLITE_BUSY || status == SQLITE_LOCKED) return SALTS_EBUSY;
  if (status == SQLITE_NOMEM) return SALTS_ENOMEM;
  if (status == SQLITE_CONSTRAINT || status == SQLITE_MISMATCH || status == SQLITE_RANGE)
    return SALTS_EINVAL;
  return SALTS_EIO;
}

static int flow_security_sqlite_open(const turbo_flow_security_sqlite_provider_t *provider,
                                     sqlite3 **out) {
  sqlite3 *database = NULL;
  int status;
  if (out) *out = NULL;
  if (!provider || !out) return SALTS_EINVAL;
  status =
      sqlite3_open_v2(provider->database_path, &database,
                      SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_FULLMUTEX, NULL);
  if (status != SQLITE_OK ||
      (provider->busy_timeout_ms > 0 &&
       sqlite3_busy_timeout(database, provider->busy_timeout_ms) != SQLITE_OK)) {
    if (database) (void)sqlite3_close(database);
    return flow_security_sqlite_status(status);
  }
  *out = database;
  return SALTS_OK;
}

static int flow_security_sqlite_cstr_valid(const char *value, size_t capacity, int required) {
  const char *end;
  if (!value || capacity == 0u) return 0;
  end = (const char *)memchr(value, '\0', capacity);
  return end && (!required || end != value);
}

static int flow_security_sqlite_rule_valid(const turbo_flow_security_rule_t *rule) {
  int allow_empty_pattern;
  if (!rule) return 0;
  allow_empty_pattern = rule->action_mask == TURBO_FLOW_SECURITY_ACTION_CONNECT &&
                        rule->resource_type == TURBO_FLOW_SECURITY_RESOURCE_GENERIC &&
                        rule->match_kind == TURBO_FLOW_SECURITY_MATCH_PREFIX;
  if (rule->size < sizeof(*rule) || rule->abi_version != TURBO_FLOW_SECURITY_ABI_V3 ||
      rule->effect < TURBO_FLOW_SECURITY_DENY || rule->effect > TURBO_FLOW_SECURITY_ALLOW ||
      rule->subject_kind < TURBO_FLOW_SECURITY_SUBJECT_ANY ||
      rule->subject_kind > TURBO_FLOW_SECURITY_SUBJECT_GROUP ||
      !flow_security_sqlite_cstr_valid(rule->subject, sizeof(rule->subject), 0) ||
      !flow_security_sqlite_cstr_valid(rule->domain_id, sizeof(rule->domain_id), 1) ||
      rule->action_mask == 0u || (rule->action_mask & ~TURBO_FLOW_SECURITY_ACTION_ALL) != 0u ||
      rule->resource_type < TURBO_FLOW_SECURITY_RESOURCE_GENERIC ||
      rule->resource_type > TURBO_FLOW_SECURITY_RESOURCE_SECRET ||
      rule->match_kind < TURBO_FLOW_SECURITY_MATCH_EXACT ||
      rule->match_kind > TURBO_FLOW_SECURITY_MATCH_ADAPTER ||
      !flow_security_sqlite_cstr_valid(rule->pattern, sizeof(rule->pattern),
                                       !allow_empty_pattern))
    return 0;
  return rule->subject_kind == TURBO_FLOW_SECURITY_SUBJECT_ANY ? rule->subject[0] == '\0'
                                                               : rule->subject[0] != '\0';
}

static void flow_security_sqlite_loaded_destroy(flow_security_sqlite_loaded_t *loaded) {
  if (!loaded) return;
  vec_destroy(&loaded->rules);
  free(loaded);
}

static int flow_security_sqlite_load(void *ctx, uint64_t required_version,
                                     turbo_flow_security_policy_bundle_t *bundle_out) {
  turbo_flow_security_sqlite_provider_t *provider = (turbo_flow_security_sqlite_provider_t *)ctx;
  flow_security_sqlite_loaded_t *loaded = NULL;
  sqlite3 *database = NULL;
  sqlite3_stmt *metadata = NULL;
  sqlite3_stmt *rules = NULL;
  uint64_t policy_version;
  uint64_t expires_at;
  size_t expected_ordinal = 0u;
  int status;
  int rc;
  if (!provider || !bundle_out || bundle_out->size < sizeof(*bundle_out)) return SALTS_EINVAL;
  *bundle_out = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
  rc = flow_security_sqlite_open(provider, &database);
  if (rc != SALTS_OK) return rc;
  status = sqlite3_exec(database, "BEGIN", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flow_security_sqlite_status(status);
    goto done;
  }
  status = sqlite3_prepare_v2(
      database,
      "SELECT policy_version,expires_at FROM turbo_flow_acl_bundle_v3 WHERE namespace_name=?1", -1,
      &metadata, NULL);
  if (status != SQLITE_OK ||
      sqlite3_bind_text(metadata, 1, provider->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK) {
    rc = flow_security_sqlite_status(status);
    goto rollback;
  }
  status = sqlite3_step(metadata);
  if (status == SQLITE_DONE) {
    rc = SALTS_ENOENT;
    goto rollback;
  }
  if (status != SQLITE_ROW || sqlite3_column_type(metadata, 0) != SQLITE_INTEGER ||
      sqlite3_column_type(metadata, 1) != SQLITE_INTEGER ||
      sqlite3_column_int64(metadata, 0) <= 0 || sqlite3_column_int64(metadata, 1) < 0) {
    rc = status == SQLITE_ROW ? SALTS_EPROTO : flow_security_sqlite_status(status);
    goto rollback;
  }
  policy_version = (uint64_t)sqlite3_column_int64(metadata, 0);
  expires_at = (uint64_t)sqlite3_column_int64(metadata, 1);
  if (required_version != 0u && required_version != policy_version) {
    rc = SALTS_ENOENT;
    goto rollback;
  }
  (void)sqlite3_finalize(metadata);
  metadata = NULL;
  loaded = (flow_security_sqlite_loaded_t *)calloc(1u, sizeof(*loaded));
  if (!loaded) {
    rc = SALTS_ENOMEM;
    goto rollback;
  }
  rc = turbo_flow_stl_error(vec_init_bytes(&loaded->rules, sizeof(turbo_flow_security_rule_t), _Alignof(turbo_flow_max_align_t), SIZE_MAX));
  if (rc != SALTS_OK) goto rollback;
  rc = turbo_flow_stl_error(vec_reserve(&loaded->rules, provider->max_rules));
  if (rc != SALTS_OK) goto rollback;
  status = sqlite3_prepare_v2(
      database,
      "SELECT ordinal,rule_line FROM turbo_flow_acl_rule_v3 "
      "WHERE namespace_name=?1 ORDER BY ordinal",
      -1, &rules, NULL);
  if (status != SQLITE_OK ||
      sqlite3_bind_text(rules, 1, provider->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK) {
    rc = flow_security_sqlite_status(status);
    goto rollback;
  }
  while ((status = sqlite3_step(rules)) == SQLITE_ROW) {
    turbo_flow_security_rule_t rule = TURBO_FLOW_SECURITY_RULE_INIT;
    const unsigned char *rule_line;
    int rule_line_size;
    sqlite3_int64 ordinal;
    if (expected_ordinal >= provider->max_rules ||
        sqlite3_column_type(rules, 0) != SQLITE_INTEGER ||
        sqlite3_column_type(rules, 1) != SQLITE_TEXT) {
      rc = expected_ordinal >= provider->max_rules ? SALTS_ENOSPC : SALTS_EPROTO;
      goto rollback;
    }
    ordinal = sqlite3_column_int64(rules, 0);
    if (ordinal < 0 || (uint64_t)ordinal != (uint64_t)expected_ordinal) {
      rc = SALTS_EPROTO;
      goto rollback;
    }
    rule_line = sqlite3_column_text(rules, 1);
    rule_line_size = sqlite3_column_bytes(rules, 1);
    if (!rule_line || rule_line_size <= 0 ||
        (size_t)rule_line_size > TURBO_FLOW_SECURITY_RULE_LINE_MAX ||
        memchr(rule_line, '\0', (size_t)rule_line_size)) {
      rc = SALTS_EPROTO;
      goto rollback;
    }
    rc = turbo_flow_security_rule_parse_line((const char *)rule_line,
                                             (size_t)rule_line_size, &rule);
    if (rc != SALTS_OK || !flow_security_sqlite_rule_valid(&rule)) {
      rc = SALTS_EPROTO;
      goto rollback;
    }
    rc = turbo_flow_stl_error(vec_push(&loaded->rules, &rule));
    if (rc != SALTS_OK) goto rollback;
    ++expected_ordinal;
  }
  if (status != SQLITE_DONE || expected_ordinal == 0u) {
    rc = status == SQLITE_DONE ? SALTS_EPROTO : flow_security_sqlite_status(status);
    goto rollback;
  }
  (void)sqlite3_finalize(rules);
  rules = NULL;
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flow_security_sqlite_status(status);
    goto rollback;
  }
  bundle_out->policy_version = policy_version;
  bundle_out->expires_at = expires_at;
  bundle_out->rules = (const turbo_flow_security_rule_t *)loaded->rules.data;
  bundle_out->rule_count = vec_size(&loaded->rules);
  bundle_out->provider_bundle = loaded;
  loaded = NULL;
  rc = SALTS_OK;
  goto done;

rollback:
  (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
done:
  if (metadata) (void)sqlite3_finalize(metadata);
  if (rules) (void)sqlite3_finalize(rules);
  flow_security_sqlite_loaded_destroy(loaded);
  (void)sqlite3_close(database);
  return rc;
}

static void flow_security_sqlite_release(void *ctx, turbo_flow_security_policy_bundle_t *bundle) {
  (void)ctx;
  if (!bundle) return;
  flow_security_sqlite_loaded_destroy((flow_security_sqlite_loaded_t *)bundle->provider_bundle);
  *bundle = (turbo_flow_security_policy_bundle_t)TURBO_FLOW_SECURITY_POLICY_BUNDLE_INIT;
}

int turbo_flow_security_sqlite_provider_publish(turbo_flow_security_sqlite_provider_t *provider,
                                                const turbo_flow_security_policy_bundle_t *bundle) {
  sqlite3 *database = NULL;
  sqlite3_stmt *current = NULL;
  sqlite3_stmt *delete_rules = NULL;
  sqlite3_stmt *insert_rule = NULL;
  sqlite3_stmt *upsert_bundle = NULL;
  int status;
  int rc;
  if (!provider || !bundle || bundle->size < sizeof(*bundle) ||
      bundle->abi_version != TURBO_FLOW_SECURITY_ABI_V3 || bundle->policy_version == 0u ||
      bundle->policy_version > INT64_MAX || bundle->expires_at > INT64_MAX || !bundle->rules ||
      bundle->rule_count == 0u || bundle->rule_count > provider->max_rules)
    return SALTS_EINVAL;
  for (size_t i = 0u; i < bundle->rule_count; ++i)
    if (!flow_security_sqlite_rule_valid(&bundle->rules[i])) return SALTS_EINVAL;
  rc = flow_security_sqlite_open(provider, &database);
  if (rc != SALTS_OK) return rc;
  status =
      sqlite3_exec(database, "PRAGMA foreign_keys=ON;BEGIN IMMEDIATE;PRAGMA defer_foreign_keys=ON",
                   NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flow_security_sqlite_status(status);
    goto done;
  }
  status = sqlite3_prepare_v2(
      database, "SELECT policy_version FROM turbo_flow_acl_bundle_v3 WHERE namespace_name=?1", -1,
      &current, NULL);
  if (status != SQLITE_OK ||
      sqlite3_bind_text(current, 1, provider->namespace_name, -1, SQLITE_STATIC) != SQLITE_OK) {
    rc = flow_security_sqlite_status(status);
    goto rollback;
  }
  status = sqlite3_step(current);
  if (status == SQLITE_ROW) {
    if (sqlite3_column_type(current, 0) != SQLITE_INTEGER ||
        sqlite3_column_int64(current, 0) <= 0) {
      rc = SALTS_EPROTO;
      goto rollback;
    }
    if (bundle->policy_version <= (uint64_t)sqlite3_column_int64(current, 0)) {
      rc = SALTS_EBUSY;
      goto rollback;
    }
  } else if (status != SQLITE_DONE) {
    rc = flow_security_sqlite_status(status);
    goto rollback;
  }
  (void)sqlite3_finalize(current);
  current = NULL;
  status =
      sqlite3_prepare_v2(database, "DELETE FROM turbo_flow_acl_rule_v3 WHERE namespace_name=?1", -1,
                         &delete_rules, NULL);
  if (status != SQLITE_OK ||
      sqlite3_bind_text(delete_rules, 1, provider->namespace_name, -1, SQLITE_STATIC) !=
          SQLITE_OK ||
      sqlite3_step(delete_rules) != SQLITE_DONE) {
    rc = flow_security_sqlite_status(sqlite3_errcode(database));
    goto rollback;
  }
  (void)sqlite3_finalize(delete_rules);
  delete_rules = NULL;
  status = sqlite3_prepare_v2(
      database,
      "INSERT INTO turbo_flow_acl_rule_v3(namespace_name,ordinal,rule_line) VALUES(?1,?2,?3)",
      -1, &insert_rule, NULL);
  if (status != SQLITE_OK) {
    rc = flow_security_sqlite_status(status);
    goto rollback;
  }
  for (size_t i = 0u; i < bundle->rule_count; ++i) {
    const turbo_flow_security_rule_t *rule = &bundle->rules[i];
    char rule_line[TURBO_FLOW_SECURITY_RULE_LINE_MAX + 1u] = {0};
    size_t rule_line_size = 0u;
    rc = turbo_flow_security_rule_format_line(rule, rule_line, sizeof(rule_line),
                                              &rule_line_size);
    if (rc != SALTS_OK) goto rollback;
    sqlite3_reset(insert_rule);
    sqlite3_clear_bindings(insert_rule);
    if (sqlite3_bind_text(insert_rule, 1, provider->namespace_name, -1, SQLITE_STATIC) !=
            SQLITE_OK ||
        sqlite3_bind_int64(insert_rule, 2, (sqlite3_int64)i) != SQLITE_OK ||
        sqlite3_bind_text(insert_rule, 3, rule_line, (int)rule_line_size, SQLITE_TRANSIENT) !=
            SQLITE_OK ||
        sqlite3_step(insert_rule) != SQLITE_DONE) {
      rc = flow_security_sqlite_status(sqlite3_errcode(database));
      goto rollback;
    }
  }
  (void)sqlite3_finalize(insert_rule);
  insert_rule = NULL;
  status = sqlite3_prepare_v2(
      database,
      "INSERT INTO turbo_flow_acl_bundle_v3(namespace_name,policy_version,expires_at) VALUES(?1,?2,"
      "?3) ON CONFLICT(namespace_name) DO UPDATE SET policy_version=excluded.policy_version,"
      "expires_at=excluded.expires_at",
      -1, &upsert_bundle, NULL);
  if (status != SQLITE_OK ||
      sqlite3_bind_text(upsert_bundle, 1, provider->namespace_name, -1, SQLITE_STATIC) !=
          SQLITE_OK ||
      sqlite3_bind_int64(upsert_bundle, 2, (sqlite3_int64)bundle->policy_version) != SQLITE_OK ||
      sqlite3_bind_int64(upsert_bundle, 3, (sqlite3_int64)bundle->expires_at) != SQLITE_OK ||
      sqlite3_step(upsert_bundle) != SQLITE_DONE) {
    rc = flow_security_sqlite_status(sqlite3_errcode(database));
    goto rollback;
  }
  (void)sqlite3_finalize(upsert_bundle);
  upsert_bundle = NULL;
  status = sqlite3_exec(database, "COMMIT", NULL, NULL, NULL);
  rc = status == SQLITE_OK ? SALTS_OK : flow_security_sqlite_status(status);
  goto done;

rollback:
  (void)sqlite3_exec(database, "ROLLBACK", NULL, NULL, NULL);
done:
  if (current) (void)sqlite3_finalize(current);
  if (delete_rules) (void)sqlite3_finalize(delete_rules);
  if (insert_rule) (void)sqlite3_finalize(insert_rule);
  if (upsert_bundle) (void)sqlite3_finalize(upsert_bundle);
  (void)sqlite3_close(database);
  return rc;
}

int turbo_flow_security_sqlite_provider_create(const turbo_flow_security_sqlite_config_t *config,
                                               turbo_flow_security_sqlite_provider_t **out) {
  turbo_flow_security_sqlite_provider_t *provider;
  sqlite3 *database = NULL;
  int status;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) ||
      config->api_version != TURBO_FLOW_SECURITY_SQLITE_API_VERSION || !out ||
      !config->database_path || !config->database_path[0] || !config->namespace_name ||
      !config->namespace_name[0] || strcmp(config->database_path, ":memory:") == 0 ||
      strlen(config->namespace_name) > TURBO_FLOW_SECURITY_SQLITE_NAMESPACE_MAX ||
      config->busy_timeout_ms < 0 || config->max_rules == 0u ||
      config->max_rules > TURBO_FLOW_SECURITY_MAX_RULES)
    return SALTS_EINVAL;
  provider = (turbo_flow_security_sqlite_provider_t *)calloc(1u, sizeof(*provider));
  if (!provider) return SALTS_ENOMEM;
  provider->database_path = tstr_dup(config->database_path);
  provider->namespace_name = tstr_dup(config->namespace_name);
  provider->busy_timeout_ms = config->busy_timeout_ms;
  provider->max_rules = config->max_rules;
  if (!provider->database_path || !provider->namespace_name) {
    rc = SALTS_ENOMEM;
    goto fail;
  }
  rc = flow_security_sqlite_open(provider, &database);
  if (rc != SALTS_OK) goto fail;
  status = sqlite3_exec(database, FLOW_SECURITY_SQLITE_SCHEMA, NULL, NULL, NULL);
  if (status != SQLITE_OK) {
    rc = flow_security_sqlite_status(status);
    goto fail;
  }
  (void)sqlite3_close(database);
  database = NULL;
  provider->interface =
      (turbo_flow_security_policy_provider_t)TURBO_FLOW_SECURITY_POLICY_PROVIDER_INIT;
  provider->interface.ctx = provider;
  provider->interface.load = flow_security_sqlite_load;
  provider->interface.release = flow_security_sqlite_release;
  *out = provider;
  return SALTS_OK;

fail:
  if (database) (void)sqlite3_close(database);
  turbo_flow_security_sqlite_provider_destroy(provider);
  return rc;
}

const turbo_flow_security_policy_provider_t *turbo_flow_security_sqlite_provider_interface(
    const turbo_flow_security_sqlite_provider_t *provider) {
  return provider ? &provider->interface : NULL;
}

void turbo_flow_security_sqlite_provider_destroy(turbo_flow_security_sqlite_provider_t *provider) {
  if (!provider) return;
  tstr_freep(&provider->database_path);
  tstr_freep(&provider->namespace_name);
  free(provider);
}
