#include "flowie_control_dashboard_internal.h"
#include "flowie_control_dashboard_view_internal.h"
#include "flowie_control_http_request_internal.h"

#include "CoroNet/turbo_coro_context.h"
#include "http_common.h"
#include "iris/cookie.h"
#include "monocypher.h"
#include "platform.h"
#include "turbo_error.h"
#include "turbo_thread.h"

#include <ctype.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

enum {
  FLOWIE_CONTROL_DASHBOARD_FORM_FIELDS = 8,
  FLOWIE_CONTROL_DASHBOARD_HOST_MAX = 255
};

static const char FLOWIE_CONTROL_DASHBOARD_CSP[] =
    "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; "
    "img-src 'self'; form-action 'self'; frame-ancestors 'none'; base-uri 'none'";
static const char FLOWIE_CONTROL_DASHBOARD_LOGIN_CSP[] =
    "default-src 'none'; style-src 'self'; form-action 'self'; frame-ancestors 'none'; "
    "base-uri 'none'";

typedef struct flowie_control_dashboard_form_field_s {
  char *key;
  char *value;
} flowie_control_dashboard_form_field_t;

typedef struct flowie_control_dashboard_form_s {
  flowie_control_dashboard_form_field_t fields[FLOWIE_CONTROL_DASHBOARD_FORM_FIELDS];
  size_t count;
} flowie_control_dashboard_form_t;

enum {
  FLOWIE_CONTROL_DASHBOARD_LOGIN_ARMED = 1,
  FLOWIE_CONTROL_DASHBOARD_LOGIN_ACCEPTED = 2,
  FLOWIE_CONTROL_DASHBOARD_LOGIN_ABANDONED = 3
};

typedef struct flowie_control_dashboard_login_job_s {
  struct flowie_control_dashboard_s *dashboard;
  coro_wait_t *wait;
  atomic_uint references;
  atomic_int completed;
  atomic_int owner_state;
  char domain_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  char principal_id[TURBO_FLOW_SECURITY_ID_MAX + 1u];
  uint8_t secret[FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX];
  size_t secret_size;
  char remote_address[FLOWIE_CONTROL_AUTH_REMOTE_ADDRESS_MAX + 1u];
  char token[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u];
  int result;
} flowie_control_dashboard_login_job_t;

struct flowie_control_dashboard_s {
  flowie_control_management_service_t *service;
  flowie_control_dashboard_resolve_session_fn resolve_session;
  void *resolve_session_ctx;
  flowie_control_dashboard_clock_fn clock;
  void *clock_ctx;
  flowie_control_dashboard_login_fn login;
  flowie_control_dashboard_logout_fn logout;
  void *session_ctx;
  uint64_t session_ttl_seconds;
  turbo_threadpool_t *login_executor;
  uint32_t login_executor_deadline_ms;
  flowie_control_dashboard_view_t *view;
  iris_app_t *bound_app;
};

static void flowie_control_dashboard_login_job_release(
    flowie_control_dashboard_login_job_t *job) {
  if (!job || atomic_fetch_sub_explicit(&job->references, 1u, memory_order_acq_rel) != 1u) return;
  (void)coro_wait_destroy(job->wait);
  crypto_wipe(job, sizeof(*job));
  free(job);
}

static void flowie_control_dashboard_login_job_run(void *arg) {
  flowie_control_dashboard_login_job_t *job = (flowie_control_dashboard_login_job_t *)arg;
  int owner_state;
  int wake_rc;
  if (!job) return;
  job->result = job->dashboard->login(
      job->dashboard->session_ctx, job->domain_id, job->principal_id, job->secret,
      job->secret_size, job->remote_address, job->token);
  atomic_store_explicit(&job->completed, 1, memory_order_release);
  while (atomic_load_explicit(&job->owner_state, memory_order_acquire) ==
         FLOWIE_CONTROL_DASHBOARD_LOGIN_ARMED) {
    wake_rc = coro_wait_interrupt(job->wait, TURBO_EINTR);
    if (wake_rc != TURBO_EALREADY) break;
    turbo_thread_yield();
  }
  owner_state = atomic_load_explicit(&job->owner_state, memory_order_acquire);
  if (owner_state == FLOWIE_CONTROL_DASHBOARD_LOGIN_ABANDONED &&
      job->result == TURBO_OK)
    (void)job->dashboard->logout(job->dashboard->session_ctx, job->token);
  flowie_control_dashboard_login_job_release(job);
}

int flowie_control_dashboard_execute_login(
    flowie_control_dashboard_t *dashboard, const char *domain_id, const char *principal_id,
    const uint8_t *secret, size_t secret_size, const char *remote_address,
    char token_out[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u]) {
  flowie_control_dashboard_login_job_t *job;
  coro_context_t *context;
  int completed;
  int wait_rc = TURBO_OK;
  int rc;
  size_t domain_size;
  size_t principal_size;
  size_t remote_size;
  if (token_out) token_out[0] = '\0';
  if (!dashboard || !dashboard->login || !domain_id || !principal_id || !secret ||
      secret_size == 0u || secret_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX ||
      !remote_address || !token_out)
    return TURBO_EINVAL;
  domain_size = strnlen(domain_id, TURBO_FLOW_SECURITY_ID_MAX + 1u);
  principal_size = strnlen(principal_id, TURBO_FLOW_SECURITY_ID_MAX + 1u);
  remote_size = strnlen(remote_address, FLOWIE_CONTROL_AUTH_REMOTE_ADDRESS_MAX + 1u);
  if (domain_size == 0u || domain_size > TURBO_FLOW_SECURITY_ID_MAX ||
      principal_size == 0u || principal_size > TURBO_FLOW_SECURITY_ID_MAX ||
      remote_size == 0u || remote_size > FLOWIE_CONTROL_AUTH_REMOTE_ADDRESS_MAX)
    return TURBO_EINVAL;
  if (!dashboard->login_executor)
    return dashboard->login(dashboard->session_ctx, domain_id, principal_id, secret,
                            secret_size, remote_address, token_out);
  context = coro_context_current();
  if (!context) return TURBO_EINVAL;
  job = (flowie_control_dashboard_login_job_t *)calloc(1u, sizeof(*job));
  if (!job) return TURBO_ENOMEM;
  job->wait = coro_wait_create(context);
  if (!job->wait) {
    crypto_wipe(job, sizeof(*job));
    free(job);
    return TURBO_ENOMEM;
  }
  job->dashboard = dashboard;
  memcpy(job->domain_id, domain_id, domain_size + 1u);
  memcpy(job->principal_id, principal_id, principal_size + 1u);
  memcpy(job->secret, secret, secret_size);
  job->secret_size = secret_size;
  memcpy(job->remote_address, remote_address, remote_size + 1u);
  job->result = TURBO_EIO;
  atomic_init(&job->references, 2u);
  atomic_init(&job->completed, 0);
  atomic_init(&job->owner_state, FLOWIE_CONTROL_DASHBOARD_LOGIN_ARMED);
  if (turbo_threadpool_try_submit(dashboard->login_executor,
                                  flowie_control_dashboard_login_job_run, job) != 0) {
    atomic_store_explicit(&job->owner_state, FLOWIE_CONTROL_DASHBOARD_LOGIN_ABANDONED,
                          memory_order_release);
    flowie_control_dashboard_login_job_release(job);
    flowie_control_dashboard_login_job_release(job);
    return TURBO_EBUSY;
  }
  completed = atomic_load_explicit(&job->completed, memory_order_acquire);
  if (!completed) wait_rc = coro_wait_for(job->wait, dashboard->login_executor_deadline_ms);
  completed = atomic_load_explicit(&job->completed, memory_order_acquire);
  if (completed) {
    rc = job->result;
    if (rc == TURBO_OK) memcpy(token_out, job->token, sizeof(job->token));
    atomic_store_explicit(&job->owner_state, FLOWIE_CONTROL_DASHBOARD_LOGIN_ACCEPTED,
                          memory_order_release);
  } else {
    rc = wait_rc == TURBO_OK ? TURBO_ETIMEDOUT : wait_rc;
    atomic_store_explicit(&job->owner_state, FLOWIE_CONTROL_DASHBOARD_LOGIN_ABANDONED,
                          memory_order_release);
  }
  flowie_control_dashboard_login_job_release(job);
  return rc;
}

static int flowie_control_dashboard_csrf_valid(const char *token) {
  if (!token ||
      strnlen(token, FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u) != FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE)
    return 0;
  for (size_t index = 0u; index < FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE; ++index) {
    unsigned char byte = (unsigned char)token[index];
    if (!isalnum(byte) && byte != '-' && byte != '_') return 0;
  }
  return 1;
}

int flowie_control_dashboard_request_is_same_origin(const Req *request) {
  const char *host = NULL;
  const char *origin = NULL;
  const char *fetch_site = NULL;
  char expected[FLOWIE_CONTROL_DASHBOARD_HOST_MAX + 32u];
  int written;
  if (flowie_control_http_header_exact(request, "Host", &host) != TURBO_OK ||
      strnlen(host, FLOWIE_CONTROL_DASHBOARD_HOST_MAX + 1u) >
          FLOWIE_CONTROL_DASHBOARD_HOST_MAX)
    return 0;
  if (flowie_control_http_header_optional_exact(request, "Origin", &origin) != TURBO_OK) return 0;
  if (!origin || strcmp(origin, "null") == 0)
    return flowie_control_http_header_exact(request, "Sec-Fetch-Site", &fetch_site) == TURBO_OK &&
           strcmp(fetch_site, "same-origin") == 0;
  written = snprintf(expected, sizeof(expected), "https://%s", host);
  return written > 0 && (size_t)written < sizeof(expected) && strcmp(origin, expected) == 0;
}

static int flowie_control_dashboard_page_section_valid(
    const flowie_control_dashboard_page_t *page) {
  int has_users = page->users_after[0] != '\0';
  int has_groups = page->groups_after[0] != '\0';
  int has_roles = page->roles_after[0] != '\0';
  switch (page->section) {
    case FLOWIE_CONTROL_DASHBOARD_SECTION_ALL:
      return 1;
    case FLOWIE_CONTROL_DASHBOARD_SECTION_OVERVIEW:
      return !has_users && !has_groups && !has_roles && !page->policy_has_after &&
             !page->audit_has_after;
    case FLOWIE_CONTROL_DASHBOARD_SECTION_USERS:
      return !has_groups && !has_roles && !page->policy_has_after && !page->audit_has_after;
    case FLOWIE_CONTROL_DASHBOARD_SECTION_GROUPS:
      return !has_users && !has_roles && !page->policy_has_after && !page->audit_has_after;
    case FLOWIE_CONTROL_DASHBOARD_SECTION_ROLES:
      return !has_users && !has_groups && !page->policy_has_after && !page->audit_has_after;
    case FLOWIE_CONTROL_DASHBOARD_SECTION_ACLS:
      return !has_users && !has_groups && !has_roles && !page->audit_has_after;
    case FLOWIE_CONTROL_DASHBOARD_SECTION_AUDIT:
      return !has_users && !has_groups && !has_roles && !page->policy_has_after;
    default:
      return 0;
  }
}

static int flowie_control_dashboard_page_valid(const flowie_control_dashboard_page_t *page) {
  return page && page->size >= sizeof(*page) &&
         strnlen(page->domain_id, sizeof(page->domain_id)) <
             sizeof(page->domain_id) &&
         strnlen(page->users_after, sizeof(page->users_after)) < sizeof(page->users_after) &&
         strnlen(page->groups_after, sizeof(page->groups_after)) < sizeof(page->groups_after) &&
         strnlen(page->roles_after, sizeof(page->roles_after)) < sizeof(page->roles_after) &&
         (page->policy_has_after == 0 || page->policy_has_after == 1) &&
         (!page->policy_has_after || page->policy_after < TURBO_FLOW_SECURITY_MAX_RULES) &&
         (page->audit_has_after == 0 || page->audit_has_after == 1) &&
         (!page->audit_has_after || page->audit_after <= (uint64_t)INT64_MAX) &&
         flowie_control_dashboard_page_section_valid(page);
}

static int flowie_control_dashboard_section_parse(
    const char *value, flowie_control_dashboard_section_t *section_out) {
  if (!value || !section_out) return TURBO_EINVAL;
  if (strcmp(value, "overview") == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_OVERVIEW;
  else if (strcmp(value, "users") == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_USERS;
  else if (strcmp(value, "groups") == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_GROUPS;
  else if (strcmp(value, "roles") == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_ROLES;
  else if (strcmp(value, "acls") == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_ACLS;
  else if (strcmp(value, "audit") == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_AUDIT;
  else
    return TURBO_EPROTO;
  return TURBO_OK;
}

static int flowie_control_dashboard_section_from_path(
    const char *path, flowie_control_dashboard_section_t *section_out) {
  if (!path || !section_out) return TURBO_EINVAL;
  if (strcmp(path, FLOWIE_CONTROL_DASHBOARD_PATH) == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_OVERVIEW;
  else if (strcmp(path, FLOWIE_CONTROL_DASHBOARD_USERS_PATH) == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_USERS;
  else if (strcmp(path, FLOWIE_CONTROL_DASHBOARD_GROUPS_PATH) == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_GROUPS;
  else if (strcmp(path, FLOWIE_CONTROL_DASHBOARD_ROLES_PATH) == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_ROLES;
  else if (strcmp(path, FLOWIE_CONTROL_DASHBOARD_ACLS_PATH) == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_ACLS;
  else if (strcmp(path, FLOWIE_CONTROL_DASHBOARD_AUDIT_PATH) == 0)
    *section_out = FLOWIE_CONTROL_DASHBOARD_SECTION_AUDIT;
  else
    return TURBO_EPROTO;
  return TURBO_OK;
}

int flowie_control_dashboard_render_shell(flowie_control_dashboard_t *dashboard,
                                          const flowie_control_dashboard_page_t *page,
                                          char **html_out, size_t *html_size_out) {
  if (!dashboard || !flowie_control_dashboard_page_valid(page)) return TURBO_EINVAL;
  return flowie_control_dashboard_view_render_shell(dashboard->view, page, html_out,
                                                    html_size_out);
}

int flowie_control_dashboard_render_login(flowie_control_dashboard_t *dashboard, int group_mode,
                                          int show_error, char **html_out,
                                          size_t *html_size_out) {
  if (!dashboard) return TURBO_EINVAL;
  return flowie_control_dashboard_view_render_login(dashboard->view, group_mode, show_error,
                                                    html_out, html_size_out);
}

int flowie_control_dashboard_render_password(
    flowie_control_dashboard_t *dashboard,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u], char **html_out,
    size_t *html_size_out) {
  if (!dashboard) return TURBO_EINVAL;
  return flowie_control_dashboard_view_render_password(dashboard->view, csrf_token, html_out,
                                                       html_size_out);
}

void flowie_control_dashboard_action_result_clear(
    flowie_control_dashboard_action_result_t *result) {
  if (!result || result->size < sizeof(*result)) return;
  crypto_wipe(result->domain_id, sizeof(result->domain_id));
  crypto_wipe(result->principal_id, sizeof(result->principal_id));
  crypto_wipe(result->token, sizeof(result->token));
  result->kind = FLOWIE_CONTROL_DASHBOARD_ACTION_NONE;
  result->token_size = 0u;
}

static int flowie_control_dashboard_action_result_valid(
    const flowie_control_dashboard_action_result_t *result) {
  if (!result) return 1;
  if (result->size < sizeof(*result)) return 0;
  if (result->kind == FLOWIE_CONTROL_DASHBOARD_ACTION_NONE)
    return result->token_size == 0u;
  return result->kind == FLOWIE_CONTROL_DASHBOARD_ACTION_CREDENTIAL_ISSUED &&
         result->domain_id[0] != '\0' && result->principal_id[0] != '\0' &&
         result->token_size == FLOWIE_CONTROL_CREDENTIAL_TOKEN_SIZE &&
         result->token[result->token_size] == '\0';
}

int flowie_control_dashboard_render_page_result(
    flowie_control_dashboard_t *dashboard, const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
    const flowie_control_dashboard_page_t *page,
    const flowie_control_dashboard_action_result_t *action_result, char **html_out,
    size_t *html_size_out) {
  flowie_control_management_caller_t scoped = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  const char *target_domain_id;
  int rc;
  if (!dashboard || !caller || !flowie_control_dashboard_page_valid(page) ||
      !flowie_control_dashboard_action_result_valid(action_result) ||
      !flowie_control_dashboard_csrf_valid(csrf_token))
    return TURBO_EINVAL;
  target_domain_id =
      page->domain_id[0] ? page->domain_id : caller->domain_id;
  rc = flowie_control_management_scope_caller(dashboard->service, caller, target_domain_id,
                                              &scoped);
  if (rc != TURBO_OK) return rc;
  return flowie_control_dashboard_view_render_content(
      dashboard->view, dashboard->service, caller, &scoped, csrf_token, page, action_result,
      html_out, html_size_out);
}

int flowie_control_dashboard_render_page(
    flowie_control_dashboard_t *dashboard, const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
    const flowie_control_dashboard_page_t *page, char **html_out, size_t *html_size_out) {
  return flowie_control_dashboard_render_page_result(dashboard, caller, csrf_token, page, NULL,
                                                      html_out, html_size_out);
}

int flowie_control_dashboard_render(flowie_control_dashboard_t *dashboard,
                                    const flowie_control_management_caller_t *caller,
                                    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u],
                                    char **html_out, size_t *html_size_out) {
  const flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
  return flowie_control_dashboard_render_page(dashboard, caller, csrf_token, &page, html_out,
                                              html_size_out);
}

void flowie_control_dashboard_html_free(char *html) { free(html); }

static void flowie_control_dashboard_form_destroy(flowie_control_dashboard_form_t *form) {
  if (!form) return;
  for (size_t index = 0u; index < form->count; ++index) {
    free(form->fields[index].key);
    free(form->fields[index].value);
  }
  memset(form, 0, sizeof(*form));
}

static int flowie_control_dashboard_form_parse(const char *body, size_t body_size,
                                               flowie_control_dashboard_form_t *form) {
  char *copy;
  char *cursor;
  int rc = TURBO_EPROTO;
  if (!body || body_size == 0u || body_size > FLOWIE_CONTROL_DASHBOARD_BODY_MAX || !form ||
      memchr(body, '\0', body_size))
    return TURBO_EINVAL;
  memset(form, 0, sizeof(*form));
  copy = (char *)malloc(body_size + 1u);
  if (!copy) return TURBO_ENOMEM;
  memcpy(copy, body, body_size);
  copy[body_size] = '\0';
  cursor = copy;
  while (*cursor) {
    char *pair = cursor;
    char *separator = strchr(pair, '&');
    char *equals;
    char *key;
    char *value;
    if (separator) {
      *separator = '\0';
      cursor = separator + 1;
    } else {
      cursor += strlen(cursor);
    }
    if (form->count >= FLOWIE_CONTROL_DASHBOARD_FORM_FIELDS || !(equals = strchr(pair, '=')) ||
        strchr(equals + 1, '='))
      goto done;
    *equals = '\0';
    key = turbo_url_decode(pair);
    value = turbo_url_decode(equals + 1);
    if (!key || !value || !key[0]) {
      free(key);
      free(value);
      goto done;
    }
    for (size_t index = 0u; index < form->count; ++index) {
      if (strcmp(form->fields[index].key, key) == 0) {
        free(key);
        free(value);
        goto done;
      }
    }
    form->fields[form->count].key = key;
    form->fields[form->count].value = value;
    ++form->count;
  }
  rc = form->count > 0u ? TURBO_OK : TURBO_EPROTO;

done:
  free(copy);
  if (rc != TURBO_OK) flowie_control_dashboard_form_destroy(form);
  return rc;
}

static const char *flowie_control_dashboard_form_get(const flowie_control_dashboard_form_t *form,
                                                     const char *key) {
  if (!form || !key) return NULL;
  for (size_t index = 0u; index < form->count; ++index) {
    if (strcmp(form->fields[index].key, key) == 0) return form->fields[index].value;
  }
  return NULL;
}

static int flowie_control_dashboard_form_exact(const flowie_control_dashboard_form_t *form,
                                               const char *const *keys, size_t key_count) {
  if (!form || !keys || form->count != key_count) return 0;
  for (size_t index = 0u; index < key_count; ++index) {
    if (!flowie_control_dashboard_form_get(form, keys[index])) return 0;
  }
  return 1;
}

static int flowie_control_dashboard_u64(const char *text, int required, uint64_t *out) {
  char *end = NULL;
  unsigned long long value;
  if (!text || !text[0]) return required ? TURBO_EPROTO : TURBO_OK;
  if (*text == '+' || *text == '-' || (text[0] == '0' && text[1] != '\0')) return TURBO_EPROTO;
  value = strtoull(text, &end, 10);
  if (!end || *end != '\0') return TURBO_EPROTO;
  *out = (uint64_t)value;
  return TURBO_OK;
}

static int flowie_control_dashboard_page_text(const char *encoded, char *out, size_t capacity) {
  char *decoded;
  char *canonical;
  size_t size;
  int rc = TURBO_EPROTO;
  if (!encoded || !encoded[0] || !out || capacity < 2u) return TURBO_EPROTO;
  decoded = turbo_url_decode(encoded);
  if (!decoded) return TURBO_ENOMEM;
  canonical = turbo_url_encode(decoded);
  if (!canonical) {
    free(decoded);
    return TURBO_ENOMEM;
  }
  size = strnlen(decoded, capacity);
  if (size > 0u && size < capacity && strcmp(encoded, canonical) == 0) {
    memcpy(out, decoded, size + 1u);
    rc = TURBO_OK;
  }
  free(canonical);
  free(decoded);
  return rc;
}

int flowie_control_dashboard_page_parse(const Req *request, flowie_control_dashboard_page_t *out) {
  flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
  int has_section = 0;
  if (out && out->size >= sizeof(*out)) *out = page;
  if (!request || !out || out->size < sizeof(*out) || request->query.count < 0 ||
      request->query.count > 7 || (request->query.count > 0 && !request->query.items))
    return TURBO_EINVAL;
  for (int index = 0; index < request->query.count; ++index) {
    const char *key = request->query.items[index].key;
    const char *value = request->query.items[index].value;
    uint64_t number = 0u;
    int rc;
    if (!key || !value) return TURBO_EPROTO;
    if (strcmp(key, "domain_id") == 0) {
      if (page.domain_id[0]) return TURBO_EPROTO;
      rc = flowie_control_dashboard_page_text(value, page.domain_id,
                                              sizeof(page.domain_id));
    } else if (strcmp(key, "section") == 0) {
      if (has_section) return TURBO_EPROTO;
      rc = flowie_control_dashboard_section_parse(value, &page.section);
      has_section = rc == TURBO_OK;
    } else if (strcmp(key, "users_after") == 0) {
      if (page.users_after[0]) return TURBO_EPROTO;
      rc = flowie_control_dashboard_page_text(value, page.users_after, sizeof(page.users_after));
    } else if (strcmp(key, "groups_after") == 0) {
      if (page.groups_after[0]) return TURBO_EPROTO;
      rc = flowie_control_dashboard_page_text(value, page.groups_after, sizeof(page.groups_after));
    } else if (strcmp(key, "roles_after") == 0) {
      if (page.roles_after[0]) return TURBO_EPROTO;
      rc = flowie_control_dashboard_page_text(value, page.roles_after, sizeof(page.roles_after));
    } else if (strcmp(key, "policy_after") == 0) {
      if (page.policy_has_after) return TURBO_EPROTO;
      rc = flowie_control_dashboard_u64(value, 1, &number);
      if (rc == TURBO_OK && number >= TURBO_FLOW_SECURITY_MAX_RULES) rc = TURBO_EPROTO;
      if (rc == TURBO_OK) {
        page.policy_after = (uint32_t)number;
        page.policy_has_after = 1;
      }
    } else if (strcmp(key, "audit_after") == 0) {
      if (page.audit_has_after) return TURBO_EPROTO;
      rc = flowie_control_dashboard_u64(value, 1, &number);
      if (rc == TURBO_OK && number > (uint64_t)INT64_MAX) rc = TURBO_EPROTO;
      if (rc == TURBO_OK) {
        page.audit_after = number;
        page.audit_has_after = 1;
      }
    } else {
      return TURBO_EPROTO;
    }
    if (rc != TURBO_OK) return rc;
  }
  if (!flowie_control_dashboard_page_valid(&page)) return TURBO_EPROTO;
  *out = page;
  return TURBO_OK;
}

static int flowie_control_dashboard_command_text(const char *text, size_t maximum) {
  size_t size;
  if (!text) return 0;
  size = strnlen(text, maximum + 1u);
  return size > 0u && size <= maximum;
}

int flowie_control_dashboard_process_form_result(
    flowie_control_dashboard_t *dashboard, const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u], const char *body,
    size_t body_size, flowie_control_dashboard_action_result_t *result_out) {
  static const char *const domain_keys[] = {"csrf", "operation", "domain_id", "request_id"};
  static const char *const user_keys[] = {"csrf", "operation", "principal_id",
                                           "principal_type", "request_id"};
  static const char *const user_disable_keys[] = {"csrf", "operation", "principal_id",
                                                  "request_id"};
  static const char *const group_keys[] = {
      "csrf", "operation", "group_id", "parent_group_id", "request_id"};
  static const char *const group_delete_keys[] = {"csrf", "operation", "group_id", "request_id"};
  static const char *const membership_keys[] = {"csrf", "operation", "principal_id", "group_id",
                                                "request_id"};
  static const char *const role_keys[] = {"csrf", "operation", "role_id", "request_id"};
  static const char *const assignment_keys[] = {"csrf", "operation", "principal_id", "role_id",
                                                 "request_id"};
  static const char *const credential_keys[] = {"csrf", "operation", "principal_id",
                                                 "request_id"};
  static const char *const rule_keys[] = {"csrf", "operation", "ordinal", "rule_line",
                                          "request_id"};
  static const char *const rule_delete_keys[] = {"csrf", "operation", "ordinal", "request_id"};
  static const char *const publish_keys[] = {"csrf", "operation", "request_id", "expires_at"};
  flowie_control_dashboard_form_t form;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  const char *submitted_csrf;
  const char *operation;
  const char *request_id;
  uint64_t occurred_at;
  int rc;
  if (!dashboard || !caller || !result_out || result_out->size < sizeof(*result_out) ||
      !flowie_control_dashboard_csrf_valid(csrf_token))
    return TURBO_EINVAL;
  flowie_control_dashboard_action_result_clear(result_out);
  rc = flowie_control_dashboard_form_parse(body, body_size, &form);
  if (rc != TURBO_OK) return rc;
  submitted_csrf = flowie_control_dashboard_form_get(&form, "csrf");
  operation = flowie_control_dashboard_form_get(&form, "operation");
  request_id = flowie_control_dashboard_form_get(&form, "request_id");
  if (!submitted_csrf || strlen(submitted_csrf) != FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE ||
      crypto_verify64((const uint8_t *)submitted_csrf, (const uint8_t *)csrf_token) != 0 ||
      !operation ||
      !flowie_control_dashboard_command_text(request_id, FLOWIE_CONTROL_REQUEST_ID_MAX)) {
    rc = TURBO_EPERM;
    goto done;
  }
  occurred_at = dashboard->clock(dashboard->clock_ctx);
  if (occurred_at == 0u) {
    rc = TURBO_EIO;
    goto done;
  }
  if (strcmp(operation, "domain.create") == 0) {
    flowie_control_domain_create_command_t command = FLOWIE_CONTROL_DOMAIN_CREATE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(&form, domain_keys,
                                             sizeof(domain_keys) / sizeof(domain_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = flowie_control_dashboard_form_get(&form, "domain_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_domain_create(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "user.create") == 0) {
    flowie_control_user_create_command_t command = FLOWIE_CONTROL_USER_CREATE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(&form, user_keys,
                                             sizeof(user_keys) / sizeof(user_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.principal_type = flowie_control_dashboard_form_get(&form, "principal_type");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_create(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "user.disable") == 0) {
    flowie_control_user_disable_command_t command = FLOWIE_CONTROL_USER_DISABLE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(
            &form, user_disable_keys, sizeof(user_disable_keys) / sizeof(user_disable_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_disable(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "credential.issue") == 0) {
    flowie_control_credential_issue_command_t command =
        FLOWIE_CONTROL_CREDENTIAL_ISSUE_COMMAND_INIT;
    flowie_control_generated_credential_t generated = FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
    const char *principal_id;
    if (!flowie_control_dashboard_form_exact(
            &form, credential_keys, sizeof(credential_keys) / sizeof(credential_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.domain_id = caller->domain_id;
    command.principal_id = principal_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_credential_generate(dashboard->service, caller, &command,
                                                       &generated);
    if (rc == TURBO_EALREADY) {
      flowie_control_generated_credential_wipe(&generated);
      generated = (flowie_control_generated_credential_t)FLOWIE_CONTROL_GENERATED_CREDENTIAL_INIT;
      rc = flowie_control_management_credential_rotate(dashboard->service, caller, &command,
                                                       &generated);
    }
    if (rc == TURBO_OK &&
        (generated.token_size != FLOWIE_CONTROL_CREDENTIAL_TOKEN_SIZE ||
         generated.token[generated.token_size] != '\0'))
      rc = TURBO_EIO;
    if (rc == TURBO_OK) {
      result_out->kind = FLOWIE_CONTROL_DASHBOARD_ACTION_CREDENTIAL_ISSUED;
      result_out->token_size = generated.token_size;
      memcpy(result_out->domain_id, caller->domain_id, strlen(caller->domain_id) + 1u);
      memcpy(result_out->principal_id, principal_id, strlen(principal_id) + 1u);
      memcpy(result_out->token, generated.token, generated.token_size + 1u);
    }
    flowie_control_generated_credential_wipe(&generated);
  } else if (strcmp(operation, "credential.revoke") == 0) {
    flowie_control_credential_revoke_command_t command =
        FLOWIE_CONTROL_CREDENTIAL_REVOKE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(
            &form, credential_keys, sizeof(credential_keys) / sizeof(credential_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_credential_revoke(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "group.create") == 0) {
    flowie_control_group_create_command_t command = FLOWIE_CONTROL_GROUP_CREATE_COMMAND_INIT;
    const char *parent_group_id;
    if (!flowie_control_dashboard_form_exact(&form, group_keys,
                                             sizeof(group_keys) / sizeof(group_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.group_id = flowie_control_dashboard_form_get(&form, "group_id");
    parent_group_id = flowie_control_dashboard_form_get(&form, "parent_group_id");
    command.parent_group_id =
        parent_group_id && strcmp(parent_group_id, caller->domain_id) != 0 ? parent_group_id : NULL;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_group_create(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "group.delete") == 0) {
    flowie_control_group_delete_command_t command = FLOWIE_CONTROL_GROUP_DELETE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(&form, group_delete_keys,
                                             sizeof(group_delete_keys) /
                                                 sizeof(group_delete_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.group_id = flowie_control_dashboard_form_get(&form, "group_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_group_delete(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "group.member.add") == 0) {
    flowie_control_membership_add_command_t command = FLOWIE_CONTROL_MEMBERSHIP_ADD_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(
            &form, membership_keys, sizeof(membership_keys) / sizeof(membership_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.group_id = flowie_control_dashboard_form_get(&form, "group_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_membership_add(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "group.member.remove") == 0) {
    flowie_control_membership_remove_command_t command =
        FLOWIE_CONTROL_MEMBERSHIP_REMOVE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(
            &form, membership_keys, sizeof(membership_keys) / sizeof(membership_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.group_id = flowie_control_dashboard_form_get(&form, "group_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_membership_remove(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "role.create") == 0) {
    flowie_control_role_create_command_t command = FLOWIE_CONTROL_ROLE_CREATE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(&form, role_keys,
                                             sizeof(role_keys) / sizeof(role_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.role_id = flowie_control_dashboard_form_get(&form, "role_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_role_create(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "role.disable") == 0) {
    flowie_control_role_disable_command_t command = FLOWIE_CONTROL_ROLE_DISABLE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(&form, role_keys,
                                             sizeof(role_keys) / sizeof(role_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.role_id = flowie_control_dashboard_form_get(&form, "role_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_role_disable(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "role.assign") == 0) {
    flowie_control_user_role_add_command_t command = FLOWIE_CONTROL_USER_ROLE_ADD_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(
            &form, assignment_keys, sizeof(assignment_keys) / sizeof(assignment_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.role_id = flowie_control_dashboard_form_get(&form, "role_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_role_add(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "role.remove") == 0) {
    flowie_control_user_role_remove_command_t command =
        FLOWIE_CONTROL_USER_ROLE_REMOVE_COMMAND_INIT;
    if (!flowie_control_dashboard_form_exact(
            &form, assignment_keys, sizeof(assignment_keys) / sizeof(assignment_keys[0]))) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.principal_id = flowie_control_dashboard_form_get(&form, "principal_id");
    command.role_id = flowie_control_dashboard_form_get(&form, "role_id");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_user_role_remove(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "policy.rule.put") == 0) {
    flowie_control_policy_rule_put_command_t command = FLOWIE_CONTROL_POLICY_RULE_PUT_COMMAND_INIT;
    uint64_t ordinal = 0u;
    if (!flowie_control_dashboard_form_exact(&form, rule_keys,
                                             sizeof(rule_keys) / sizeof(rule_keys[0])) ||
        flowie_control_dashboard_u64(flowie_control_dashboard_form_get(&form, "ordinal"), 1,
                                     &ordinal) != TURBO_OK ||
        ordinal >= TURBO_FLOW_SECURITY_MAX_RULES) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.ordinal = (uint32_t)ordinal;
    command.rule_line = flowie_control_dashboard_form_get(&form, "rule_line");
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc = flowie_control_management_policy_rule_put(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "policy.rule.delete") == 0) {
    flowie_control_policy_rule_delete_command_t command =
        FLOWIE_CONTROL_POLICY_RULE_DELETE_COMMAND_INIT;
    uint64_t ordinal = 0u;
    if (!flowie_control_dashboard_form_exact(
            &form, rule_delete_keys, sizeof(rule_delete_keys) / sizeof(rule_delete_keys[0])) ||
        flowie_control_dashboard_u64(flowie_control_dashboard_form_get(&form, "ordinal"), 1,
                                     &ordinal) != TURBO_OK ||
        ordinal >= TURBO_FLOW_SECURITY_MAX_RULES) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.ordinal = (uint32_t)ordinal;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    rc =
        flowie_control_management_policy_rule_delete(dashboard->service, caller, &command, &result);
  } else if (strcmp(operation, "policy.publish") == 0) {
    flowie_control_policy_publish_command_t command = FLOWIE_CONTROL_POLICY_PUBLISH_COMMAND_INIT;
    flowie_control_policy_publish_result_t publish = FLOWIE_CONTROL_POLICY_PUBLISH_RESULT_INIT;
    uint64_t expires_at = 0u;
    if (!flowie_control_dashboard_form_exact(&form, publish_keys,
                                             sizeof(publish_keys) / sizeof(publish_keys[0])) ||
        flowie_control_dashboard_u64(flowie_control_dashboard_form_get(&form, "expires_at"), 0,
                                     &expires_at) != TURBO_OK) {
      rc = TURBO_EPROTO;
      goto done;
    }
    command.domain_id = caller->domain_id;
    command.actor = caller->actor;
    command.request_id = request_id;
    command.occurred_at = occurred_at;
    command.expires_at = expires_at;
    rc = flowie_control_management_policy_publish(dashboard->service, caller, &command, &publish);
  } else {
    rc = TURBO_EPROTO;
  }

done:
  flowie_control_dashboard_form_destroy(&form);
  return rc;
}

int flowie_control_dashboard_process_form(
    flowie_control_dashboard_t *dashboard, const flowie_control_management_caller_t *caller,
    const char csrf_token[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u], const char *body,
    size_t body_size) {
  flowie_control_dashboard_action_result_t result = FLOWIE_CONTROL_DASHBOARD_ACTION_RESULT_INIT;
  int rc = flowie_control_dashboard_process_form_result(dashboard, caller, csrf_token, body,
                                                        body_size, &result);
  flowie_control_dashboard_action_result_clear(&result);
  return rc;
}

static const char *const FLOWIE_CONTROL_DASHBOARD_ROUTES[] = {
    FLOWIE_CONTROL_DASHBOARD_PATH,
    FLOWIE_CONTROL_DASHBOARD_USERS_PATH,
    FLOWIE_CONTROL_DASHBOARD_GROUPS_PATH,
    FLOWIE_CONTROL_DASHBOARD_ROLES_PATH,
    FLOWIE_CONTROL_DASHBOARD_ACLS_PATH,
    FLOWIE_CONTROL_DASHBOARD_AUDIT_PATH,
    FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH,
    FLOWIE_CONTROL_DASHBOARD_ACTION_PATH,
    FLOWIE_CONTROL_DASHBOARD_CSS_PATH,
    FLOWIE_CONTROL_DASHBOARD_JS_PATH,
    FLOWIE_CONTROL_DASHBOARD_HTMX_PATH,
    FLOWIE_CONTROL_DASHBOARD_LOGIN_PATH,
    FLOWIE_CONTROL_DASHBOARD_PASSWORD_PATH, FLOWIE_CONTROL_DASHBOARD_LOGOUT_PATH};

static void flowie_control_dashboard_headers(Res *response) {
  set_header(response, "Cache-Control", "no-store");
  set_header(response, "Content-Security-Policy", FLOWIE_CONTROL_DASHBOARD_CSP);
  set_header(response, "X-Content-Type-Options", "nosniff");
  set_header(response, "X-Frame-Options", "DENY");
  set_header(response, "Referrer-Policy", "no-referrer");
  set_header(response, "Cross-Origin-Opener-Policy", "same-origin");
  set_header(response, "Cross-Origin-Resource-Policy", "same-origin");
  set_header(response, "Permissions-Policy", "camera=(), microphone=(), geolocation=()");
}

static void flowie_control_dashboard_login_headers(Res *response) {
  flowie_control_dashboard_headers(response);
  set_header(response, "Content-Security-Policy", FLOWIE_CONTROL_DASHBOARD_LOGIN_CSP);
}

static void flowie_control_dashboard_redirect(Res *response, const char *location) {
  set_header(response, "Location", location);
  reply(response, SEE_OTHER, "text/plain; charset=utf-8", "", 0u);
}

static void flowie_control_dashboard_auth_required(Res *response, int htmx) {
  if (htmx) {
    set_header(response, "HX-Redirect", FLOWIE_CONTROL_DASHBOARD_LOGIN_PATH);
    send_text(response, UNAUTHORIZED, "Authentication required");
  } else {
    flowie_control_dashboard_redirect(response, FLOWIE_CONTROL_DASHBOARD_LOGIN_PATH);
  }
}

int flowie_control_dashboard_request_is_htmx(const Req *request) {
  const char *header;
  return flowie_control_http_header_exact(request, "HX-Request", &header) == TURBO_OK &&
         strcmp(header, "true") == 0;
}

static int flowie_control_dashboard_status(int rc) {
  return rc == TURBO_EPERM                          ? FORBIDDEN
         : rc == TURBO_EBUSY                        ? CONFLICT
         : rc == TURBO_ENOENT                       ? NOT_FOUND
         : rc == TURBO_EPROTO || rc == TURBO_EINVAL ? BAD_REQUEST
                                                    : INTERNAL_SERVER_ERROR;
}

static const char *flowie_control_dashboard_error_message(int status) {
  return status == FORBIDDEN      ? "You do not have permission to perform this action."
         : status == CONFLICT     ? "The control state changed. Reload and submit again."
         : status == NOT_FOUND    ? "That item no longer exists. Reload the page and try again."
         : status == BAD_REQUEST  ? "Check the fields and try again."
                                  : "Unable to complete the request. Try again or contact an administrator.";
}

static flowie_control_dashboard_t *flowie_control_dashboard_from_request(const Req *request) {
  if (!request || !request->app || !request->path) return NULL;
  return (flowie_control_dashboard_t *)iris_app_lookup_rpc_context(request->app, request->path);
}

static int flowie_control_dashboard_resolve(
    flowie_control_dashboard_t *dashboard, const Req *request,
    flowie_control_management_caller_t *caller,
    char csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u]) {
  int rc;
  if (!dashboard || !request || !caller || !csrf) return TURBO_EINVAL;
  rc = dashboard->resolve_session(dashboard->resolve_session_ctx, request, caller, csrf);
  if (rc == TURBO_OK && !flowie_control_dashboard_csrf_valid(csrf)) rc = TURBO_EPERM;
  return rc;
}

static void flowie_control_dashboard_send_error(flowie_control_dashboard_t *dashboard,
                                                Res *response, int rc) {
  char *html = NULL;
  size_t html_size = 0u;
  int status = flowie_control_dashboard_status(rc);
  int render_rc = flowie_control_dashboard_view_render_error(
      dashboard->view, flowie_control_dashboard_error_message(status), &html, &html_size);
  if (render_rc != TURBO_OK) {
    send_text(response, INTERNAL_SERVER_ERROR, "Internal error");
    return;
  }
  set_header(response, "HX-Retarget", "#dashboard-feedback");
  set_header(response, "HX-Reswap", "innerHTML");
  reply(response, status, "text/html; charset=utf-8", html, html_size);
  flowie_control_dashboard_html_free(html);
}

static void flowie_control_dashboard_shell_handler(Req *request, Res *response) {
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
  char csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u] = {0};
  char *html = NULL;
  size_t html_size = 0u;
  int rc;
  flowie_control_dashboard_headers(response);
  if (!dashboard || !request->security || !request->security->authenticated) {
    flowie_control_dashboard_auth_required(response, 0);
    return;
  }
  rc = flowie_control_dashboard_resolve(dashboard, request, &caller, csrf);
  if (rc == TURBO_OK && caller.permissions == FLOWIE_CONTROL_MANAGEMENT_PASSWORD_CHANGE) {
    crypto_wipe(csrf, sizeof(csrf));
    flowie_control_dashboard_redirect(response, FLOWIE_CONTROL_DASHBOARD_PASSWORD_PATH);
    return;
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_page_parse(request, &page);
  if (rc == TURBO_OK && page.section != FLOWIE_CONTROL_DASHBOARD_SECTION_ALL)
    rc = TURBO_EPROTO;
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_section_from_path(request->path, &page.section);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_shell(dashboard, &page, &html, &html_size);
  crypto_wipe(csrf, sizeof(csrf));
  if (rc != TURBO_OK) {
    send_text(response, flowie_control_dashboard_status(rc), "Dashboard unavailable");
    return;
  }
  reply(response, OK, "text/html; charset=utf-8", html, html_size);
  flowie_control_dashboard_html_free(html);
}

static void flowie_control_dashboard_content_handler(Req *request, Res *response) {
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
  char csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u] = {0};
  char *html = NULL;
  size_t html_size = 0u;
  int rc;
  flowie_control_dashboard_headers(response);
  if (!dashboard || !request->security || !request->security->authenticated) {
    flowie_control_dashboard_auth_required(response, 1);
    return;
  }
  if (!flowie_control_dashboard_request_is_htmx(request)) {
    send_text(response, BAD_REQUEST, "HTMX request required");
    return;
  }
  rc = flowie_control_dashboard_resolve(dashboard, request, &caller, csrf);
  if (rc == TURBO_OK && caller.permissions == FLOWIE_CONTROL_MANAGEMENT_PASSWORD_CHANGE) {
    crypto_wipe(csrf, sizeof(csrf));
    set_header(response, "HX-Redirect", FLOWIE_CONTROL_DASHBOARD_PASSWORD_PATH);
    send_text(response, FORBIDDEN, "Password change required");
    return;
  }
  if (rc == TURBO_OK) rc = flowie_control_dashboard_page_parse(request, &page);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_page(dashboard, &caller, csrf, &page, &html, &html_size);
  crypto_wipe(csrf, sizeof(csrf));
  if (rc != TURBO_OK) {
    flowie_control_dashboard_send_error(dashboard, response, rc);
    return;
  }
  reply(response, OK, "text/html; charset=utf-8", html, html_size);
  flowie_control_dashboard_html_free(html);
}

static void flowie_control_dashboard_post_handler(Req *request, Res *response) {
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_management_caller_t scoped = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_dashboard_page_t page = FLOWIE_CONTROL_DASHBOARD_PAGE_INIT;
  flowie_control_dashboard_action_result_t action_result =
      FLOWIE_CONTROL_DASHBOARD_ACTION_RESULT_INIT;
  char csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u] = {0};
  char *html = NULL;
  size_t html_size = 0u;
  const char *content_type;
  int rc;
  int has_secret = 0;
  flowie_control_dashboard_headers(response);
  if (!dashboard || !request->security || !request->security->authenticated) {
    flowie_control_dashboard_auth_required(response, 1);
    return;
  }
  if (!flowie_control_dashboard_request_is_htmx(request)) {
    send_text(response, BAD_REQUEST, "HTMX request required");
    return;
  }
  if (flowie_control_http_header_exact(request, "Content-Type", &content_type) != TURBO_OK ||
      strcmp(content_type, "application/x-www-form-urlencoded") != 0 ||
      request->body_len == 0u || request->body_len > FLOWIE_CONTROL_DASHBOARD_BODY_MAX) {
    flowie_control_dashboard_send_error(dashboard, response, TURBO_EPROTO);
    return;
  }
  rc = flowie_control_dashboard_resolve(dashboard, request, &caller, csrf);
  if (rc == TURBO_OK) rc = flowie_control_dashboard_page_parse(request, &page);
  if (rc == TURBO_OK)
    rc = flowie_control_management_scope_caller(
        dashboard->service, &caller,
        page.domain_id[0] ? page.domain_id : caller.domain_id, &scoped);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_process_form_result(
        dashboard, &scoped, csrf, request->body, request->body_len, &action_result);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_page_result(dashboard, &caller, csrf, &page,
                                                      &action_result, &html, &html_size);
  has_secret = action_result.kind == FLOWIE_CONTROL_DASHBOARD_ACTION_CREDENTIAL_ISSUED;
  flowie_control_dashboard_action_result_clear(&action_result);
  crypto_wipe(csrf, sizeof(csrf));
  if (rc != TURBO_OK) {
    flowie_control_dashboard_send_error(dashboard, response, rc);
    return;
  }
  reply(response, OK, "text/html; charset=utf-8", html, html_size);
  if (has_secret) crypto_wipe(html, html_size);
  flowie_control_dashboard_html_free(html);
}

static void flowie_control_dashboard_asset_handler(Req *request, Res *response,
                                                   flowie_control_dashboard_asset_t asset,
                                                   const char *content_type) {
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  const void *data = NULL;
  size_t size = 0u;
  int rc;
  flowie_control_dashboard_headers(response);
  if (!dashboard) {
    send_text(response, NOT_FOUND, "Asset unavailable");
    return;
  }
  rc = flowie_control_dashboard_view_asset(dashboard->view, asset, &data, &size);
  if (rc != TURBO_OK) {
    send_text(response, INTERNAL_SERVER_ERROR, "Asset unavailable");
    return;
  }
  reply(response, OK, content_type, data, size);
}

static void flowie_control_dashboard_css_handler(Req *request, Res *response) {
  flowie_control_dashboard_asset_handler(request, response, FLOWIE_CONTROL_DASHBOARD_ASSET_CSS,
                                         "text/css; charset=utf-8");
}

static void flowie_control_dashboard_js_handler(Req *request, Res *response) {
  flowie_control_dashboard_asset_handler(request, response, FLOWIE_CONTROL_DASHBOARD_ASSET_JS,
                                         "text/javascript; charset=utf-8");
}

static void flowie_control_dashboard_htmx_handler(Req *request, Res *response) {
  flowie_control_dashboard_asset_handler(request, response, FLOWIE_CONTROL_DASHBOARD_ASSET_HTMX,
                                         "text/javascript; charset=utf-8");
}

static int flowie_control_dashboard_login_group_mode(const Req *request, int *group_mode_out) {
  const request_item_t *scope;
  if (group_mode_out) *group_mode_out = 0;
  if (!request || !group_mode_out || request->query.count < 0 ||
      (request->query.count > 0 && !request->query.items))
    return TURBO_EINVAL;
  if (request->query.count == 0) return TURBO_OK;
  if (request->query.count != 1) return TURBO_EPROTO;
  scope = &request->query.items[0];
  if (!scope->key || !scope->value || strcmp(scope->key, "scope") != 0 ||
      strcmp(scope->value, "group") != 0)
    return TURBO_EPROTO;
  *group_mode_out = 1;
  return TURBO_OK;
}

static void flowie_control_dashboard_login_get_handler(Req *request, Res *response) {
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  char *html = NULL;
  size_t html_size = 0u;
  int group_mode = 0;
  int rc;
  flowie_control_dashboard_login_headers(response);
  if (!dashboard) {
    send_text(response, NOT_FOUND, "Login unavailable");
    return;
  }
  if (request->security && request->security->authenticated) {
    flowie_control_dashboard_redirect(response, FLOWIE_CONTROL_DASHBOARD_PATH);
    return;
  }
  rc = flowie_control_dashboard_login_group_mode(request, &group_mode);
  if (rc == TURBO_OK)
    rc = flowie_control_dashboard_render_login(dashboard, group_mode, 0, &html, &html_size);
  if (rc != TURBO_OK) {
    send_text(response, rc == TURBO_EPROTO ? BAD_REQUEST : INTERNAL_SERVER_ERROR,
              "Login unavailable");
    return;
  }
  reply(response, OK, "text/html; charset=utf-8", html, html_size);
  flowie_control_dashboard_html_free(html);
}

static void flowie_control_dashboard_login_post_handler(Req *request, Res *response) {
  static const char *const keys[] = {"domain", "principal", "password"};
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  flowie_control_dashboard_form_t form = {0};
  char token[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {0};
  const char *content_type;
  const char *domain = NULL;
  const char *principal = NULL;
  const char *password = NULL;
  char *html = NULL;
  size_t html_size = 0u;
  size_t password_size;
  int rc = TURBO_EPROTO;
  flowie_control_dashboard_login_headers(response);
  if (!dashboard || !dashboard->login || !request->client || request->body_stream ||
      !request->body || request->body_len == 0u ||
      request->body_len > FLOWIE_CONTROL_DASHBOARD_BODY_MAX ||
      !flowie_control_dashboard_request_is_same_origin(request)) {
    send_text(response, BAD_REQUEST, "Invalid login request");
    return;
  }
  if (flowie_control_http_header_exact(request, "Content-Type", &content_type) != TURBO_OK ||
      strcmp(content_type, "application/x-www-form-urlencoded") != 0) {
    send_text(response, BAD_REQUEST, "Invalid login request");
    return;
  }
  rc = flowie_control_dashboard_form_parse(request->body, request->body_len, &form);
  if (rc != TURBO_OK ||
      !flowie_control_dashboard_form_exact(&form, keys, sizeof(keys) / sizeof(keys[0])))
    goto denied;
  domain = flowie_control_dashboard_form_get(&form, keys[0]);
  principal = flowie_control_dashboard_form_get(&form, keys[1]);
  password = flowie_control_dashboard_form_get(&form, keys[2]);
  password_size = password ? strnlen(password, FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX + 1u) : 0u;
  if (!domain || !principal || password_size == 0u ||
      password_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX)
    goto denied;
  rc = flowie_control_dashboard_execute_login(dashboard, domain, principal,
                                              (const uint8_t *)password, password_size,
                                              "management-dashboard", token);
  if (rc == TURBO_OK) {
    cookie_options_t options = {(int)dashboard->session_ttl_seconds, "/v2/control", "Strict",
                                true, true};
    set_cookie(response, FLOWIE_CONTROL_MANAGEMENT_SESSION_COOKIE, token, &options);
    flowie_control_dashboard_redirect(response, FLOWIE_CONTROL_DASHBOARD_PATH);
    goto done;
  }
  if (rc == TURBO_EBUSY) {
    send_text(response, TOO_MANY_REQUESTS, "Login temporarily rate limited");
    goto done;
  }
  if (rc == TURBO_ETIMEDOUT || rc == TURBO_EINTR || rc == TURBO_EIO) {
    send_text(response, SERVICE_UNAVAILABLE, "Login temporarily unavailable");
    goto done;
  }

denied:
  rc = flowie_control_dashboard_render_login(
      dashboard,
      domain && strcmp(domain, FLOWIE_CONTROL_MANAGEMENT_SYSTEM_DOMAIN) != 0, 1,
      &html, &html_size);
  if (rc == TURBO_OK) {
    reply(response, UNAUTHORIZED, "text/html; charset=utf-8", html, html_size);
    flowie_control_dashboard_html_free(html);
  } else {
    send_text(response, INTERNAL_SERVER_ERROR, "Login unavailable");
  }

done:
  if (form.count > 0u) {
    char *owned_password = (char *)flowie_control_dashboard_form_get(&form, "password");
    if (owned_password) crypto_wipe(owned_password, strlen(owned_password));
  }
  crypto_wipe(token, sizeof(token));
  flowie_control_dashboard_form_destroy(&form);
}

static void flowie_control_dashboard_password_get_handler(Req *request, Res *response) {
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  char csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u] = {0};
  char *html = NULL;
  size_t html_size = 0u;
  int rc;
  flowie_control_dashboard_login_headers(response);
  if (!dashboard || !request->security || !request->security->authenticated) {
    flowie_control_dashboard_auth_required(response, 0);
    return;
  }
  rc = flowie_control_dashboard_resolve(dashboard, request, &caller, csrf);
  if (rc != TURBO_OK) {
    crypto_wipe(csrf, sizeof(csrf));
    flowie_control_dashboard_auth_required(response, 0);
    return;
  }
  if (caller.permissions != FLOWIE_CONTROL_MANAGEMENT_PASSWORD_CHANGE) {
    crypto_wipe(csrf, sizeof(csrf));
    flowie_control_dashboard_redirect(response, FLOWIE_CONTROL_DASHBOARD_PATH);
    return;
  }
  rc = flowie_control_dashboard_render_password(dashboard, csrf, &html, &html_size);
  crypto_wipe(csrf, sizeof(csrf));
  if (rc != TURBO_OK) {
    send_text(response, INTERNAL_SERVER_ERROR, "Password form unavailable");
    return;
  }
  reply(response, OK, "text/html; charset=utf-8", html, html_size);
  flowie_control_dashboard_html_free(html);
}

static void flowie_control_dashboard_password_post_handler(Req *request, Res *response) {
  static const char *const keys[] = {"csrf", "new_password", "confirm_password"};
  static const char hex[] = "0123456789abcdef";
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  flowie_control_management_caller_t caller = FLOWIE_CONTROL_MANAGEMENT_CALLER_INIT;
  flowie_control_password_change_command_t command = FLOWIE_CONTROL_PASSWORD_CHANGE_COMMAND_INIT;
  flowie_control_command_result_t result = FLOWIE_CONTROL_COMMAND_RESULT_INIT;
  flowie_control_dashboard_form_t form = {0};
  uint8_t random[16] = {0};
  char csrf[FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE + 1u] = {0};
  char request_id[sizeof("password-change-") + sizeof(random) * 2u] = "password-change-";
  char token[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {0};
  const char *submitted_csrf;
  const char *new_password;
  const char *confirm_password;
  const char *content_type;
  size_t password_size = 0u;
  cookie_options_t options = {0, "/v2/control", "Strict", true, true};
  int rc = TURBO_EPROTO;
  flowie_control_dashboard_login_headers(response);
  if (!dashboard || !request->security || !request->security->authenticated ||
      !flowie_control_dashboard_request_is_same_origin(request) || request->body_stream ||
      !request->body ||
      request->body_len == 0u || request->body_len > FLOWIE_CONTROL_DASHBOARD_BODY_MAX) {
    send_text(response, BAD_REQUEST, "Invalid password change request");
    return;
  }
  if (flowie_control_http_header_exact(request, "Content-Type", &content_type) != TURBO_OK ||
      strcmp(content_type, "application/x-www-form-urlencoded") != 0) {
    send_text(response, BAD_REQUEST, "Invalid password change request");
    return;
  }
  rc = flowie_control_dashboard_resolve(dashboard, request, &caller, csrf);
  if (rc != TURBO_OK || caller.permissions != FLOWIE_CONTROL_MANAGEMENT_PASSWORD_CHANGE) goto done;
  rc = flowie_control_dashboard_form_parse(request->body, request->body_len, &form);
  if (rc != TURBO_OK ||
      !flowie_control_dashboard_form_exact(&form, keys, sizeof(keys) / sizeof(keys[0])))
    goto done;
  submitted_csrf = flowie_control_dashboard_form_get(&form, keys[0]);
  new_password = flowie_control_dashboard_form_get(&form, keys[1]);
  confirm_password = flowie_control_dashboard_form_get(&form, keys[2]);
  password_size = new_password ? strnlen(new_password, FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX + 1u)
                               : 0u;
  if (!submitted_csrf || strlen(submitted_csrf) != FLOWIE_CONTROL_DASHBOARD_CSRF_SIZE ||
      crypto_verify64((const uint8_t *)submitted_csrf, (const uint8_t *)csrf) != 0 ||
      password_size < FLOWIE_CONTROL_HUMAN_PASSWORD_MIN_SIZE ||
      password_size > FLOWIE_CONTROL_CREDENTIAL_SECRET_MAX || !confirm_password ||
      strlen(confirm_password) != password_size ||
      memcmp(new_password, confirm_password, password_size) != 0) {
    rc = TURBO_EINVAL;
    goto done;
  }
  rc = turbo_secure_random(random, sizeof(random));
  for (size_t index = 0u; rc == TURBO_OK && index < sizeof(random); ++index) {
    request_id[sizeof("password-change-") - 1u + index * 2u] = hex[random[index] >> 4u];
    request_id[sizeof("password-change-") + index * 2u] = hex[random[index] & 0x0fu];
  }
  if (rc == TURBO_OK) {
    command.new_password = new_password;
    command.new_password_size = password_size;
    command.request_id = request_id;
    command.occurred_at = dashboard->clock(dashboard->clock_ctx);
    rc = flowie_control_management_password_change(dashboard->service, &caller, &command, &result);
  }
  if (rc == TURBO_OK) {
    if (flowie_control_http_cookie_exact(request, FLOWIE_CONTROL_MANAGEMENT_SESSION_COOKIE, token,
                                         sizeof(token)) == TURBO_OK)
      (void)dashboard->logout(dashboard->session_ctx, token);
    set_cookie(response, FLOWIE_CONTROL_MANAGEMENT_SESSION_COOKIE, "", &options);
    flowie_control_dashboard_redirect(response, FLOWIE_CONTROL_DASHBOARD_LOGIN_PATH);
  }

done:
  if (rc != TURBO_OK) send_text(response, flowie_control_dashboard_status(rc), "Password change failed");
  if (form.count > 0u) {
    char *owned = (char *)flowie_control_dashboard_form_get(&form, "new_password");
    char *confirmation = (char *)flowie_control_dashboard_form_get(&form, "confirm_password");
    if (owned) crypto_wipe(owned, strlen(owned));
    if (confirmation) crypto_wipe(confirmation, strlen(confirmation));
  }
  crypto_wipe(token, sizeof(token));
  crypto_wipe(random, sizeof(random));
  crypto_wipe(csrf, sizeof(csrf));
  crypto_wipe(request_id, sizeof(request_id));
  flowie_control_dashboard_form_destroy(&form);
}

static void flowie_control_dashboard_logout_handler(Req *request, Res *response) {
  flowie_control_dashboard_t *dashboard = flowie_control_dashboard_from_request(request);
  char token[FLOWIE_CONTROL_MANAGEMENT_SESSION_TOKEN_SIZE + 1u] = {0};
  int has_token = flowie_control_http_cookie_exact(request, FLOWIE_CONTROL_MANAGEMENT_SESSION_COOKIE,
                                                   token, sizeof(token)) == TURBO_OK;
  cookie_options_t options = {0, "/v2/control", "Strict", true, true};
  flowie_control_dashboard_headers(response);
  if (!flowie_control_dashboard_request_is_same_origin(request)) {
    crypto_wipe(token, sizeof(token));
    send_text(response, FORBIDDEN, "Invalid origin");
    return;
  }
  if (dashboard && dashboard->logout && has_token)
    (void)dashboard->logout(dashboard->session_ctx, token);
  set_cookie(response, FLOWIE_CONTROL_MANAGEMENT_SESSION_COOKIE, "", &options);
  crypto_wipe(token, sizeof(token));
  flowie_control_dashboard_redirect(response, FLOWIE_CONTROL_DASHBOARD_LOGIN_PATH);
}

int flowie_control_dashboard_create(const flowie_control_dashboard_config_t *config,
                                    flowie_control_dashboard_t **out) {
  flowie_control_dashboard_t *dashboard;
  const char *resource_directory;
  int rc;
  if (out) *out = NULL;
  if (!config || config->size < sizeof(*config) || !config->service || !config->resolve_session ||
      !config->clock || !config->login || !config->logout || !config->session_ctx ||
      config->session_ttl_seconds < 60u || config->session_ttl_seconds > INT_MAX ||
      (config->login_executor_enabled &&
       (config->login_executor_workers == 0u ||
        config->login_executor_workers > FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_MAX_WORKERS ||
        config->login_executor_queue_capacity == 0u ||
        config->login_executor_queue_capacity >
            FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_MAX_QUEUE_CAPACITY ||
        config->login_executor_deadline_ms == 0u ||
        config->login_executor_deadline_ms >
            FLOWIE_CONTROL_DASHBOARD_LOGIN_EXECUTOR_MAX_DEADLINE_MS)) ||
      !out)
    return TURBO_EINVAL;
  resource_directory = config->resource_directory;
#ifdef FLOWIE_CONTROL_DASHBOARD_RESOURCE_DIR
  if (!resource_directory) resource_directory = FLOWIE_CONTROL_DASHBOARD_RESOURCE_DIR;
#endif
  if (!resource_directory || resource_directory[0] == '\0') return TURBO_EINVAL;
  dashboard = (flowie_control_dashboard_t *)calloc(1u, sizeof(*dashboard));
  if (!dashboard) return TURBO_ENOMEM;
  rc = flowie_control_dashboard_view_create(resource_directory, &dashboard->view);
  if (rc != TURBO_OK) {
    free(dashboard);
    return rc;
  }
  dashboard->service = config->service;
  dashboard->resolve_session = config->resolve_session;
  dashboard->resolve_session_ctx = config->resolve_session_ctx;
  dashboard->clock = config->clock;
  dashboard->clock_ctx = config->clock_ctx;
  dashboard->login = config->login;
  dashboard->logout = config->logout;
  dashboard->session_ctx = config->session_ctx;
  dashboard->session_ttl_seconds = config->session_ttl_seconds;
  dashboard->login_executor_deadline_ms = config->login_executor_deadline_ms;
  if (config->login_executor_enabled) {
    turbo_threadpool_config_t executor_config = {(int)config->login_executor_workers,
                                                 config->login_executor_queue_capacity};
    dashboard->login_executor = turbo_threadpool_create_with_config(&executor_config);
    if (!dashboard->login_executor) {
      flowie_control_dashboard_view_destroy(dashboard->view);
      memset(dashboard, 0, sizeof(*dashboard));
      free(dashboard);
      return TURBO_ENOMEM;
    }
  }
  *out = dashboard;
  return TURBO_OK;
}

int flowie_control_dashboard_bind(flowie_control_dashboard_t *dashboard, iris_app_t *app) {
  size_t bound = 0u;
  if (!dashboard || !app || dashboard->bound_app) return TURBO_EINVAL;
  for (size_t index = 0u;
       index < sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES) / sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES[0]);
       ++index) {
    if (iris_app_lookup_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ROUTES[index]))
      return TURBO_EBUSY;
  }
  for (; bound < sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES) /
                         sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES[0]);
       ++bound) {
    if (iris_app_bind_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ROUTES[bound], dashboard) != 0)
      break;
  }
  if (bound != sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES) /
                   sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES[0])) {
    while (bound > 0u) {
      --bound;
      (void)iris_app_unbind_rpc_context(app, FLOWIE_CONTROL_DASHBOARD_ROUTES[bound], dashboard);
    }
    return TURBO_EBUSY;
  }
  dashboard->bound_app = app;
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_PATH, flowie_control_dashboard_shell_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_USERS_PATH, flowie_control_dashboard_shell_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_GROUPS_PATH, flowie_control_dashboard_shell_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_ROLES_PATH, flowie_control_dashboard_shell_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_ACLS_PATH, flowie_control_dashboard_shell_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_AUDIT_PATH, flowie_control_dashboard_shell_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_CONTENT_PATH, flowie_control_dashboard_content_handler);
  iris_app_post(app, FLOWIE_CONTROL_DASHBOARD_ACTION_PATH, flowie_control_dashboard_post_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_CSS_PATH, flowie_control_dashboard_css_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_JS_PATH, flowie_control_dashboard_js_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_HTMX_PATH, flowie_control_dashboard_htmx_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_LOGIN_PATH,
               flowie_control_dashboard_login_get_handler);
  iris_app_post(app, FLOWIE_CONTROL_DASHBOARD_LOGIN_PATH,
                flowie_control_dashboard_login_post_handler);
  iris_app_get(app, FLOWIE_CONTROL_DASHBOARD_PASSWORD_PATH,
               flowie_control_dashboard_password_get_handler);
  iris_app_post(app, FLOWIE_CONTROL_DASHBOARD_PASSWORD_PATH,
                flowie_control_dashboard_password_post_handler);
  iris_app_post(app, FLOWIE_CONTROL_DASHBOARD_LOGOUT_PATH,
                flowie_control_dashboard_logout_handler);
  return TURBO_OK;
}

void flowie_control_dashboard_unbind(flowie_control_dashboard_t *dashboard) {
  if (!dashboard || !dashboard->bound_app) return;
  for (size_t index = sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES) /
                          sizeof(FLOWIE_CONTROL_DASHBOARD_ROUTES[0]);
       index > 0u; --index) {
    (void)iris_app_unbind_rpc_context(dashboard->bound_app,
                                      FLOWIE_CONTROL_DASHBOARD_ROUTES[index - 1u], dashboard);
  }
  dashboard->bound_app = NULL;
}

void flowie_control_dashboard_destroy(flowie_control_dashboard_t *dashboard) {
  if (!dashboard) return;
  flowie_control_dashboard_unbind(dashboard);
  turbo_threadpool_destroy(dashboard->login_executor);
  dashboard->login_executor = NULL;
  flowie_control_dashboard_view_destroy(dashboard->view);
  memset(dashboard, 0, sizeof(*dashboard));
  free(dashboard);
}
