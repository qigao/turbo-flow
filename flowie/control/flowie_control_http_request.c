#include "flowie_control_http_request_internal.h"

#include "turbo_error.h"

#include <ctype.h>
#include <string.h>

static int flowie_control_http_ascii_equal(const char *left, const char *right) {
  if (!left || !right) return 0;
  while (*left && *right) {
    if (tolower((unsigned char)*left++) != tolower((unsigned char)*right++)) return 0;
  }
  return *left == '\0' && *right == '\0';
}

int flowie_control_http_header_optional_exact(const Req *request, const char *name,
                                              const char **value_out) {
  const char *found = NULL;
  size_t count = 0u;
  if (value_out) *value_out = NULL;
  if (!request || !name || !name[0] || !value_out || request->headers.count < 0 ||
      (request->headers.count > 0 && !request->headers.items))
    return TURBO_EINVAL;
  for (int index = 0; index < request->headers.count; ++index) {
    const request_item_t *item = &request->headers.items[index];
    if (item->key && item->value && flowie_control_http_ascii_equal(item->key, name)) {
      found = item->value;
      ++count;
    }
  }
  if (count > 1u) return TURBO_EPROTO;
  *value_out = found;
  return TURBO_OK;
}

int flowie_control_http_header_exact(const Req *request, const char *name,
                                     const char **value_out) {
  int rc = flowie_control_http_header_optional_exact(request, name, value_out);
  if (rc != TURBO_OK) return rc;
  return *value_out ? TURBO_OK : TURBO_EPROTO;
}

int flowie_control_http_cookie_exact(const Req *request, const char *name, char *value_out,
                                     size_t value_capacity) {
  const char *header = NULL;
  const char *cursor;
  size_t name_size;
  size_t matches = 0u;
  if (value_out && value_capacity > 0u) value_out[0] = '\0';
  if (!request || !name || !name[0] || !value_out || value_capacity < 2u) return TURBO_EINVAL;
  if (flowie_control_http_header_exact(request, "Cookie", &header) != TURBO_OK)
    return TURBO_EPROTO;
  name_size = strlen(name);
  cursor = header;
  while (*cursor) {
    const char *end;
    const char *equals;
    const char *value;
    size_t key_size;
    size_t value_size;
    while (*cursor == ' ' || *cursor == '\t') ++cursor;
    end = strchr(cursor, ';');
    if (!end) end = cursor + strlen(cursor);
    while (end > cursor && (end[-1] == ' ' || end[-1] == '\t')) --end;
    equals = (const char *)memchr(cursor, '=', (size_t)(end - cursor));
    if (!equals || equals == cursor) return TURBO_EPROTO;
    key_size = (size_t)(equals - cursor);
    value = equals + 1;
    value_size = (size_t)(end - value);
    if (key_size == name_size && memcmp(cursor, name, name_size) == 0) {
      if (++matches != 1u || value_size == 0u || value_size >= value_capacity)
        return TURBO_EPROTO;
      memcpy(value_out, value, value_size);
      value_out[value_size] = '\0';
    }
    cursor = *end == ';' ? end + 1 : end;
  }
  return matches == 1u ? TURBO_OK : TURBO_EPROTO;
}
