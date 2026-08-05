#include "flowie_control_acl_internal.h"

#include "tinytest.h"
#include "turbo_error.h"

#include <string.h>

spec("Flowie Control user ACL grammar") {
  it("parses and canonically formats one user parent with bounded topic entries") {
    static const char input[] =
        "user 4fc55867-cdb8-4458-9ba7-afca8e4e2867 allow {\n"
        "write topic booth/groups/china/east/operators/devices/%u/{event,heartbeat,process}\n"
        "read topic booth/groups/china/east/operators/devices/%c/{command,payment}\n"
        "deny readwrite topic booth/groups/china/east/operators/devices/%u/private\n"
        "}";
    static const char canonical[] =
        "user 4fc55867-cdb8-4458-9ba7-afca8e4e2867 allow {\n"
        "  write topic booth/groups/china/east/operators/devices/%u/{event,heartbeat,process}\n"
        "  read topic booth/groups/china/east/operators/devices/%c/{command,payment}\n"
        "  deny readwrite topic booth/groups/china/east/operators/devices/%u/private\n"
        "}";
    flowie_control_acl_document_t document = FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
    char formatted[FLOWIE_CONTROL_ACL_DOCUMENT_MAX + 1u];
    size_t formatted_size = 0u;

    check_int_eq(flowie_control_acl_parse(input, sizeof(input) - 1u, &document), TURBO_OK);
    check_str_eq(document.subject, "4fc55867-cdb8-4458-9ba7-afca8e4e2867");
    check_int_eq(document.connection_effect, TURBO_FLOW_SECURITY_ALLOW);
    check_size_eq(document.entry_count, 3u);
    check_uint_eq(document.entries[0].action_mask, TURBO_FLOW_SECURITY_ACTION_PUBLISH);
    check_uint_eq(document.entries[0].group_count, 3u);
    check_uint_eq(document.entries[0].alternative_count, 3u);
    check_true(document.entries[0].uses_username);
    check_false(document.entries[0].uses_client_id);
    check_uint_eq(document.entries[1].action_mask, TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE);
    check_true(document.entries[1].uses_client_id);
    check_int_eq(document.entries[2].effect, TURBO_FLOW_SECURITY_DENY);
    check_uint_eq(document.entries[2].action_mask,
                  TURBO_FLOW_SECURITY_ACTION_PUBLISH |
                      TURBO_FLOW_SECURITY_ACTION_SUBSCRIBE);
    check_int_eq(flowie_control_acl_format(&document, formatted, sizeof(formatted),
                                           &formatted_size),
                 TURBO_OK);
    check_size_eq(formatted_size, sizeof(canonical) - 1u);
    check_str_eq(formatted, canonical);
  }

  it("compiles connection state substitutions and alternatives into bounded internal rules") {
    static const char input[] =
        "user device-1 allow {"
        "write topic root-a/groups/region/operators/devices/%u/{event,heartbeat} "
        "read topic root-a/groups/region/operators/devices/%c/command"
        "}";
    flowie_control_acl_document_t document = FLOWIE_CONTROL_ACL_DOCUMENT_INIT;
    turbo_flow_security_rule_t rules[4] = {TURBO_FLOW_SECURITY_RULE_INIT,
                                           TURBO_FLOW_SECURITY_RULE_INIT,
                                           TURBO_FLOW_SECURITY_RULE_INIT,
                                           TURBO_FLOW_SECURITY_RULE_INIT};
    size_t count = 0u;

    check_int_eq(flowie_control_acl_parse(input, sizeof(input) - 1u, &document), TURBO_OK);
    check_int_eq(flowie_control_acl_compile(&document, "root-a", rules, 4u, &count), TURBO_OK);
    check_size_eq(count, 4u);
    check_uint_eq(rules[0].action_mask, TURBO_FLOW_SECURITY_ACTION_CONNECT);
    check_int_eq(rules[0].resource_type, TURBO_FLOW_SECURITY_RESOURCE_GENERIC);
    check_int_eq(rules[0].match_kind, TURBO_FLOW_SECURITY_MATCH_PREFIX);
    check_str_eq(rules[0].pattern, "");
    check_str_eq(rules[1].pattern,
                 "root-a/groups/region/operators/devices/%u/event");
    check_str_eq(rules[2].pattern,
                 "root-a/groups/region/operators/devices/%u/heartbeat");
    check_str_eq(rules[3].pattern,
                 "root-a/groups/region/operators/devices/%c/command");
  }

  it("rejects malformed trees duplicate alternatives denied blocks and capacity overflow") {
    static const char bad_tree[] =
        "user device-1 allow { read topic root-a/operators/device-1/event }";
    static const char duplicate_alternative[] =
        "user device-1 allow { read topic root-a/groups/operators/devices/%u/{event,event} }";
    static const char denied_block[] =
        "user device-1 deny { read topic root-a/groups/operators/devices/%u/event }";
    flowie_control_acl_document_t document = FLOWIE_CONTROL_ACL_DOCUMENT_INIT;

    check_int_eq(flowie_control_acl_parse(bad_tree, sizeof(bad_tree) - 1u, &document),
                 TURBO_EPROTO);
    check_int_eq(flowie_control_acl_parse(duplicate_alternative,
                                          sizeof(duplicate_alternative) - 1u, &document),
                 TURBO_EPROTO);
    check_int_eq(flowie_control_acl_parse(denied_block, sizeof(denied_block) - 1u, &document),
                 TURBO_EPROTO);
  }
}
