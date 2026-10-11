#ifndef TURBO_FLOW_RULESFORGE_RESOURCE_H
#define TURBO_FLOW_RULESFORGE_RESOURCE_H

#include <cmeta/interface.h>
#include <rules_forge.h>

#define TURBO_FLOW_RULESFORGE_RESOURCE_CONTRACT_ID "turbo_flow.rulesforge.knowledge_base"
enum { TURBO_FLOW_RULESFORGE_RESOURCE_CONTRACT_VERSION = 1u };
#define TURBO_FLOW_RULESFORGE_RESOURCE_IMMUTABLE (UINT64_C(1) << 0)

/** Deployment-owned, fully loaded KnowledgeBase. Rule/schema/predicate mutation
 * is forbidden while leased; independent per-call sessions may share it.
 * The deployment owner keeps RulesForge initialized and all KB dependencies
 * alive until its resource module has no leases. It then destroys the KB before
 * engine cleanup. fact_type is immutable, nonempty, NUL-terminated, 1..127 bytes.
 * Operation result domains may retain this view beyond generation destruction.
 * Paths, credentials and loading policy never enter the Graph or operation request.
 */
typedef struct turbo_flow_rulesforge_resource_view_s {
  size_t size;
  ruleforge_knowledge_base_t knowledge_base;
  const char *fact_type;
} turbo_flow_rulesforge_resource_view_t;
#define TURBO_FLOW_RULESFORGE_RESOURCE_VIEW_INIT                                                   \
  {sizeof(turbo_flow_rulesforge_resource_view_t), NULL, NULL}

/** snapshot is control-thread-only and has no side effects. It fills an exact-size
 * initialized output or returns a Salts error. The returned view borrows the
 * resource lease; it neither transfers the KB nor authorizes mutation/destruction.
 * Example:
 *   turbo_flow_rulesforge_resource_view_t view = TURBO_FLOW_RULESFORGE_RESOURCE_VIEW_INIT;
 *   int rc = turbo_flow_rulesforge_resource_snapshot(&handle, &view);
 *   // Only after rc == SALTS_OK, use view.knowledge_base under the resource lease.
 */
#define TURBO_FLOW_RULESFORGE_RESOURCE_METHODS(X, I)                                               \
  X(I, R1, int, snapshot, turbo_flow_rulesforge_resource_view_t *, view_out)
CMETA_INTERFACE(turbo_flow_rulesforge_resource, TURBO_FLOW_RULESFORGE_RESOURCE_METHODS);

#endif
