#include "turbo_flow_plugin.h"
#include "turbo_flow_plugin_protocol_mapper.h"

#include <type_traits>

static_assert(std::is_standard_layout<turbo_flow_plugin_protocol_mapper_catalog_entry_v1_t>::value,
              "protocol mapper catalog entry must remain standard-layout");
static_assert(std::is_standard_layout<turbo_flow_plugin_protocol_mapper_catalog_v1_t>::value,
              "protocol mapper catalog must remain standard-layout");
static_assert(offsetof(turbo_flow_plugin_host_config_t, protocol_mapper_capacity) >
                  offsetof(turbo_flow_plugin_host_config_t, materializer_capacity),
              "mapper capacity must remain an append-only host-config tail field");
static_assert(offsetof(turbo_flow_plugin_registration_v1_t, add_protocol_mapper) >
                  offsetof(turbo_flow_plugin_registration_v1_t, add_materializer),
              "mapper registration must remain an append-only registration tail field");
