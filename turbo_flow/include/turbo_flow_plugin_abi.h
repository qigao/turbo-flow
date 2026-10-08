#ifndef TURBO_FLOW_PLUGIN_ABI_H
#define TURBO_FLOW_PLUGIN_ABI_H

/*
 * Shared PluginHost ABI version.
 *
 * Host and plugin descriptors use exact size/major/minor matching. ABI 3.3
 * makes the published Salts Component generation an explicit provider-lifetime
 * input to Plugin generation configuration. ABI 3.2 callers are rejected
 * explicitly rather than zero-extended across the boundary.
 */
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR 3u
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR 3u

#endif /* TURBO_FLOW_PLUGIN_ABI_H */
