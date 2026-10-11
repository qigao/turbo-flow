#ifndef TURBO_FLOW_PLUGIN_ABI_H
#define TURBO_FLOW_PLUGIN_ABI_H

/*
 * Shared PluginHost ABI version.
 *
 * Host and plugin descriptors use exact size/major/minor matching. ABI 3.4
 * replaces operation resolved-config requests with exact deployment resource
 * Interfaces and adds the operation's resource requirement. ABI 3.3 modules
 * must be rebuilt; old layouts are rejected rather than zero-extended.
 */
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR 3u
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR 4u

#endif /* TURBO_FLOW_PLUGIN_ABI_H */
