#ifndef TURBO_FLOW_PLUGIN_ABI_H
#define TURBO_FLOW_PLUGIN_ABI_H

/*
 * Shared PluginHost ABI version.
 *
 * Host and plugin descriptors use exact size/major/minor matching. ABI 3.5
 * removes the resolved-config argument from generation creation. Hosts and
 * modules must be rebuilt together; old descriptors are rejected rather than
 * zero-extended. Operation resources remain explicit leased Interfaces.
 */
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR 3u
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR 5u

#endif /* TURBO_FLOW_PLUGIN_ABI_H */
