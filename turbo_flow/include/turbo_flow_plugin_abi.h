#ifndef TURBO_FLOW_PLUGIN_ABI_H
#define TURBO_FLOW_PLUGIN_ABI_H

/*
 * Shared PluginHost ABI version.
 *
 * Host and plugin descriptors use exact size/major/minor matching. ABI 3.2 adds
 * the pre-durable protocol mapper registration/catalog surface; ABI 3.1 DLLs
 * are therefore rejected explicitly rather than zero-extended across the
 * boundary.
 */
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR 3u
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR 2u

#endif /* TURBO_FLOW_PLUGIN_ABI_H */
