#ifndef TURBO_FLOW_PLUGIN_ABI_H
#define TURBO_FLOW_PLUGIN_ABI_H

/*
 * Shared PluginHost ABI version.
 *
 * Host and plugin descriptors use exact size/major/minor matching. ABI 3.1 adds
 * the typed materializer registration surface; ABI 3.0 DLLs are therefore
 * rejected explicitly rather than zero-extended across the boundary.
 */
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MAJOR 3u
#define TURBO_FLOW_PLUGIN_ABI_VERSION_MINOR 1u

#endif /* TURBO_FLOW_PLUGIN_ABI_H */
