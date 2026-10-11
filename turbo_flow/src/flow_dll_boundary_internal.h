/*
 * Internal DLL-to-DLL entry points. Graph and Product own disjoint runtime
 * functions; PluginHost is a borrower and must import only the declared ones.
 * Do not use the global TURBO_FLOW_BUILD for this cross-target boundary.
 */
#ifndef FLOW_DLL_BOUNDARY_INTERNAL_H
#define FLOW_DLL_BOUNDARY_INTERNAL_H
#if defined(_WIN32)
  #if defined(turbo_flow_graph_EXPORTS)
    #define TURBO_FLOW_GRAPH_INTERNAL_API __declspec(dllexport)
  #else
    #define TURBO_FLOW_GRAPH_INTERNAL_API __declspec(dllimport)
  #endif
  #if defined(turbo_flow_product_EXPORTS)
    #define TURBO_FLOW_PRODUCT_INTERNAL_API __declspec(dllexport)
  #else
    #define TURBO_FLOW_PRODUCT_INTERNAL_API __declspec(dllimport)
  #endif
#elif defined(__GNUC__) && __GNUC__ >= 4
  #define TURBO_FLOW_GRAPH_INTERNAL_API __attribute__((visibility("default")))
  #define TURBO_FLOW_PRODUCT_INTERNAL_API __attribute__((visibility("default")))
#else
  #define TURBO_FLOW_GRAPH_INTERNAL_API
  #define TURBO_FLOW_PRODUCT_INTERNAL_API
#endif
#endif /* FLOW_DLL_BOUNDARY_INTERNAL_H */
