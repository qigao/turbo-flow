#if defined(_WIN32)
  #define FLOW_PLUGIN_FIXTURE_EXPORT __declspec(dllexport)
#elif defined(__GNUC__) && __GNUC__ >= 4
  #define FLOW_PLUGIN_FIXTURE_EXPORT __attribute__((visibility("default")))
#else
  #define FLOW_PLUGIN_FIXTURE_EXPORT
#endif

FLOW_PLUGIN_FIXTURE_EXPORT int turbo_flow_plugin_fixture_not_the_canonical_symbol(void) {
  return 1;
}
