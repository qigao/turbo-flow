#if defined(_WIN32)
#define FIXTURE_EXPORT __declspec(dllexport)
#else
#define FIXTURE_EXPORT
#endif
FIXTURE_EXPORT int closure_leaf_value(void) { return 7; }
