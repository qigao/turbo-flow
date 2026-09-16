#if defined(_WIN32)
#define FIXTURE_EXPORT __declspec(dllexport)
#else
#define FIXTURE_EXPORT
#endif
extern int closure_leaf_value(void);
FIXTURE_EXPORT int closure_mid_value(void) { return closure_leaf_value(); }
