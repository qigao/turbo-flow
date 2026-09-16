#if defined(_WIN32)
#define FIXTURE_EXPORT __declspec(dllexport)
#else
#define FIXTURE_EXPORT
#endif
extern int closure_mid_value(void);
FIXTURE_EXPORT int closure_root_value(void) { return closure_mid_value(); }
