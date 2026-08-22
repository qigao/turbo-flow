#ifndef TURBO_FLOW_EXPORT_H
#define TURBO_FLOW_EXPORT_H

/* TurboFlow owns its ABI marker. TurboUtils' TURBO_API only describes
 * TurboUtils::Core and must not leak its producer/consumer state here. */
#ifndef TURBO_FLOW_API
#  if defined(_WIN32) && defined(TURBO_FLOW_BUILD)
#    define TURBO_FLOW_API __declspec(dllexport)
#  elif !defined(_WIN32) && defined(__GNUC__) && __GNUC__ >= 4
#    define TURBO_FLOW_API __attribute__((visibility("default")))
#  else
#    define TURBO_FLOW_API
#  endif
#endif

#ifndef TURBO_FLOW_C_API
#  ifdef __cplusplus
#    define TURBO_FLOW_C_API extern "C" TURBO_FLOW_API
#  else
#    define TURBO_FLOW_C_API TURBO_FLOW_API
#  endif
#endif

#endif /* TURBO_FLOW_EXPORT_H */
