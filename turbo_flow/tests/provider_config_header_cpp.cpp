#include "provider_config_native.h"

#include <cstddef>

extern "C" size_t flow_provider_config_cpp_policy_size(void) {
  return sizeof(HeaderPolicy_t);
}

extern "C" size_t flow_provider_config_cpp_headers_offset(void) {
  return offsetof(HeaderPolicy_t, headers);
}

extern "C" size_t flow_provider_config_cpp_header_size(void) {
  return sizeof(StaticHeader_t);
}
