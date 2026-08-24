include(CMakeDependentOption)

set(CMAKE_COLOR_DIAGNOSTICS ON)

# building the tests
option(ENABLE_TESTS "Enable the tests" ON)

# Address Sanitizer - only enabled for Debug builds
cmake_dependent_option(ENABLE_ASAN "Enable Address Sanitizer" ON
                       "CMAKE_BUILD_TYPE STREQUAL Debug" OFF)


# if(MSVC) add_compile_options(/bigobj) endif()

option(BUILD_EXAMPLES "Build example programs" ON)
option(BUILD_TESTS "Build test suite" ON)
option(BUILD_BENCHMARKS "Build benchmark suite" ON) 

option(TURBO_FLOW_REDIS_LIVE_TESTS
       "Enable Redis integration tests against 127.0.0.1:6379" OFF)
option(TURBO_FLOW_PROTOCOL_TRANSPORT_SOAK_TESTS
       "Register scheduled protocol-ingress CoroNet transport soak tests" OFF)

option(ENABLE_SANITIZER_ADDRESS "Enable AddressSanitizer" OFF)
option(ENABLE_SANITIZER_UNDEFINED "Enable UndefinedBehaviorSanitizer" OFF)
option(ENABLE_SANITIZER_LEAK "Enable LeakSanitizer" OFF)
option(ENABLE_SANITIZER_THREAD "Enable ThreadSanitizer" OFF)
option(ENABLE_SANITIZER_MEMORY "Enable MemorySanitizer (Clang only)" OFF)

set_property(GLOBAL PROPERTY USE_FOLDERS ON)
