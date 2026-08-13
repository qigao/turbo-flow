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
option(TURBO_FLOW_PGSQL_LIVE_TESTS
       "Enable PostgreSQL integration tests using TURBO_FLOW_PGSQL_TEST_CONNINFO" OFF)
option(GATEWAY_TRANSPORT_SOAK_TESTS
       "Register scheduled gateway CoroNet transport soak tests" OFF)

set_property(GLOBAL PROPERTY USE_FOLDERS ON)
