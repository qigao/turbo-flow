include(CMakeDependentOption)

set(CMAKE_COLOR_DIAGNOSTICS ON)

# building the tests
option(ENABLE_TESTS "Enable the tests" ON)

# Address Sanitizer - only enabled for Debug builds
cmake_dependent_option(ENABLE_ASAN "Enable Address Sanitizer" ON
                       "CMAKE_BUILD_TYPE STREQUAL Debug" OFF)

# SSL support
option(ENABLE_SSL "Enable SSL support" ON)
cmake_dependent_option(USE_OPENSSL "Use OpenSSL" ON
                       "ENABLE_SSL;NOT USE_MBEDTLS" OFF)
cmake_dependent_option(USE_MBEDTLS "Use MbedTLS" OFF
                       "ENABLE_SSL;NOT USE_OPENSSL" OFF)

if(ENABLE_SSL)
  if(USE_OPENSSL)
    set(SSL_BACKEND_USED "OpenSSL")
  elseif(USE_MBEDTLS)
    set(SSL_BACKEND_USED "MbedTLS")
  else()
    message(
      FATAL_ERROR
        "No valid SSL backend selected. Please enable either USE_OPENSSL or USE_MBEDTLS."
    )
  endif()
endif()
message(STATUS "SSL backend used: ${SSL_BACKEND_USED}")
# if(MSVC) add_compile_options(/bigobj) endif()

option(BUILD_EXAMPLES "Build example programs" ON)
option(BUILD_TESTS "Build test suite" ON)
option(BUILD_BENCHMARKS "Build benchmark suite" ON) 
option(TURBO_FLOW_EXPR_ENABLE_JIT "Enable MIR JIT expression backend" ON)
option(TURBO_FLOW_BUILD_FLOWIE "Build the Flowie MQTT application and protocol SDK" ON)

# Optional modules default to the historical full build. Disable the master
# switch for a core-only package, or keep it enabled and select components.
option(TURBO_FLOW_BUILD_ADAPTERS "Build TurboFlow optional adapter modules" ON)
cmake_dependent_option(TURBO_FLOW_BUILD_STORAGE "Build file storage adapters" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_CODEC "Build codec and DataBind adapters" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_SOCKET "Build generic CoroNet socket adapter" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_HTTP_CLIENT "Build TurboHTTP client adapter" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_HTTP_SERVER "Build Iris HTTP server adapter" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_RPC "Build TurboHTTP RPC adapters" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_S3 "Build TurboHTTP S3 adapter" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_EMAIL "Build TurboNet SMTP adapter" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_FMQ "Build the FlowMQ broker and TurboFlow connector" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
option(TURBO_FLOW_BUILD_FMQ_PROTOCOL
       "Build the standalone FlowMQ v2 protocol SDK without broker dependencies" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_REDIS "Build TurboNet Redis Streams adapters" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_PGSQL "Build PostgreSQL sink adapter" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_OBSERVE "Build opt-in metrics and summary sink" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_SCHEDULE "Build interval, one-shot, and cron sources" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_QUEUE "Build bounded in-memory queue adapters" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
option(TURBO_FLOW_BUILD_PROFILE_TESTS
       "Test nested core-only and representative component builds" ON)
option(TURBO_FLOW_REDIS_LIVE_TESTS
       "Enable Redis integration tests against 127.0.0.1:6379" OFF)
option(TURBO_FLOW_PGSQL_LIVE_TESTS
       "Enable PostgreSQL outbox integration tests using TURBO_FLOW_PGSQL_TEST_CONNINFO" OFF)
option(FLOWIE_MQTT_PUBLIC_LIVE_TESTS
       "Enable Flowie MQTT client integration tests against public MQTT brokers" OFF)

set_property(GLOBAL PROPERTY USE_FOLDERS ON)
