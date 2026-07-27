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
option(TURBO_FLOW_EXPR_ENABLE_JIT "Enable MIR JIT expression backend" ON)
option(TURBO_FLOW_BUILD_FLOWIE "Build the Flowie MQTT application and protocol SDK" ON)
option(TURBO_FLOW_BUILD_STORE "Build the typed FlowStore state, index, log, and series stores" ON)

# Optional modules default to the historical full build. Disable the master
# switch for a core-only package, or keep it enabled and select components.
option(TURBO_FLOW_BUILD_ADAPTERS "Build TurboFlow optional adapter modules" ON)
cmake_dependent_option(TURBO_FLOW_BUILD_GATEWAYS
                       "Build MQTT protocol gateway ABI and six codec plugins" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_GATEWAY_BUSINESS
                       "Build schema-bound gateway business providers" ON
                       "TURBO_FLOW_BUILD_GATEWAYS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_SECURITY_SQLITE
                       "Build the embedded SQLite ACL policy provider" ON
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
       "Build the standalone FlowMQ v3 protocol SDK without broker dependencies" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_REDIS "Build TurboNet Redis Streams adapters" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_PGSQL "Build PostgreSQL sink adapter" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_OBSERVE "Build opt-in metrics and summary sink" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
cmake_dependent_option(TURBO_FLOW_BUILD_SCHEDULE "Build interval, one-shot, and cron sources" ON
                       "TURBO_FLOW_BUILD_ADAPTERS" OFF)
option(TURBO_FLOW_BUILD_PROFILE_TESTS
       "Test nested core-only and representative component builds" ON)
option(TURBO_FLOW_REDIS_LIVE_TESTS
       "Enable Redis integration tests against 127.0.0.1:6379" OFF)
option(TURBO_FLOW_PGSQL_LIVE_TESTS
       "Enable PostgreSQL outbox integration tests using TURBO_FLOW_PGSQL_TEST_CONNINFO" OFF)
option(FLOWIE_MQTT_PUBLIC_LIVE_TESTS
       "Enable optional Flowie MQTT client external-connectivity smoke tests" OFF)
option(FLOWIE_MQTT_FIXED_INTEROP_TESTS
       "Run Flowie and Mosquitto interoperability against one fixed broker" OFF)
set(FLOWIE_MQTT_FIXED_BROKER_NAME "Mosquitto-2.0.22" CACHE STRING
    "Fixed broker implementation and exact version")
set(FLOWIE_MQTT_FIXED_HOST "127.0.0.1" CACHE STRING "Fixed broker host")
set(FLOWIE_MQTT_FIXED_TCP_PORT 1883 CACHE STRING "Fixed broker MQTT TCP port")
set(FLOWIE_MQTT_FIXED_TLS_PORT 8883 CACHE STRING "Fixed broker MQTT TLS port")
set(FLOWIE_MQTT_FIXED_WS_PORT 8083 CACHE STRING "Fixed broker MQTT WS port")
set(FLOWIE_MQTT_FIXED_WSS_PORT 8084 CACHE STRING "Fixed broker MQTT WSS port")
set(FLOWIE_MQTT_FIXED_WS_PATH "/mqtt" CACHE STRING "Fixed broker MQTT WS/WSS path")
set(FLOWIE_MQTT_FIXED_CA_FILE "" CACHE FILEPATH
    "CA certificate used to verify the fixed broker TLS/WSS listeners")
option(FLOWIE_MQTT_FIXED_SUPPORT_31 "Fixed broker accepts MQTT 3.1 on TCP and TLS" ON)
option(FLOWIE_MQTT_FIXED_SUPPORT_31_WS
       "Fixed broker accepts MQTT 3.1 over WS and WSS with the mqtt subprotocol" ON)
option(FLOWIE_MQTT_SOAK_TESTS "Register scheduled Flowie MQTT soak tests" OFF)
option(GATEWAY_TRANSPORT_SOAK_TESTS
       "Register scheduled gateway CoroNet transport soak tests" OFF)
option(FLOWIE_MQTT_FUZZ_TARGETS "Build Flowie MQTT libFuzzer targets" OFF)
option(FLOWIE_MQTT_RELEASE_GATE
       "Register the strict Flowie MQTT release-manifest CTest" OFF)
set(FLOWIE_RELEASE_REVISION "" CACHE STRING
    "Immutable source revision written into Flowie release/nightly evidence")
set(FLOWIE_REDIS_SERVER_VERSION "8.8.0" CACHE STRING
    "Redis server version written into release evidence")
set(FLOWIE_MQTT_NIGHTLY_SEED "11794061671962178383" CACHE STRING
    "Reproducible non-zero seed for Flowie nightly corpus/soak/fuzz evidence")
set(FLOWIE_MQTT_FUZZ_RUNS 100000 CACHE STRING
    "Number of libFuzzer inputs executed by the Flowie nightly evidence target")
set(FLOWIE_MQTT_NIGHTLY_SOAK_SHORT_MS 1800000 CACHE STRING
    "Nightly duration for each 30-minute Flowie MQTT soak case")
set(FLOWIE_MQTT_NIGHTLY_SOAK_LONG_MS 3600000 CACHE STRING
    "Nightly duration for each 60-minute Flowie MQTT soak case")

set_property(GLOBAL PROPERTY USE_FOLDERS ON)
