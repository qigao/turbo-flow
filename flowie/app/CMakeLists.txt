if(NOT TARGET flowie_control_runtime)
  message(FATAL_ERROR
          "flowie_server requires the Auth/ACL Control runtime (SQLite, HTTP server, and RPC)")
endif()

add_library(flowie_server_application STATIC
            flowie_server_application.c
            flowie_server_application_internal.h
            flowie_worker_runtime.c
            flowie_worker_runtime_internal.h)
cmake_config_target(flowie_server_application FOLDER "flowie/app")
target_link_libraries(flowie_server_application PUBLIC Flowie::App TurboFlow::Socket
                                                       TurboFlow::LocalStorage TurboFlow::Redis
                                                       TurboFlow::PostgreSQL TurboFlow::HttpClient
                                                       TurboFlow::HttpServer flowie_control_runtime
                                                       TurboUtils::Core)

add_executable(flowie_server flowie_server.c)
cmake_config_target(flowie_server FOLDER "flowie/app")
target_link_libraries(flowie_server PRIVATE flowie_server_application TurboUtils::Core)
cmake_target_enable_boringssl_linkage(flowie_server)

add_executable(flowie_supervisor flowie_supervisor.c flowie_supervisor_runtime.c
                                 flowie_supervisor_runtime_internal.h)
cmake_config_target(flowie_supervisor FOLDER "flowie/app")
target_link_libraries(flowie_supervisor PRIVATE TurboUtils::Core)

install(TARGETS flowie_server flowie_supervisor RUNTIME DESTINATION bin)
install(FILES "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.yml"
              "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow"
        DESTINATION share/turboflow/examples/flowie)
install(DIRECTORY "${PROJECT_SOURCE_DIR}/flowie/deploy/nginx/"
        DESTINATION share/turboflow/deploy/flowie-nginx)
install(FILES "${PROJECT_SOURCE_DIR}/flowie/deploy/provision-root.ps1"
        DESTINATION share/turboflow/deploy)
install(
  FILES "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-dev.yml"
        "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-dev.flow"
        "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-smb.yml"
        "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-smb.flow"
  DESTINATION share/turboflow/examples/flowie/products)

if(BUILD_TESTS)
  if(FLOWIE_MQTT_FIXED_INTEROP_TESTS)
    find_program(FLOWIE_MOSQUITTO_PUB_EXECUTABLE NAMES mosquitto_pub REQUIRED)
    find_program(FLOWIE_MOSQUITTO_SUB_EXECUTABLE NAMES mosquitto_sub REQUIRED)
  endif()

  add_library(flowie_storage_backend_plugin_invalid_api SHARED
              tests/flowie_storage_backend_plugin_invalid_api.c)
  target_include_directories(flowie_storage_backend_plugin_invalid_api PRIVATE
                             ${PROJECT_SOURCE_DIR}/flowie/app
                             ${PROJECT_SOURCE_DIR}/flowie/include
                             ${PROJECT_SOURCE_DIR}/turbo_flow/include
                             ${PROJECT_SOURCE_DIR}/io/common/storage/include)
  target_link_libraries(flowie_storage_backend_plugin_invalid_api PRIVATE TurboUtils::Core)
  if(WIN32)
    set_target_properties(flowie_storage_backend_plugin_invalid_api PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
  endif()
  add_library(flowie_storage_backend_plugin_duplicate_redis SHARED
              tests/flowie_storage_backend_plugin_duplicate_redis.c)
  target_include_directories(flowie_storage_backend_plugin_duplicate_redis PRIVATE
                             ${PROJECT_SOURCE_DIR}/flowie/app
                             ${PROJECT_SOURCE_DIR}/flowie/include
                             ${PROJECT_SOURCE_DIR}/turbo_flow/include
                             ${PROJECT_SOURCE_DIR}/io/common/storage/include)
  target_link_libraries(flowie_storage_backend_plugin_duplicate_redis PRIVATE TurboUtils::Core)
  if(WIN32)
    set_target_properties(flowie_storage_backend_plugin_duplicate_redis PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS ON)
  endif()

  add_executable(flowie_supervisor_test_worker tests/flowie_supervisor_test_worker.c)
  target_link_libraries(flowie_supervisor_test_worker PRIVATE TurboUtils::Core)
  cmake_config_target(flowie_supervisor_test_worker NO_INSTALL FOLDER "flowie/app/tests")

  cmake_add_test(
    test_flowie_supervisor
    SOURCES tests/test_flowie_supervisor.c flowie_supervisor_runtime.c
            flowie_supervisor_runtime_internal.h
    LIBS TurboUtils::Core TurboUtils::TinyTest
    INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}
    FOLDER "flowie/app/tests")
  target_compile_definitions(
    test_flowie_supervisor
    PRIVATE FLOWIE_TEST_WORKER_PROGRAM="$<TARGET_FILE:flowie_server>"
            FLOWIE_TEST_LONG_RUNNING_WORKER_PROGRAM="$<TARGET_FILE:flowie_supervisor_test_worker>"
            FLOWIE_TEST_CONFIG_PATH="${PROJECT_SOURCE_DIR}/flowie/examples/flowie.yml"
            FLOWIE_TEST_GRAPH_PATH="${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow")
  add_dependencies(test_flowie_supervisor flowie_server flowie_supervisor_test_worker)
  set_tests_properties(test_flowie_supervisor
                       PROPERTIES LABELS "flowie;flowie-release;product-contract")

  cmake_add_test(
    test_flowie_server_application
    SOURCES tests/test_flowie_server_application.c
    LIBS flowie_server_application TurboUtils::TinyTest
    INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/io/socket/tests
             ${PROJECT_SOURCE_DIR}/flowie/tests
    FOLDER "flowie/app/tests")
  target_compile_definitions(
    test_flowie_server_application
    PRIVATE FLOWIE_TEST_GRAPH_PATH="${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_no_policy.flow")
  cmake_target_enable_boringssl_linkage(test_flowie_server_application)
  if(WIN32)
    target_link_libraries(test_flowie_server_application PRIVATE ws2_32)
  endif()
  set_tests_properties(
    test_flowie_server_application
    PROPERTIES LABELS "flowie;flowie-release;product-contract;security-contract;integration"
               TIMEOUT 30)

  add_test(
    NAME flowie_supervisor_check
    COMMAND flowie_supervisor --check --capture-output 1048576
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.yml"
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow")
  set_tests_properties(flowie_supervisor_check
                       PROPERTIES LABELS "flowie;flowie-release;product-contract")

  add_test(
    NAME flowie_supervisor_require_security_without_policy
    COMMAND flowie_supervisor --check --require-security --capture-output 1048576
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_no_policy.yml"
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_no_policy.flow")
  set_tests_properties(
    flowie_supervisor_require_security_without_policy
    PROPERTIES WILL_FAIL TRUE
               LABELS "flowie;flowie-release;product-contract;security-contract")

  add_test(
    NAME flowie_supervisor_output_limit
    COMMAND flowie_supervisor --check --capture-output 1
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.yml"
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow")
  set_tests_properties(
    flowie_supervisor_output_limit
    PROPERTIES WILL_FAIL TRUE
               LABELS "flowie;flowie-release;product-contract;resource-limit")

  add_test(
    NAME flowie_server_check
    COMMAND flowie_server --check
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.yml"
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow")
  set_tests_properties(flowie_server_check
                       PROPERTIES LABELS "flowie;flowie-release;product-contract")

  add_test(
    NAME flowie_server_check_without_policy
    COMMAND flowie_server --check
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_no_policy.yml"
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_no_policy.flow")
  set_tests_properties(flowie_server_check_without_policy
                       PROPERTIES LABELS "flowie;flowie-release;product-contract")

  add_test(
    NAME flowie_server_require_security_without_policy
    COMMAND flowie_server --check --require-security
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_no_policy.yml"
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_no_policy.flow")
  set_tests_properties(
    flowie_server_require_security_without_policy
    PROPERTIES WILL_FAIL TRUE
               LABELS "flowie;flowie-release;product-contract;security-contract")

  add_test(
    NAME flowie_server_check_dev_product
    COMMAND flowie_server --check
            "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-dev.yml"
            "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-dev.flow")
  set_tests_properties(flowie_server_check_dev_product
                       PROPERTIES LABELS "flowie;flowie-release;product-contract")

  cmake_add_test(
    test_flowie_dev_server_bdd
    SOURCES tests/test_flowie_dev_server_bdd.c
    LIBS Flowie::Client TurboUtils::Core TurboUtils::TinyTest
    INCLUDES ${PROJECT_SOURCE_DIR}/flowie/tests
    FOLDER "flowie/app/tests")
  target_compile_definitions(
    test_flowie_dev_server_bdd
    PRIVATE FLOWIE_DEV_SERVER_EXECUTABLE="$<TARGET_FILE:flowie_server>"
            FLOWIE_DEV_CONFIG_PATH="${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-dev.yml"
            FLOWIE_DEV_GRAPH_PATH="${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-dev.flow")
  if(WIN32)
    target_link_libraries(test_flowie_dev_server_bdd PRIVATE ws2_32)
  endif()
  add_dependencies(test_flowie_dev_server_bdd flowie_server)
  set_tests_properties(
    test_flowie_dev_server_bdd
    PROPERTIES LABELS "flowie;flowie-release;product-contract;integration" TIMEOUT 30)

  if(FLOWIE_MQTT_FIXED_INTEROP_TESTS)
    cmake_add_test(
      test_flowie_mosquitto_interop
      SOURCES tests/test_flowie_mosquitto_interop.c
      LIBS TurboUtils::Core TurboUtils::TinyTest
      INCLUDES ${PROJECT_SOURCE_DIR}/flowie/tests
      FOLDER "flowie/app/tests")
    target_compile_definitions(
      test_flowie_mosquitto_interop
      PRIVATE FLOWIE_MOSQUITTO_PUB="${FLOWIE_MOSQUITTO_PUB_EXECUTABLE}"
              FLOWIE_MOSQUITTO_SUB="${FLOWIE_MOSQUITTO_SUB_EXECUTABLE}"
              FLOWIE_DEV_SERVER_EXECUTABLE="$<TARGET_FILE:flowie_server>"
              FLOWIE_DEV_CONFIG_PATH="${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-dev.yml"
              FLOWIE_DEV_GRAPH_PATH="${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-dev.flow")
    add_dependencies(test_flowie_mosquitto_interop flowie_server)
    set_tests_properties(
      test_flowie_mosquitto_interop
      PROPERTIES LABELS
                 "flowie;flowie-release;mqtt-interop;mqtt-interop-flowie;mqtt-persistence"
                 TIMEOUT 300)

    cmake_add_test(
      test_flowie_mosquitto_fixed_interop
      SOURCES tests/test_flowie_mosquitto_interop.c
      LIBS TurboUtils::Core TurboUtils::TinyTest
      INCLUDES ${PROJECT_SOURCE_DIR}/flowie/tests
      FOLDER "flowie/app/tests")
    target_compile_definitions(
      test_flowie_mosquitto_fixed_interop
      PRIVATE FLOWIE_INTEROP_EXTERNAL_BROKER=1
              FLOWIE_MOSQUITTO_PUB="${FLOWIE_MOSQUITTO_PUB_EXECUTABLE}"
              FLOWIE_MOSQUITTO_SUB="${FLOWIE_MOSQUITTO_SUB_EXECUTABLE}"
              FLOWIE_INTEROP_BROKER_HOST="${FLOWIE_MQTT_FIXED_HOST}"
              FLOWIE_INTEROP_BROKER_PORT=${FLOWIE_MQTT_FIXED_TCP_PORT})
    set_tests_properties(
      test_flowie_mosquitto_fixed_interop
      PROPERTIES LABELS
                 "flowie;flowie-release;mqtt-interop;mqtt-fixed-interop;mqtt-persistence"
                 TIMEOUT 300)
    if(WIN32)
      target_link_libraries(test_flowie_mosquitto_interop PRIVATE ws2_32)
      target_link_libraries(test_flowie_mosquitto_fixed_interop PRIVATE ws2_32)
    endif()
  endif()

  add_test(
    NAME flowie_server_check_redis_data_sink
    COMMAND flowie_server --check
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_redis_sink.yml"
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_redis_sink.flow")
  set_tests_properties(
    flowie_server_check_redis_data_sink
    PROPERTIES LABELS "flowie;flowie-release;product-contract;storage-contract")

  add_test(
    NAME flowie_server_check_redis_session_store
    COMMAND flowie_server --check
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_redis.yml"
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow")
  set_tests_properties(
    flowie_server_check_redis_session_store
    PROPERTIES LABELS
               "flowie;flowie-release;product-contract;storage-contract;redis-live")
  if(NOT TURBO_FLOW_REDIS_LIVE_TESTS)
    set_tests_properties(flowie_server_check_redis_session_store PROPERTIES DISABLED TRUE)
  endif()

  add_test(
    NAME flowie_server_check_storage_backend_plugin_duplicate_redis
    COMMAND flowie_server --check
            --storage-backend-plugin "$<TARGET_FILE:flowie_storage_backend_plugin_duplicate_redis>"
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_no_policy.yml"
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_no_policy.flow")
  set_tests_properties(
    flowie_server_check_storage_backend_plugin_duplicate_redis
    PROPERTIES WILL_FAIL TRUE
               LABELS "flowie;flowie-release;storage-contract;storage-backend-plugin")

  add_test(
    NAME flowie_server_check_storage_plugin_invalid_api
    COMMAND flowie_server --check
            --storage-backend-plugin "$<TARGET_FILE:flowie_storage_backend_plugin_invalid_api>"
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_no_policy.yml"
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_no_policy.flow")
  set_tests_properties(
    flowie_server_check_storage_plugin_invalid_api
    PROPERTIES WILL_FAIL TRUE
               LABELS "flowie;flowie-release;storage-contract;storage-backend-plugin")

  add_test(
    NAME flowie_server_check_https_auth_provider
    COMMAND flowie_server --check --require-security
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_https_secure.yml"
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow")
  set_tests_properties(
    flowie_server_check_https_auth_provider
    PROPERTIES LABELS "flowie;flowie-release;product-contract;security-contract"
               ENVIRONMENT "FLOWIE_TEST_AUTH_SERVICE_TOKEN=test-only-token")

  add_test(
    NAME flowie_server_check_smb_product
    COMMAND flowie_server --check
            "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-smb.yml"
            "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-smb.flow")
  set_tests_properties(
    flowie_server_check_smb_product
    PROPERTIES LABELS
               "flowie;flowie-release;product-contract;storage-contract;pgsql-live")
  if(NOT TURBO_FLOW_PGSQL_LIVE_TESTS)
    set_tests_properties(flowie_server_check_smb_product PROPERTIES DISABLED TRUE)
  endif()
endif()
