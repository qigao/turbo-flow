add_executable(flowie_server flowie_server.c flowie_worker_runtime.c
                             flowie_worker_runtime_internal.h)
cmake_config_target(flowie_server FOLDER "flowie/app")
target_link_libraries(flowie_server PRIVATE Flowie::App TurboFlow::Queue TurboFlow::Socket
                                            TurboUtils::Core)
if(TARGET TurboFlow::Redis)
  target_link_libraries(flowie_server PRIVATE TurboFlow::Redis)
  target_compile_definitions(flowie_server PRIVATE FLOWIE_SERVER_HAVE_REDIS=1)
endif()

add_executable(flowie_supervisor flowie_supervisor.c flowie_supervisor_runtime.c
                                 flowie_supervisor_runtime_internal.h)
cmake_config_target(flowie_supervisor FOLDER "flowie/app")
target_link_libraries(flowie_supervisor PRIVATE TurboUtils::Core)

install(TARGETS flowie_server flowie_supervisor RUNTIME DESTINATION bin)
install(FILES "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.yml"
              "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow"
        DESTINATION share/turboflow/examples/flowie)

if(BUILD_TESTS)
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
  set_tests_properties(test_flowie_supervisor PROPERTIES LABELS "flowie;product-contract")

  add_test(
    NAME flowie_supervisor_check
    COMMAND flowie_supervisor --check --capture-output 1048576
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.yml"
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow")
  set_tests_properties(flowie_supervisor_check PROPERTIES LABELS "flowie;product-contract")

  add_test(
    NAME flowie_supervisor_output_limit
    COMMAND flowie_supervisor --check --capture-output 1
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.yml"
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow")
  set_tests_properties(
    flowie_supervisor_output_limit
    PROPERTIES WILL_FAIL TRUE
               LABELS "flowie;product-contract;resource-limit")

  add_test(
    NAME flowie_server_check
    COMMAND flowie_server --check
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.yml"
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow")
  set_tests_properties(flowie_server_check PROPERTIES LABELS "flowie;product-contract")

  add_test(
    NAME flowie_server_check_sqlite_session_store
    COMMAND flowie_server --check
            "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_sqlite.yml"
            "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow")
  set_tests_properties(flowie_server_check_sqlite_session_store
                       PROPERTIES LABELS "flowie;product-contract;storage-contract")

  if(TARGET TurboFlow::Redis)
    add_test(
      NAME flowie_server_check_redis_session_store
      COMMAND flowie_server --check
              "${PROJECT_SOURCE_DIR}/flowie/app/tests/flowie_server_redis.yml"
              "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow")
    set_tests_properties(flowie_server_check_redis_session_store
                         PROPERTIES DISABLED TRUE
                                    LABELS "flowie;product-contract;storage-contract;redis-live")
  endif()
endif()
