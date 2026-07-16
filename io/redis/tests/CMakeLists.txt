cmake_add_test(
  test_turbo_flow_redis
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_redis.c
  LIBS TurboFlow::Redis TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "io/redis/tests")

target_compile_definitions(
  test_turbo_flow_redis PRIVATE TURBO_FLOW_REDIS_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/..")

cmake_add_test(
  test_turbo_flow_redis_live
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_turbo_flow_redis_live.c
  LIBS TurboFlow::Redis TurboFlow::FMQ TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "io/redis/tests")

if(TARGET Flowie::App)
  target_link_libraries(test_turbo_flow_redis_live PRIVATE Flowie::App Flowie::Protocol)
  target_include_directories(test_turbo_flow_redis_live PRIVATE ${PROJECT_SOURCE_DIR}/flowie/tests)
  target_compile_definitions(test_turbo_flow_redis_live PRIVATE TURBO_FLOW_REDIS_HAS_FLOWIE=1)
endif()

set_tests_properties(test_turbo_flow_redis PROPERTIES LABELS "fmq-release;storage-contract")
set_tests_properties(
  test_turbo_flow_redis_live
  PROPERTIES LABELS "fmq-release;fmq-chaos;integration;redis-live")
if(NOT TURBO_FLOW_REDIS_LIVE_TESTS)
  set_tests_properties(test_turbo_flow_redis_live PROPERTIES DISABLED TRUE)
endif()

if(WIN32)
  target_link_libraries(test_turbo_flow_redis PRIVATE ws2_32)
  target_link_libraries(test_turbo_flow_redis_live PRIVATE ws2_32)
endif()

if(WIN32)
  set_tests_properties(
    test_turbo_flow_redis test_turbo_flow_redis_live
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboNet::Redis>;PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboNet::CoroNet>;PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboUtils::Core>;PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboUtils::Parser>")
endif()

