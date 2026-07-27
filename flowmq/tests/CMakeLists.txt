cmake_add_test(
  test_fmq
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_fmq.c
  LIBS TurboFlow::FMQ FlowMQ::Protocol flowmq_coronet_transport TurboFlow::Flow
       tf_executor_common TurboUtils::TinyTest
  FOLDER "flowmq/tests")

cmake_add_test(
  test_fmq_bench_stats
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_fmq_bench_stats.c
  LIBS TurboUtils::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}/../benchmarks
  FOLDER "flowmq/tests")

if(TARGET TurboFlow::SecuritySQLite)
  cmake_add_test(
    test_fmq_security_owner
    SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_fmq_security_owner.c
    LIBS TurboFlow::FMQ TurboFlow::SecuritySQLite TurboFlow::Flow TurboUtils::TinyTest
    FOLDER "flowmq/tests")
  if(TARGET TurboFlow::HttpClient)
    target_link_libraries(test_fmq_security_owner PRIVATE TurboFlow::HttpClient)
    target_compile_definitions(test_fmq_security_owner PRIVATE FMQ_TEST_HAVE_HTTP_SECURITY=1)
  endif()
  set_tests_properties(test_fmq_security_owner
                       PROPERTIES LABELS "fmq-release;fmq-chaos;security-contract")
endif()

cmake_add_test(
  test_fmq_broker
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_fmq_broker.c
  LIBS TurboFlow::FMQ TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowmq/tests")

cmake_add_test(
  test_fmq_pubsub
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_fmq_pubsub.c
  LIBS TurboFlow::FMQ TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowmq/tests")

cmake_add_test(
  test_fmq_deployment
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_fmq_deployment.c
  LIBS TurboFlow::FMQ TurboUtils::TinyTest
  FOLDER "flowmq/tests")

cmake_add_test(
  test_fmq_management_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_fmq_management_protocol.c
  LIBS TurboFlow::FMQ TurboUtils::TinyTest
  FOLDER "flowmq/tests")

cmake_add_test(
  test_fmq_management
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_fmq_management.c
  LIBS TurboFlow::FMQ TurboUtils::TinyTest
  FOLDER "flowmq/tests")

set_tests_properties(
  test_fmq test_fmq_bench_stats test_fmq_broker test_fmq_pubsub
  test_fmq_deployment test_fmq_management_protocol test_fmq_management
  PROPERTIES LABELS "fmq-release;fmq-chaos")

target_sources(test_fmq PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src/fmq_config_validate.c)
target_include_directories(
  test_fmq PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src
                   ${PROJECT_SOURCE_DIR}/io/socket/tests)
target_compile_definitions(
  test_fmq PRIVATE TURBO_FLOW_FMQ_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/..")

if(WIN32)
  target_link_libraries(test_fmq PRIVATE ws2_32)
  set_tests_properties(
    test_fmq test_fmq_bench_stats test_fmq_broker test_fmq_pubsub
    test_fmq_deployment test_fmq_management
    test_fmq_management_protocol
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboNet::CoroNet>;PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboUtils::Core>;PATH=path_list_prepend:$<TARGET_FILE_DIR:TurboUtils::Parser>")
endif()
