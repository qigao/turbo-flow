cmake_add_test(
  test_flowie
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie.c
  LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)

set_tests_properties(test_flowie PROPERTIES LABELS "flowie;flowie-release")

cmake_add_test(
  test_flowie_topic_index
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_topic_index.c
  LIBS Flowie::App TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie_topic_index PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
set_tests_properties(test_flowie_topic_index PROPERTIES LABELS "flowie;flowie-release")

cmake_add_test(
  test_flowie_ingress
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_ingress.c
  LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie_ingress PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)

set_tests_properties(test_flowie_ingress PROPERTIES LABELS "flowie;flowie-release")

cmake_add_test(
  test_flowie_endpoint
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_endpoint.c
  LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie_endpoint PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

set_tests_properties(test_flowie_endpoint PROPERTIES LABELS "flowie;flowie-release")
target_compile_definitions(test_flowie_endpoint PRIVATE FLOWIE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/..")

cmake_add_test(
  test_flowie_transport
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_transport.c
  LIBS Flowie::App Flowie::Client Flowie::Protocol TurboFlow::Flow TurboNet::CoroNet
       TurboUtils::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/io/socket/tests
  FOLDER "flowie/tests")

set_tests_properties(test_flowie_transport
                     PROPERTIES LABELS "flowie;flowie-release;transport-contract"
                                TIMEOUT 60)

if(WIN32)
  target_link_libraries(test_flowie_endpoint PRIVATE ws2_32)
  target_link_libraries(test_flowie_transport PRIVATE ws2_32)
endif()
