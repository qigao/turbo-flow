cmake_add_test(
  test_flowie
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie.c
  LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)

set_tests_properties(test_flowie PROPERTIES LABELS "flowie")

cmake_add_test(
  test_flowie_topic_index
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_topic_index.c
  LIBS Flowie::App TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie_topic_index PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
set_tests_properties(test_flowie_topic_index PROPERTIES LABELS "flowie")

cmake_add_test(
  test_flowie_ingress
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_ingress.c
  LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie_ingress PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)

set_tests_properties(test_flowie_ingress PROPERTIES LABELS "flowie")

cmake_add_test(
  test_flowie_endpoint
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_endpoint.c
  LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie_endpoint PRIVATE ${CMAKE_CURRENT_SOURCE_DIR})

set_tests_properties(test_flowie_endpoint PROPERTIES LABELS "flowie")
target_compile_definitions(test_flowie_endpoint PRIVATE FLOWIE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/..")

if(WIN32)
  target_link_libraries(test_flowie_endpoint PRIVATE ws2_32)
endif()
