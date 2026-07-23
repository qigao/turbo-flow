cmake_add_test(
  test_socket
  SOURCES
  ${CMAKE_CURRENT_SOURCE_DIR}/test_socket.c
  LIBS
  TurboFlow::Socket
  TurboFlow::Flow
  TurboNet::CoroNet
  TurboUtils::TinyTest
  FOLDER
  "io/socket/tests")

set_tests_properties(test_socket PROPERTIES LABELS "fmq-release;fmq-chaos")

find_package(OpenSSL REQUIRED)
target_link_libraries(test_socket PRIVATE OpenSSL::SSL
                                                      OpenSSL::Crypto)
cmake_target_enable_boringssl_linkage(test_socket)

if(WIN32)
  target_link_libraries(test_socket PRIVATE ws2_32)
endif()

