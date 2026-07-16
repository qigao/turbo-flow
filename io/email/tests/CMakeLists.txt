add_executable(test_turbo_flow_email
  test_turbo_flow_email.c)

target_link_libraries(test_turbo_flow_email PRIVATE
  TurboFlow::Email
  TurboNet::CoroNet
  TurboUtils::TinyTest)

if(WIN32)
  target_link_libraries(test_turbo_flow_email PRIVATE ws2_32)
endif()

set_target_properties(test_turbo_flow_email PROPERTIES FOLDER "io/email/tests")

add_test(NAME test_turbo_flow_email COMMAND test_turbo_flow_email)

