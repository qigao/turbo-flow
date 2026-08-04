if(NOT DEFINED CTEST_COMMAND OR NOT DEFINED TEST_DIR)
  message(FATAL_ERROR "CTEST_COMMAND and TEST_DIR are required")
endif()

execute_process(
  COMMAND "${CTEST_COMMAND}" --test-dir "${TEST_DIR}" --show-only=json-v1 -L flowie-release
  RESULT_VARIABLE manifest_result
  OUTPUT_VARIABLE manifest_json
  ERROR_VARIABLE manifest_error)
if(NOT manifest_result EQUAL 0)
  message(FATAL_ERROR "cannot read Flowie release manifest\n${manifest_error}")
endif()

set(required_tests
    test_flowie_mqtt_protocol_matrix
    test_flowie_mqtt_protocol_corpus
    test_flowie_mqtt_client_fixed_interop
    flowie_mqtt_fixed_mqtt31_capability
    flowie_mqtt_fixed_mqtt31_ws_capability
    test_flowie_mosquitto_interop
    test_flowie_mosquitto_fixed_interop
    test_flowie_session_store_faults
    test_flowie_transport
    test_flowie_mqtt_endurance
    test_flowie_mqtt_soak
    test_flowie_cluster_route_redis_live
    flowie_server_rejects_redis_protocol_store
    flowie_server_check_https_auth_provider)
set(allowed_disabled_tests flowie_server_check_smb_product)
set(observed_tests)
set(disabled_tests)

string(JSON test_count LENGTH "${manifest_json}" tests)
if(test_count EQUAL 0)
  message(FATAL_ERROR "Flowie release manifest is empty")
endif()
math(EXPR last_test "${test_count} - 1")
foreach(test_index RANGE 0 ${last_test})
  string(JSON test_name GET "${manifest_json}" tests ${test_index} name)
  list(APPEND observed_tests "${test_name}")
  string(JSON property_count LENGTH "${manifest_json}" tests ${test_index} properties)
  if(property_count EQUAL 0)
    continue()
  endif()
  math(EXPR last_property "${property_count} - 1")
  foreach(property_index RANGE 0 ${last_property})
    string(JSON property_name GET "${manifest_json}" tests ${test_index} properties
                                      ${property_index} name)
    if(property_name STREQUAL "DISABLED")
      string(JSON property_value GET "${manifest_json}" tests ${test_index} properties
                                        ${property_index} value)
      if(property_value)
        list(APPEND disabled_tests "${test_name}")
      endif()
    endif()
  endforeach()
endforeach()

foreach(required_test IN LISTS required_tests)
  list(FIND observed_tests "${required_test}" observed_index)
  if(observed_index EQUAL -1)
    message(FATAL_ERROR "Flowie release gate is missing required test: ${required_test}")
  endif()
  list(FIND disabled_tests "${required_test}" disabled_index)
  if(NOT disabled_index EQUAL -1)
    message(FATAL_ERROR "Flowie release gate has disabled required test: ${required_test}")
  endif()
endforeach()

foreach(disabled_test IN LISTS disabled_tests)
  list(FIND allowed_disabled_tests "${disabled_test}" allowed_index)
  if(allowed_index EQUAL -1)
    message(FATAL_ERROR "Flowie release gate has unexpected disabled test: ${disabled_test}")
  endif()
endforeach()

message(STATUS "Flowie release manifest verified: ${test_count} tests, disabled=${disabled_tests}")
