# MQTT-GATE-002: collect and verify one complete release evidence manifest.
foreach(required CTEST_COMMAND CMAKE_COMMAND TEST_DIR OUTPUT_FILE REVISION PROJECT_VERSION
                 FIXED_BROKER_VERSION REDIS_VERSION)
  if(NOT DEFINED ${required} OR "${${required}}" STREQUAL "")
    message(FATAL_ERROR "${required} is required to collect release evidence")
  endif()
endforeach()
if(NOT DEFINED FIXED_SUPPORT_31 OR
   NOT "${FIXED_SUPPORT_31}" MATCHES "^(0|1|ON|OFF|TRUE|FALSE)$")
  message(FATAL_ERROR "FIXED_SUPPORT_31 must be an explicit CMake boolean")
endif()
if(NOT DEFINED FIXED_SUPPORT_31_WS OR
   NOT "${FIXED_SUPPORT_31_WS}" MATCHES "^(0|1|ON|OFF|TRUE|FALSE)$")
  message(FATAL_ERROR "FIXED_SUPPORT_31_WS must be an explicit CMake boolean")
endif()

foreach(metadata REVISION PROJECT_VERSION FIXED_BROKER_VERSION REDIS_VERSION)
  if(NOT "${${metadata}}" MATCHES "^[A-Za-z0-9._+:/-]+$")
    message(FATAL_ERROR
            "${metadata} contains characters that cannot be represented safely in evidence")
  endif()
endforeach()

execute_process(
  COMMAND "${CMAKE_COMMAND}" "-DCTEST_COMMAND=${CTEST_COMMAND}" "-DTEST_DIR=${TEST_DIR}"
          -P "${CMAKE_CURRENT_LIST_DIR}/VerifyReleaseGate.cmake"
  RESULT_VARIABLE gate_result)
if(NOT gate_result EQUAL 0)
  message(FATAL_ERROR "Flowie release manifest verification failed before execution")
endif()

set(records
    "test_flowie_mqtt_protocol_matrix|local|${PROJECT_VERSION}|none"
    "test_flowie_mqtt_protocol_corpus|local|${PROJECT_VERSION}|none"
    "test_flowie_mqtt_client_fixed_interop|fixed|${FIXED_BROKER_VERSION}|TLS/WSS"
    "flowie_mqtt_fixed_mqtt31_capability|fixed|${FIXED_BROKER_VERSION}|TLS"
    "flowie_mqtt_fixed_mqtt31_ws_capability|fixed|${FIXED_BROKER_VERSION}|WSS"
    "test_flowie_mosquitto_interop|local|${PROJECT_VERSION}|TCP"
    "test_flowie_mosquitto_fixed_interop|fixed|${FIXED_BROKER_VERSION}|TCP"
    "test_flowie_session_store_faults|memory|${PROJECT_VERSION}|none"
    "test_flowie_transport|local|OpenSSL|TLS/WSS"
    "test_flowie_mqtt_endurance|memory|${PROJECT_VERSION}|none"
    "test_flowie_mqtt_soak|redis|Redis-${REDIS_VERSION}+PostgreSQL|TLS/WSS"
    "flowie_server_check_redis_session_store|redis|${REDIS_VERSION}|none"
    "flowie_server_check_https_auth_provider|http|${PROJECT_VERSION}|mTLS")

file(WRITE "${OUTPUT_FILE}" "{\n  \"revision\": \"${REVISION}\",\n  \"tests\": [\n")
set(first_record 1)
set(any_failure 0)
foreach(record IN LISTS records)
  string(REPLACE "|" ";" fields "${record}")
  list(GET fields 0 name)
  list(GET fields 1 backend)
  list(GET fields 2 version)
  list(GET fields 3 tls)
  if((name STREQUAL "flowie_mqtt_fixed_mqtt31_capability" AND NOT FIXED_SUPPORT_31) OR
     (name STREQUAL "flowie_mqtt_fixed_mqtt31_ws_capability" AND NOT FIXED_SUPPORT_31_WS))
    set(result SKIP)
  else()
    set(ctest_args --test-dir "${TEST_DIR}" --output-on-f -R "^${name}$")
    if(DEFINED CONFIG AND NOT "${CONFIG}" STREQUAL "")
      list(APPEND ctest_args -C "${CONFIG}")
    endif()
    execute_process(COMMAND "${CTEST_COMMAND}" ${ctest_args} RESULT_VARIABLE test_result)
    if(test_result EQUAL 0)
      set(result PASS)
    else()
      set(result FAIL)
      set(any_failure 1)
    endif()
  endif()
  if(first_record)
    set(first_record 0)
  else()
    file(APPEND "${OUTPUT_FILE}" ",\n")
  endif()
  file(APPEND "${OUTPUT_FILE}"
       "    {\"name\": \"${name}\", \"result\": \"${result}\", "
       "\"label\": \"flowie-release\", \"backend\": \"${backend}\", "
       "\"version\": \"${version}\", \"tls\": \"${tls}\"}")
endforeach()
file(APPEND "${OUTPUT_FILE}" "\n  ]\n}\n")

execute_process(
  COMMAND "${CMAKE_COMMAND}" "-DEVIDENCE_FILE=${OUTPUT_FILE}" -P
          "${CMAKE_CURRENT_LIST_DIR}/VerifyReleaseEvidence.cmake"
  RESULT_VARIABLE evidence_result)
if(any_failure OR NOT evidence_result EQUAL 0)
  message(FATAL_ERROR "Flowie release evidence contains failed or incomplete records")
endif()
message(STATUS "Flowie release evidence written to ${OUTPUT_FILE}")
