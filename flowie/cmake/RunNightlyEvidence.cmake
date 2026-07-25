# MQTT-GATE-003: correlate corpus, soak, and sanitizer evidence at one revision and seed.
foreach(required CTEST_COMMAND CMAKE_COMMAND TEST_DIR OUTPUT_FILE REVISION SEED SOAK_SHORT_MS
                 SOAK_LONG_MS SANITIZER_EXECUTABLE)
  if(NOT DEFINED ${required} OR "${${required}}" STREQUAL "")
    message(FATAL_ERROR "${required} is required to collect nightly evidence")
  endif()
endforeach()
if(NOT EXISTS "${SANITIZER_EXECUTABLE}")
  message(FATAL_ERROR "sanitizer executable does not exist: ${SANITIZER_EXECUTABLE}")
endif()
if(NOT REVISION MATCHES "^[A-Za-z0-9._-]+$")
  message(FATAL_ERROR "REVISION contains characters that cannot be represented safely in evidence")
endif()
if(NOT SEED MATCHES "^[0-9]+$")
  message(FATAL_ERROR "SEED must be an unsigned decimal integer")
endif()
if(NOT SOAK_SHORT_MS MATCHES "^[1-9][0-9]*$" OR
   NOT SOAK_LONG_MS MATCHES "^[1-9][0-9]*$")
  message(FATAL_ERROR "SOAK_SHORT_MS and SOAK_LONG_MS must be positive milliseconds")
endif()
if(SOAK_SHORT_MS LESS 1800000 OR SOAK_LONG_MS LESS 3600000 OR
   SOAK_SHORT_MS GREATER 28800000 OR SOAK_LONG_MS GREATER 28800000)
  message(FATAL_ERROR
          "nightly soak windows must be at least 30/60 minutes and no longer than 8 hours")
endif()
if(NOT DEFINED FUZZ_RUNS OR FUZZ_RUNS LESS 1)
  set(FUZZ_RUNS 100000)
endif()
set(soak_asan_options "quarantine_size_mb=0:thread_local_quarantine_size_kb=0")
if(DEFINED ENV{ASAN_OPTIONS} AND NOT "$ENV{ASAN_OPTIONS}" STREQUAL "")
  set(soak_asan_options "$ENV{ASAN_OPTIONS}:${soak_asan_options}")
endif()

set(config_args)
if(DEFINED CONFIG AND NOT "${CONFIG}" STREQUAL "")
  set(config_args -C "${CONFIG}")
endif()
execute_process(
  COMMAND "${CTEST_COMMAND}" --test-dir "${TEST_DIR}" --output-on-f ${config_args}
          -R "^test_flowie_mqtt_protocol_corpus$"
  RESULT_VARIABLE corpus_result)
execute_process(
  COMMAND "${CMAKE_COMMAND}" -E env
          "ASAN_OPTIONS=${soak_asan_options}"
          "FLOWIE_MQTT_SOAK_SEED=${SEED}"
          "FLOWIE_MQTT_SOAK_001_DURATION_MS=${SOAK_SHORT_MS}"
          "FLOWIE_MQTT_SOAK_002_DURATION_MS=${SOAK_SHORT_MS}"
          "FLOWIE_MQTT_SOAK_003_DURATION_MS=${SOAK_SHORT_MS}"
          "FLOWIE_MQTT_SOAK_004_DURATION_MS=${SOAK_LONG_MS}"
          "FLOWIE_MQTT_SOAK_005_DURATION_MS=${SOAK_LONG_MS}"
          "FLOWIE_MQTT_SOAK_006_DURATION_MS=${SOAK_SHORT_MS}"
          "${CTEST_COMMAND}" --test-dir "${TEST_DIR}" -V --output-on-f ${config_args}
          -R "^test_flowie_mqtt_soak$"
  RESULT_VARIABLE soak_result
  OUTPUT_VARIABLE soak_output
  ERROR_VARIABLE soak_error)
string(REGEX MATCHALL "resource_monotonic_growth=false" soak_resource_records
             "${soak_output}\n${soak_error}")
list(LENGTH soak_resource_records soak_resource_count)
if(NOT soak_resource_count EQUAL 6)
  set(soak_growth true)
else()
  set(soak_growth false)
endif()
set(fuzz_artifact_dir "${TEST_DIR}/flowie-fuzz-artifacts")
file(MAKE_DIRECTORY "${fuzz_artifact_dir}")
string(REPLACE "\\" "/" fuzz_artifact_json "${fuzz_artifact_dir}")
execute_process(
  COMMAND "${SANITIZER_EXECUTABLE}" "-runs=${FUZZ_RUNS}" "-seed=${SEED}"
          "-artifact_prefix=${fuzz_artifact_dir}/" -print_final_stats=1
  RESULT_VARIABLE sanitizer_result)

foreach(kind corpus soak sanitizer)
  if("${${kind}_result}" STREQUAL "0")
    set(${kind}_status PASS)
  else()
    set(${kind}_status FAIL)
  endif()
endforeach()
file(WRITE "${OUTPUT_FILE}"
     "{\n  \"revision\": \"${REVISION}\",\n  \"records\": [\n"
     "    {\"kind\": \"corpus\", \"revision\": \"${REVISION}\", "
     "\"result\": \"${corpus_status}\", \"seed\": \"${SEED}\", "
     "\"resource_monotonic_growth\": false},\n"
     "    {\"kind\": \"soak\", \"revision\": \"${REVISION}\", "
     "\"result\": \"${soak_status}\", \"seed\": \"${SEED}\", "
     "\"resource_monotonic_growth\": ${soak_growth}},\n"
     "    {\"kind\": \"sanitizer\", \"revision\": \"${REVISION}\", "
     "\"result\": \"${sanitizer_status}\", \"seed\": \"${SEED}\", "
     "\"artifact_directory\": \"${fuzz_artifact_json}\", "
     "\"resource_monotonic_growth\": false}\n  ]\n}\n")
execute_process(
  COMMAND "${CMAKE_COMMAND}" "-DEVIDENCE_FILE=${OUTPUT_FILE}" -P
          "${CMAKE_CURRENT_LIST_DIR}/VerifyNightlyEvidence.cmake"
  RESULT_VARIABLE evidence_result)
if(NOT evidence_result EQUAL 0)
  message(FATAL_ERROR "Flowie nightly evidence contains failed or incomplete records")
endif()
message(STATUS "Flowie nightly evidence written to ${OUTPUT_FILE}")
