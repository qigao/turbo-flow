cmake_minimum_required(VERSION 3.20)

if(DEFINED CHTTP_TEST_MODULE)
  string(REPLACE "__CHTTP_LIST__" ";" CHTTP_TEST_ALLOWED_ROOTS
         "${CHTTP_TEST_ALLOWED_ROOTS}")
  string(REPLACE "__CHTTP_LIST__" ";" CHTTP_TEST_SEARCH_DIRS
         "${CHTTP_TEST_SEARCH_DIRS}")
  foreach(required_var IN ITEMS
          CHTTP_TEST_ALLOWED_ROOTS CHTTP_TEST_SEARCH_DIRS CHTTP_TEST_CONFIG)
    if(NOT DEFINED ${required_var} OR "${${required_var}}" STREQUAL "")
      message(FATAL_ERROR "Missing required variable: ${required_var}")
    endif()
  endforeach()
  if(DEFINED CHTTP_EXPECT_FAILURE_PATTERN AND
     NOT DEFINED CHTTP_VALIDATE_ONLY)
    string(REPLACE ";" "__CHTTP_LIST__" closure_allowed_roots_arg
           "${CHTTP_TEST_ALLOWED_ROOTS}")
    string(REPLACE ";" "__CHTTP_LIST__" closure_search_dirs_arg
           "${CHTTP_TEST_SEARCH_DIRS}")
    execute_process(
      COMMAND "${CMAKE_COMMAND}"
              "-DCHTTP_TEST_MODULE=${CHTTP_TEST_MODULE}"
              "-DCHTTP_TEST_ALLOWED_ROOTS=${closure_allowed_roots_arg}"
              "-DCHTTP_TEST_SEARCH_DIRS=${closure_search_dirs_arg}"
              "-DCHTTP_TEST_CONFIG=${CHTTP_TEST_CONFIG}"
              -DCHTTP_VALIDATE_ONLY=ON
              -P "${CMAKE_CURRENT_LIST_FILE}"
      RESULT_VARIABLE expected_failure_result
      OUTPUT_VARIABLE expected_failure_output
      ERROR_VARIABLE expected_failure_error)
    string(CONCAT expected_failure_diagnostic
           "${expected_failure_output}" "\n${expected_failure_error}")
    if(expected_failure_result EQUAL 0)
      message(FATAL_ERROR "CHTTP closure validation unexpectedly succeeded")
    endif()
    if(NOT expected_failure_diagnostic MATCHES "${CHTTP_EXPECT_FAILURE_PATTERN}")
      message(FATAL_ERROR
              "CHTTP closure validation failed without '${CHTTP_EXPECT_FAILURE_PATTERN}'\n${expected_failure_diagnostic}")
    endif()
    return()
  endif()
  if(NOT EXISTS "${CHTTP_TEST_MODULE}")
    message(FATAL_ERROR "CHTTP closure root does not exist: ${CHTTP_TEST_MODULE}")
  endif()
  file(GET_RUNTIME_DEPENDENCIES
       LIBRARIES "${CHTTP_TEST_MODULE}"
       DIRECTORIES ${CHTTP_TEST_SEARCH_DIRS}
       RESOLVED_DEPENDENCIES_VAR closure_resolved
       UNRESOLVED_DEPENDENCIES_VAR closure_unresolved
       CONFLICTING_DEPENDENCIES_PREFIX closure_conflicts
       PRE_EXCLUDE_REGEXES "^(api-ms-|ext-ms-)"
       POST_EXCLUDE_REGEXES
         "^[A-Za-z]:[/\\\\][Ww][Ii][Nn][Dd][Oo][Ww][Ss][/\\\\]([Ss]ystem32|[Ss]ys[Ww][Oo][Ww]64)[/\\\\]")
  if(closure_unresolved)
    message(FATAL_ERROR
            "CHTTP closure has unresolved dependencies: ${closure_unresolved}")
  endif()
  if(closure_conflicts_FILENAMES)
    message(FATAL_ERROR
            "CHTTP closure has conflicting dependencies: ${closure_conflicts_FILENAMES}")
  endif()
  foreach(resolved_dependency IN LISTS closure_resolved)
    cmake_path(GET resolved_dependency FILENAME resolved_name)
    string(TOLOWER "${resolved_name}" resolved_name_lower)
    if(resolved_name_lower MATCHES "^salts_chttp[^ \\t\\r\\n]*\\.dll$")
      message(FATAL_ERROR
              "CHTTP recursive closure contains a legacy salts_chttp DLL: ${resolved_dependency}")
    endif()
    if(CHTTP_TEST_CONFIG STREQUAL "Release" AND
       resolved_name_lower MATCHES "^(vcruntime[0-9]*d|ucrtbased)\\.dll$")
      message(FATAL_ERROR
              "Release CHTTP recursive closure contains a Debug CRT: ${resolved_dependency}")
    endif()
    if(resolved_name_lower MATCHES
       "^(tf_chttp.*|chttp_.*|salts_.*|turbo_flow.*)\\.dll$")
      set(resolved_is_allowed FALSE)
      foreach(allowed_root IN LISTS CHTTP_TEST_ALLOWED_ROOTS)
        if(IS_DIRECTORY "${allowed_root}")
          file(REAL_PATH "${allowed_root}" allowed_root_real)
          file(REAL_PATH "${resolved_dependency}" resolved_dependency_real)
          cmake_path(IS_PREFIX allowed_root_real "${resolved_dependency_real}"
                     NORMALIZE resolved_is_within_root)
          if(resolved_is_within_root)
            set(resolved_is_allowed TRUE)
          endif()
        endif()
      endforeach()
      if(NOT resolved_is_allowed)
        message(FATAL_ERROR
                "CHTTP first-party dependency resolved outside the active profile roots: ${resolved_dependency}")
      endif()
    endif()
  endforeach()
  return()
endif()

if(DEFINED CHTTP_EXPECT_FAILURE_PATTERN AND
   NOT DEFINED CHTTP_VALIDATE_ONLY)
  execute_process(
    COMMAND "${CMAKE_COMMAND}"
            "-DCHTTP_TEST_LAYER=${CHTTP_TEST_LAYER}"
            "-DCHTTP_TEST_DEPENDENTS=${CHTTP_TEST_DEPENDENTS}"
            -DCHTTP_VALIDATE_ONLY=ON
            -P "${CMAKE_CURRENT_LIST_FILE}"
    RESULT_VARIABLE expected_failure_result
    OUTPUT_VARIABLE expected_failure_output
    ERROR_VARIABLE expected_failure_error)
  string(CONCAT expected_failure_diagnostic
         "${expected_failure_output}" "\n${expected_failure_error}")
  if(expected_failure_result EQUAL 0)
    message(FATAL_ERROR "CHTTP dependency validation unexpectedly succeeded")
  endif()
  if(NOT expected_failure_diagnostic MATCHES "${CHTTP_EXPECT_FAILURE_PATTERN}")
    message(FATAL_ERROR
            "CHTTP dependency validation failed without '${CHTTP_EXPECT_FAILURE_PATTERN}'\n${expected_failure_diagnostic}")
  endif()
  return()
endif()

foreach(required_var IN ITEMS CHTTP_TEST_LAYER CHTTP_TEST_DEPENDENTS)
  if(NOT DEFINED ${required_var} OR "${${required_var}}" STREQUAL "")
    message(FATAL_ERROR "Missing required variable: ${required_var}")
  endif()
endforeach()

string(TOLOWER "${CHTTP_TEST_DEPENDENTS}" chttp_dependents)
if(chttp_dependents MATCHES "salts_chttp[^ \t\r\n]*\\.dll")
  message(FATAL_ERROR
          "CHTTP dependency closure must not contain a legacy salts_chttp DLL")
endif()

if(CHTTP_TEST_LAYER STREQUAL "adapter")
  string(REGEX MATCHALL "(^|[ \t\r\n])chttp_[^ \t\r\n]*dll([ \t\r\n]|$)"
         all_native_imports "${chttp_dependents}")
  foreach(native_import IN LISTS all_native_imports)
    string(STRIP "${native_import}" native_import)
    if(NOT native_import STREQUAL "chttp_client.dll" AND
       NOT native_import STREQUAL "chttp_server.dll")
      message(FATAL_ERROR "CHTTP adapter has an unknown native import: ${native_import}")
    endif()
  endforeach()
  foreach(native_dll_regex IN ITEMS "chttp_client\\.dll" "chttp_server\\.dll")
    string(REGEX MATCHALL "(^|[ \t\r\n])${native_dll_regex}([ \t\r\n]|$)"
           native_imports "${chttp_dependents}")
    list(LENGTH native_imports native_import_count)
    if(NOT native_import_count EQUAL 1)
      string(REPLACE "\\." "." native_dll "${native_dll_regex}")
      message(FATAL_ERROR
              "CHTTP adapter requires exactly one ${native_dll} import")
    endif()
  endforeach()
elseif(CHTTP_TEST_LAYER STREQUAL "provider")
  string(REGEX MATCHALL
         "(^|[ \t\r\n])tf_chttp_adapter\\.dll([ \t\r\n]|$)"
         adapter_imports "${chttp_dependents}")
  list(LENGTH adapter_imports adapter_import_count)
  if(NOT adapter_import_count EQUAL 1)
    message(FATAL_ERROR
            "CHTTP provider requires exactly one tf_chttp_adapter.dll import")
  endif()
  if(chttp_dependents MATCHES "turbo_flow\\.dll")
    message(FATAL_ERROR "CHTTP provider must not depend on turbo_flow.dll")
  endif()
elseif(CHTTP_TEST_LAYER STREQUAL "gateway")
  if(chttp_dependents MATCHES
     "tf_chttp_adapter\\.dll|chttp_client\\.dll|chttp_server\\.dll|turbo_flow\\.dll")
    message(FATAL_ERROR "Gateway must not link a concrete HTTP implementation")
  endif()
else()
  message(FATAL_ERROR "Unknown CHTTP dependency layer: ${CHTTP_TEST_LAYER}")
endif()
