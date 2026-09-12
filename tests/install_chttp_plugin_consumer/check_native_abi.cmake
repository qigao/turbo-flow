cmake_minimum_required(VERSION 3.20)

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
