set(TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE
    ""
    CACHE FILEPATH
          "Host tbe_compiler used to generate DataBind 2.0 typed gateway bindings")

if(NOT TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE)
  find_program(
    _turbo_flow_tbe_compiler
    NAMES tbe_compiler tbe_compiler.exe
    HINTS
      "${TURBO_UTILS_ROOT}/bin"
      "${PROJECT_SOURCE_DIR}/../turbo-utils/build/Msvc/bin"
      "${PROJECT_SOURCE_DIR}/../turbo-utils/build/Msvc-Release/bin"
      "${PROJECT_SOURCE_DIR}/../turbo-utils/build/linux-gcc-debug/bin"
      "${PROJECT_SOURCE_DIR}/../turbo-utils/build/linux-gcc-release/bin")
  if(_turbo_flow_tbe_compiler)
    set(TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE
        "${_turbo_flow_tbe_compiler}"
        CACHE FILEPATH
              "Host tbe_compiler used to generate DataBind 2.0 typed gateway bindings"
              FORCE)
  endif()
endif()

if(NOT EXISTS "${TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE}")
  message(
    FATAL_ERROR
      "The complete TurboFlow build requires the DataBind 2.0 host "
      "tbe_compiler. Set TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE to the "
      "compiler executable.")
endif()

add_subdirectory(ocpp201_core)

unset(_turbo_flow_tbe_compiler)
