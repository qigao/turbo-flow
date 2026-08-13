set(TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE
    ""
    CACHE FILEPATH
          "Host tbe_compiler used to generate DataBind 2.0 typed gateway bindings")

if(NOT EXISTS "${TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE}")
  message(
    FATAL_ERROR
      "The complete TurboFlow build requires the DataBind 2.0 host "
      "tbe_compiler. Set TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE to the "
      "compiler executable.")
endif()

add_subdirectory(ocpp201_core)
