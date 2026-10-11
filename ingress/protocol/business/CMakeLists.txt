set(TURBO_FLOW_IDLC_HOST_EXECUTABLE
    ""
    CACHE FILEPATH
          "Host salts-idlc used to generate DataBind 3 typed protocol bindings")

if(NOT EXISTS "${TURBO_FLOW_IDLC_HOST_EXECUTABLE}")
  message(
    FATAL_ERROR
      "The complete TurboFlow build requires the DataBind 3 host "
      "salts-idlc. Set TURBO_FLOW_IDLC_HOST_EXECUTABLE to the "
      "compiler executable.")
endif()

add_subdirectory(ocpp201_core)
