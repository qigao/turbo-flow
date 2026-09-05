set(_ocpp201_core_schema "${CMAKE_CURRENT_SOURCE_DIR}/ocpp201_core.schema")
set(_ocpp201_core_generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(_ocpp201_core_generated_header
    "${_ocpp201_core_generated_dir}/ocpp201_core.h")
set(_ocpp201_core_generated_source
    "${_ocpp201_core_generated_dir}/ocpp201_core.c")

add_custom_command(
  OUTPUT "${_ocpp201_core_generated_header}"
         "${_ocpp201_core_generated_source}"
  COMMAND ${CMAKE_COMMAND} -E make_directory
          "${_ocpp201_core_generated_dir}"
  COMMAND
    "${TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE}"
    "${_ocpp201_core_schema}"
    --lang c
    --output "${_ocpp201_core_generated_header}"
    --source-output "${_ocpp201_core_generated_source}"
  DEPENDS "${_ocpp201_core_schema}"
          "${TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE}"
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMENT "Generating DataBind 2.0 OCPP 2.0.1 Core typed bindings"
  VERBATIM)

add_library(tf_protocol_business_ocpp201_core_schema STATIC
            "${_ocpp201_core_generated_source}")
set_target_properties(
  tf_protocol_business_ocpp201_core_schema
  PROPERTIES POSITION_INDEPENDENT_CODE ON
             FOLDER "ingress/protocol/business/internal")
target_include_directories(
  tf_protocol_business_ocpp201_core_schema
  PUBLIC "${_ocpp201_core_generated_dir}")
target_link_libraries(tf_protocol_business_ocpp201_core_schema
                      PUBLIC Salts::DataBind)

add_library(tf_protocol_business_ocpp201_core SHARED
            flow_protocol_business_ocpp201_core.c)
cmake_config_target(
  tf_protocol_business_ocpp201_core
  FOLDER "ingress/protocol/business"
  EXPORT_NAME ProtocolBusinessOcpp201Core)
target_compile_definitions(tf_protocol_business_ocpp201_core PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  tf_protocol_business_ocpp201_core
  PRIVATE "${PROJECT_SOURCE_DIR}/ingress/protocol/src")
target_link_libraries(
  tf_protocol_business_ocpp201_core
  PRIVATE tf_protocol_business_ocpp201_core_schema
          tf_protocol_plugin_support
          TurboFlow::ProtocolIngress
          Salts::DataBind
          Salts::JsonParser)

if(BUILD_TESTS)
  add_library(tf_protocol_business_ocpp201_core_schema_shared SHARED
              "${_ocpp201_core_generated_source}")
  target_compile_definitions(
    tf_protocol_business_ocpp201_core_schema_shared
    PRIVATE TBE_GENERATED_BUILD_SHARED)
  target_include_directories(
    tf_protocol_business_ocpp201_core_schema_shared
    PUBLIC "${_ocpp201_core_generated_dir}")
  target_link_libraries(tf_protocol_business_ocpp201_core_schema_shared
                        PUBLIC Salts::DataBind)
  cmake_config_target(
    tf_protocol_business_ocpp201_core_schema_shared
    FOLDER "ingress/protocol/business/tests")
endif()

unset(_ocpp201_core_generated_dir)
unset(_ocpp201_core_generated_header)
unset(_ocpp201_core_generated_source)
unset(_ocpp201_core_schema)
