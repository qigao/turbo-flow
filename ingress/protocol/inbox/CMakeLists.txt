set(_protocol_inbox_schema "${CMAKE_CURRENT_SOURCE_DIR}/protocol_inbox.schema")
set(_protocol_inbox_generated_dir "${CMAKE_CURRENT_BINARY_DIR}/generated")
set(_protocol_inbox_generated_header
    "${_protocol_inbox_generated_dir}/turbo_flow_protocol_inbox_envelope.h")
set(_protocol_inbox_generated_source
    "${_protocol_inbox_generated_dir}/turbo_flow_protocol_inbox_envelope.c")
set(_protocol_inbox_generated_rfl
    "${_protocol_inbox_generated_dir}/turbo_flow_protocol_inbox_envelope.rfl")
set(_protocol_inbox_generated_ts
    "${_protocol_inbox_generated_dir}/turbo_flow_protocol_inbox_envelope.d.ts")

add_custom_command(
  OUTPUT "${_protocol_inbox_generated_header}"
         "${_protocol_inbox_generated_source}"
         "${_protocol_inbox_generated_rfl}"
  COMMAND ${CMAKE_COMMAND} -E make_directory
          "${_protocol_inbox_generated_dir}"
  COMMAND
    "${TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE}"
    "${_protocol_inbox_schema}"
    --lang c
    --output "${_protocol_inbox_generated_header}"
    --source-output "${_protocol_inbox_generated_source}"
    --dsl-output "${_protocol_inbox_generated_rfl}"
  DEPENDS "${_protocol_inbox_schema}"
          "${TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE}"
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMENT "Generating canonical protocol Inbox TBE and RulesForge bindings"
  VERBATIM)

add_custom_command(
  OUTPUT "${_protocol_inbox_generated_ts}"
  COMMAND ${CMAKE_COMMAND} -E make_directory
          "${_protocol_inbox_generated_dir}"
  COMMAND
    "${TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE}"
    "${_protocol_inbox_schema}"
    --lang ts
    --output "${_protocol_inbox_generated_ts}"
  DEPENDS "${_protocol_inbox_schema}"
          "${TURBO_FLOW_TBE_COMPILER_HOST_EXECUTABLE}"
  WORKING_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}"
  COMMENT "Generating protocol Inbox TurboScript declarations"
  VERBATIM)

add_custom_target(
  tf_protocol_inbox_declarations
  DEPENDS "${_protocol_inbox_generated_rfl}"
          "${_protocol_inbox_generated_ts}")
set_target_properties(
  tf_protocol_inbox_declarations
  PROPERTIES FOLDER "ingress/protocol/inbox/internal")

add_library(tf_protocol_inbox_schema STATIC
            "${_protocol_inbox_generated_source}")
add_library(TurboFlow::ProtocolIngressInboxSchema ALIAS
            tf_protocol_inbox_schema)
add_dependencies(tf_protocol_inbox_schema tf_protocol_inbox_declarations)
set_target_properties(
  tf_protocol_inbox_schema
  PROPERTIES POSITION_INDEPENDENT_CODE ON
             EXPORT_NAME ProtocolIngressInboxSchema
             FOLDER "ingress/protocol/inbox/internal")
target_include_directories(
  tf_protocol_inbox_schema
  PUBLIC $<BUILD_INTERFACE:${_protocol_inbox_generated_dir}>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(tf_protocol_inbox_schema PUBLIC Salts::DataBind)

add_library(
  tf_protocol_inbox SHARED
  src/flow_protocol_inbox.c
  include/turbo_flow_protocol_inbox.h)
cmake_config_target(
  tf_protocol_inbox
  ALIAS TurboFlow::ProtocolIngressInbox
  FOLDER "ingress/protocol/inbox"
  EXPORT_NAME ProtocolIngressInbox)
target_compile_definitions(tf_protocol_inbox PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  tf_protocol_inbox
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(
  tf_protocol_inbox
  PUBLIC TurboFlow::ProtocolIngress TurboFlow::Graph
  PRIVATE tf_protocol_inbox_schema)

install(FILES include/turbo_flow_protocol_inbox.h DESTINATION include)
install(FILES "${_protocol_inbox_generated_header}" DESTINATION include)
install(
  FILES "${_protocol_inbox_schema}"
        "${_protocol_inbox_generated_rfl}"
        "${_protocol_inbox_generated_ts}"
  DESTINATION share/TurboFlow/protocol/inbox)

unset(_protocol_inbox_generated_dir)
unset(_protocol_inbox_generated_header)
unset(_protocol_inbox_generated_rfl)
unset(_protocol_inbox_generated_source)
unset(_protocol_inbox_generated_ts)
unset(_protocol_inbox_schema)
