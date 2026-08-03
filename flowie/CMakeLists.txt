add_subdirectory(protocol)
add_subdirectory(client)
  add_subdirectory(control)

set(TARGET_NAME flowie)
set(FLOWIE_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/flowie.h)

add_library(
  ${TARGET_NAME} SHARED
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_client_adapter.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_config.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_endpoint.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_ingress.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_ingress_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_proxy_protocol.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_proxy_protocol_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_protocol_store.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_protocol_store_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_rule.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_rule_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_security_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_session.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_session_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_task_group_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_topic_index.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_topic_index_internal.h
  ${FLOWIE_HEADERS})
cmake_config_target(
  ${TARGET_NAME}
  ALIAS Flowie::App
  FOLDER "flowie"
  EXPORT_NAME Flowie)
target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>
  PRIVATE ${PROJECT_SOURCE_DIR}/io/common/include)
target_link_libraries(
  ${TARGET_NAME}
  PUBLIC Flowie::Protocol TurboFlow::Flow TurboNet::CoroNet TurboUtils::Core
  PRIVATE xxHash::xxhash)
if(WIN32)
  target_link_libraries(${TARGET_NAME} PRIVATE ws2_32)
endif()
target_link_libraries(${TARGET_NAME} PRIVATE Flowie::Client tf_executor_common TurboFlow::FlowStore)
install(FILES ${FLOWIE_HEADERS} DESTINATION include)

# Kept internal until the endpoint cluster binding connects the owner adapter
# and lease/fact workers; this is not yet a production cluster API.
add_library(
  flowie_cluster_pgsql STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_pgsql.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_pgsql_fact.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_pgsql_fact_worker.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_pgsql_worker.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_pgsql_internal.h)
cmake_config_target(flowie_cluster_pgsql NO_INSTALL FOLDER "flowie/cluster")
target_include_directories(flowie_cluster_pgsql PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(flowie_cluster_pgsql PUBLIC Flowie::App
                      PRIVATE PostgreSQL::PostgreSQL TurboUtils::Core monocypher)

# Internal wire contract. It remains separate from the installed Flowie ABI
# until peer authentication, transport shutdown, and owner-lane binding pass.
add_library(
  flowie_cluster_peer STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_connect_bind.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_connection_lost.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_edge_action.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_mqtt_command.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_publish_settle.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_mqtt_reply.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_owner.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_protocol.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_registry.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_takeover_close.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_transport.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_wire_internal.h)
cmake_config_target(flowie_cluster_peer NO_INSTALL FOLDER "flowie/cluster")
target_include_directories(flowie_cluster_peer PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(flowie_cluster_peer PUBLIC Flowie::App TurboNet::CoroNet
                      PRIVATE tf_executor_common TurboUtils::Core)

# Internal shard lifecycle and composition of the peer owner lane with fenced
# fact workers. It stays NO_INSTALL until application and listener wiring pass.
add_library(
  flowie_cluster_session STATIC
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_broadcast_dispatch.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_broadcast_dispatch_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_broadcast_event.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_broadcast_event_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_broadcast_target_dispatch.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_broadcast_target_dispatch_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_broadcast_target_owner.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_broadcast_target_owner_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_delivery_action.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_delivery_action_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_delivery_dispatch.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_delivery_dispatch_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_edge_bind.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_edge_bind_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_lifecycle_dispatch.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_lifecycle_dispatch_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_lifecycle_owner.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_lifecycle_owner_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_membership_controller.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_membership_controller_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_node_router.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_node_router_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_connector.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_connector_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_listener.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_peer_listener_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_publish_event.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_publish_event_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_route_store.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_route_store_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_route_projection.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_route_projection_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_route_dispatch.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_route_dispatch_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_shard_runtime.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_shard_runtime_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_session_bind.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_session_bind_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_takeover_dispatch.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_takeover_dispatch_internal.h
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_topology.c
  ${CMAKE_CURRENT_SOURCE_DIR}/src/flowie_cluster_topology_internal.h)
cmake_config_target(flowie_cluster_session NO_INSTALL FOLDER "flowie/cluster")
target_include_directories(flowie_cluster_session PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(flowie_cluster_session PUBLIC flowie_cluster_peer flowie_cluster_pgsql
                                                    Flowie::App TurboFlow::FlowStore TurboFlow::Redis
                      PRIVATE tf_executor_common TurboUtils::Core monocypher)

if(BUILD_TESTING)
  add_subdirectory(tests)
  add_subdirectory(benchmarks)
  if(FLOWIE_MQTT_RELEASE_GATE)
    add_test(
      NAME flowie_release_gate_manifest
      COMMAND
        ${CMAKE_COMMAND} "-DCTEST_COMMAND=${CMAKE_CTEST_COMMAND}"
        "-DTEST_DIR=${CMAKE_BINARY_DIR}" -P
        "${CMAKE_CURRENT_SOURCE_DIR}/cmake/VerifyReleaseGate.cmake")
    set_tests_properties(
      flowie_release_gate_manifest
      PROPERTIES LABELS "flowie;flowie-release;release-gate" TIMEOUT 30)
  endif()
endif()
