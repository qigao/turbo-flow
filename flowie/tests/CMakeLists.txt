cmake_add_test(
  test_flowie
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie.c
  LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)

set_tests_properties(
  test_flowie
  PROPERTIES LABELS "flowie;flowie-release;mqtt-fast;mqtt-negative;mqtt-persistence")

cmake_add_test(
  test_flowie_proxy_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_proxy_protocol.c
          ${CMAKE_CURRENT_SOURCE_DIR}/../src/flowie_proxy_protocol.c
  LIBS Flowie::App TurboUtils::TinyTest
  FOLDER "flowie/tests")
target_include_directories(test_flowie_proxy_protocol PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
if(WIN32)
  target_link_libraries(test_flowie_proxy_protocol PRIVATE ws2_32)
endif()
set_tests_properties(test_flowie_proxy_protocol
                     PROPERTIES LABELS "flowie;cluster;proxy;protocol;security")

cmake_add_test(
  test_flowie_topic_index
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_topic_index.c
  LIBS Flowie::App TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie_topic_index PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
set_tests_properties(test_flowie_topic_index PROPERTIES LABELS "flowie;flowie-release")

cmake_add_test(
  test_flowie_cluster
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster.c
  LIBS Flowie::App TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie_cluster PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)
set_tests_properties(test_flowie_cluster PROPERTIES LABELS "flowie;cluster;flowie-release")

cmake_add_test(
  test_flowie_cluster_peer_protocol
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_protocol.c
  LIBS flowie_cluster_peer TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_peer_protocol
                     PROPERTIES LABELS "flowie;cluster;peer;protocol")

cmake_add_test(
  test_flowie_cluster_peer_registry
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_registry.c
  LIBS flowie_cluster_peer TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_peer_registry
                     PROPERTIES LABELS "flowie;cluster;peer;registry;lifecycle")

cmake_add_test(
  test_flowie_cluster_peer_connect_bind
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_connect_bind.c
  LIBS flowie_cluster_peer TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_peer_connect_bind
                     PROPERTIES LABELS "flowie;cluster;peer;protocol;mqtt;security")

cmake_add_test(
  test_flowie_cluster_peer_connection_lost
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_connection_lost.c
  LIBS flowie_cluster_peer TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_peer_connection_lost
                     PROPERTIES LABELS "flowie;cluster;peer;protocol;mqtt;lifecycle")

cmake_add_test(
  test_flowie_cluster_peer_mqtt_command
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_mqtt_command.c
  LIBS flowie_cluster_peer TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_peer_mqtt_command
                     PROPERTIES LABELS "flowie;cluster;peer;protocol;mqtt")

cmake_add_test(
  test_flowie_cluster_peer_publish_settle
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_publish_settle.c
  LIBS flowie_cluster_peer TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_peer_publish_settle
                     PROPERTIES LABELS "flowie;cluster;peer;protocol;mqtt;settlement")

cmake_add_test(
  test_flowie_cluster_peer_mqtt_reply
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_mqtt_reply.c
  LIBS flowie_cluster_peer TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_peer_mqtt_reply
                     PROPERTIES LABELS "flowie;cluster;peer;protocol;mqtt;reply")

cmake_add_test(
  test_flowie_cluster_peer_edge_action
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_edge_action.c
  LIBS flowie_cluster_peer TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_peer_edge_action
                     PROPERTIES LABELS "flowie;cluster;peer;mqtt;edge-action")

cmake_add_test(
  test_flowie_cluster_peer_takeover_close
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_takeover_close.c
  LIBS flowie_cluster_peer TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_peer_takeover_close
                     PROPERTIES LABELS "flowie;cluster;peer;protocol;mqtt;lifecycle;takeover")

cmake_add_test(
  test_flowie_cluster_peer_owner
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_owner.c
  LIBS flowie_cluster_peer tf_executor_common TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_peer_owner
                     PROPERTIES LABELS "flowie;cluster;peer;owner;fencing")

cmake_add_test(
  test_flowie_cluster_session_bind
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_session_bind.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_session_bind
                     PROPERTIES LABELS "flowie;cluster;session;owner;fencing;pgsql")

cmake_add_test(
  test_flowie_cluster_publish_event
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_publish_event.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_publish_event
                     PROPERTIES LABELS "flowie;cluster;session;publish;outbox")

cmake_add_test(
  test_flowie_cluster_broadcast_event
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_broadcast_event.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_broadcast_event
                     PROPERTIES LABELS "flowie;cluster;session;publish;broadcast;outbox")

cmake_add_test(
  test_flowie_cluster_broadcast_dispatch
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_broadcast_dispatch.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_broadcast_dispatch
                     PROPERTIES LABELS "flowie;cluster;session;publish;broadcast;outbox;pgsql")

cmake_add_test(
  test_flowie_cluster_broadcast_target_dispatch
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_broadcast_target_dispatch.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_broadcast_target_dispatch
                     PROPERTIES LABELS "flowie;cluster;session;publish;broadcast;target;transport")

cmake_add_test(
  test_flowie_cluster_broadcast_target_owner
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_broadcast_target_owner.c
  LIBS flowie_cluster_session tf_executor_common TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_broadcast_target_owner
                     PROPERTIES LABELS "flowie;cluster;session;publish;broadcast;target;owner;pgsql")

cmake_add_test(
  test_flowie_cluster_delivery_action
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_delivery_action.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_delivery_action
                     PROPERTIES LABELS "flowie;cluster;session;delivery;edge;outbox")

cmake_add_test(
  test_flowie_cluster_delivery_dispatch
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_delivery_dispatch.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_delivery_dispatch
                     PROPERTIES LABELS "flowie;cluster;session;delivery;peer;edge;outbox")

cmake_add_test(
  test_flowie_cluster_lifecycle_dispatch
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_lifecycle_dispatch.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_lifecycle_dispatch
                     PROPERTIES LABELS "flowie;cluster;session;lifecycle;outbox")

cmake_add_test(
  test_flowie_cluster_lifecycle_owner
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_lifecycle_owner.c
  LIBS flowie_cluster_session tf_executor_common TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_lifecycle_owner
                     PROPERTIES LABELS "flowie;cluster;session;lifecycle;owner;pgsql")

cmake_add_test(
  test_flowie_cluster_topology
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_topology.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_topology
                     PROPERTIES LABELS "flowie;cluster;membership;topology")

cmake_add_test(
  test_flowie_cluster_route_store
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_route_store.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_route_store
                     PROPERTIES LABELS "flowie;cluster;route;redis;state-store")

cmake_add_test(
  test_flowie_cluster_route_projection
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_route_projection.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_route_projection
                     PROPERTIES LABELS "flowie;cluster;route;projection;reconciliation")

cmake_add_test(
  test_flowie_cluster_route_dispatch
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_route_dispatch.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
cmake_target_enable_boringssl_linkage(test_flowie_cluster_route_dispatch)
set_tests_properties(test_flowie_cluster_route_dispatch
                     PROPERTIES LABELS "flowie;cluster;session;route;outbox;settlement")

cmake_add_test(
  test_flowie_cluster_membership_controller
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_membership_controller.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_membership_controller
                     PROPERTIES LABELS "flowie;cluster;membership;topology;pgsql")

cmake_add_test(
  test_flowie_cluster_takeover_dispatch
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_takeover_dispatch.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_takeover_dispatch
                     PROPERTIES LABELS "flowie;cluster;session;peer;takeover;outbox")

cmake_add_test(
  test_flowie_cluster_shard_runtime
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_shard_runtime.c
  LIBS flowie_cluster_session tf_executor_common TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_shard_runtime
                     PROPERTIES LABELS "flowie;cluster;session;owner;fencing;pgsql")
if(WIN32)
  set_tests_properties(
    test_flowie_cluster_route_dispatch test_flowie_cluster_shard_runtime
    PROPERTIES ENVIRONMENT_MODIFICATION
               "PATH=path_list_prepend:${RulesForge_ROOT}/bin")
endif()

cmake_add_test(
  test_flowie_cluster_node_router
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_node_router.c
  LIBS flowie_cluster_session TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_node_router
                     PROPERTIES LABELS "flowie;cluster;peer;session;router;lifecycle")

cmake_add_test(
  test_flowie_cluster_peer_listener
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_listener.c
  LIBS flowie_cluster_session tf_executor_common TurboNet::CoroNet TurboUtils::TinyTest
       OpenSSL::SSL OpenSSL::Crypto
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}
           ${PROJECT_SOURCE_DIR}/../turbonet/CoroNet/tests
  FOLDER "flowie/tests")
cmake_target_enable_boringssl_linkage(test_flowie_cluster_peer_listener)
set_tests_properties(test_flowie_cluster_peer_listener
                     PROPERTIES LABELS "flowie;cluster;peer;listener;security;lifecycle" TIMEOUT 20)
if(WIN32)
  target_link_libraries(test_flowie_cluster_peer_listener PRIVATE ws2_32)
endif()

cmake_add_test(
  test_flowie_cluster_peer_connector
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_connector.c
  LIBS flowie_cluster_session tf_executor_common TurboNet::CoroNet TurboUtils::TinyTest
       OpenSSL::SSL OpenSSL::Crypto
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}
           ${PROJECT_SOURCE_DIR}/../turbonet/CoroNet/tests
  FOLDER "flowie/tests")
cmake_target_enable_boringssl_linkage(test_flowie_cluster_peer_connector)
set_tests_properties(test_flowie_cluster_peer_connector
                     PROPERTIES LABELS "flowie;cluster;peer;connector;security;lifecycle" TIMEOUT 20)
if(WIN32)
  target_link_libraries(test_flowie_cluster_peer_connector PRIVATE ws2_32)
endif()

cmake_add_test(
  test_flowie_cluster_edge_bind
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_edge_bind.c
  LIBS flowie_cluster_session tf_executor_common TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_edge_bind
                     PROPERTIES LABELS "flowie;cluster;edge;session;fencing")

cmake_add_test(
  test_flowie_cluster_peer_transport
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_peer_transport.c
  LIBS flowie_cluster_peer TurboNet::CoroNet TurboUtils::TinyTest OpenSSL::SSL OpenSSL::Crypto
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}
           ${PROJECT_SOURCE_DIR}/../turbonet/CoroNet/tests
  FOLDER "flowie/tests")
cmake_target_enable_boringssl_linkage(test_flowie_cluster_peer_transport)
set_tests_properties(test_flowie_cluster_peer_transport
                     PROPERTIES LABELS "flowie;cluster;peer;transport;security" TIMEOUT 20)
if(WIN32)
  target_link_libraries(test_flowie_cluster_peer_transport PRIVATE ws2_32)
endif()

cmake_add_test(
  test_flowie_cluster_pgsql
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_pgsql.c
  LIBS flowie_cluster_pgsql PostgreSQL::PostgreSQL TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_pgsql PROPERTIES LABELS "flowie;cluster;pgsql")

cmake_add_test(
  test_flowie_cluster_pgsql_live
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_cluster_pgsql_live.c
  LIBS flowie_cluster_session tf_executor_common PostgreSQL::PostgreSQL TurboUtils::TinyTest
  FOLDER "flowie/tests")
set_tests_properties(test_flowie_cluster_pgsql_live
                     PROPERTIES LABELS "flowie;cluster;pgsql;integration;pgsql-live")
if(NOT TURBO_FLOW_PGSQL_LIVE_TESTS)
  set_tests_properties(test_flowie_cluster_pgsql_live PROPERTIES DISABLED TRUE)
endif()

cmake_add_test(
  test_flowie_ingress
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_ingress.c
  LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(test_flowie_ingress PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)

set_tests_properties(test_flowie_ingress PROPERTIES LABELS "flowie;flowie-release")

cmake_add_test(
  test_flowie_client_adapter
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_client_adapter.c
  LIBS Flowie::App TurboFlow::Flow TurboUtils::TinyTest
       $<$<PLATFORM_ID:Windows>:ws2_32>
  FOLDER "flowie/tests")

set_tests_properties(
  test_flowie_client_adapter
  PROPERTIES LABELS "flowie;flowie-release;mqtt;client;product-contract")

cmake_add_test(
  test_flowie_endpoint
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_endpoint.c
  LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "flowie/tests")

target_include_directories(
  test_flowie_endpoint PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}
                               ${CMAKE_CURRENT_SOURCE_DIR}/../src
                               ${PROJECT_SOURCE_DIR}/io/common/include
                               ${PROJECT_SOURCE_DIR}/io/socket/tests)

set_tests_properties(
  test_flowie_endpoint
  PROPERTIES LABELS
             "flowie;flowie-release;mqtt-fast;mqtt-negative;mqtt-persistence;mqtt-transport;mqtt-security"
             TIMEOUT 180)
target_compile_definitions(test_flowie_endpoint PRIVATE FLOWIE_SOURCE_DIR="${CMAKE_CURRENT_SOURCE_DIR}/..")

cmake_add_test(
  test_flowie_mqtt_endurance
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_mqtt_endurance.c
  LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}
  FOLDER "flowie/tests")

set_tests_properties(
  test_flowie_mqtt_endurance
  PROPERTIES LABELS "flowie;flowie-release;mqtt-endurance;mqtt-persistence"
             TIMEOUT 180)

cmake_add_test(
  test_flowie_session_store_faults
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_session_store_faults.c
  LIBS Flowie::App Flowie::Protocol TurboFlow::Flow TurboUtils::TinyTest
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR}
  FOLDER "flowie/tests")

target_include_directories(test_flowie_session_store_faults
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../src)

set_tests_properties(
  test_flowie_session_store_faults
  PROPERTIES LABELS
             "flowie;flowie-release;mqtt-negative;mqtt-persistence;persistence-contract"
             TIMEOUT 180)

cmake_add_test(
  test_flowie_transport
  SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/test_flowie_transport.c
  LIBS Flowie::App Flowie::Client Flowie::Protocol TurboFlow::Flow TurboNet::CoroNet
       TurboUtils::TinyTest OpenSSL::SSL OpenSSL::Crypto
  INCLUDES ${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/io/socket/tests
  FOLDER "flowie/tests")
cmake_target_enable_boringssl_linkage(test_flowie_transport)

set_tests_properties(
  test_flowie_transport
  PROPERTIES LABELS
             "flowie;flowie-release;transport-contract;mqtt-transport;mqtt-security;mqtt-negative"
             TIMEOUT 90)

if(WIN32)
  target_link_libraries(test_flowie_endpoint PRIVATE ws2_32)
  target_link_libraries(test_flowie_mqtt_endurance PRIVATE ws2_32)
  target_link_libraries(test_flowie_session_store_faults PRIVATE ws2_32)
  target_link_libraries(test_flowie_transport PRIVATE ws2_32)
endif()
