add_library(
  flowie_cluster_runtime STATIC
  flowie_cluster_endpoint_binding.c
  flowie_cluster_endpoint_binding_internal.h
  flowie_cluster_generation.c
  flowie_cluster_generation_internal.h
  flowie_cluster_member_directory.c
  flowie_cluster_member_directory_internal.h
  flowie_cluster_membership_runtime.c
  flowie_cluster_membership_runtime_internal.h
  flowie_cluster_node.c
  flowie_cluster_node_internal.h
  flowie_cluster_owner_directory.c
  flowie_cluster_owner_directory_internal.h
  flowie_cluster_owner_directory_runtime.c
  flowie_cluster_owner_directory_runtime_internal.h
  flowie_cluster_peer_authority.c
  flowie_cluster_peer_authority_internal.h
  flowie_cluster_redis_bus.c
  flowie_cluster_redis_bus_internal.h)
cmake_config_target(flowie_cluster_runtime NO_INSTALL FOLDER "flowie/cluster")
target_include_directories(
  flowie_cluster_runtime
  PUBLIC ${CMAKE_CURRENT_SOURCE_DIR} ${PROJECT_SOURCE_DIR}/flowie/src)
target_link_libraries(
  flowie_cluster_runtime
  PUBLIC flowie_cluster_session Flowie::App tf_executor_common TurboFlow::Redis TurboUtils::Core
  PRIVATE monocypher)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
