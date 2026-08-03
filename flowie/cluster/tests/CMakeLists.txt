cmake_add_test(
  test_flowie_cluster_generation
  SOURCES test_flowie_cluster_generation.c
  LIBS flowie_cluster_runtime TurboUtils::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowie/cluster ${PROJECT_SOURCE_DIR}/flowie/src
  FOLDER "flowie/cluster/tests")
cmake_target_enable_boringssl_linkage(test_flowie_cluster_generation)
if(WIN32)
  target_link_libraries(test_flowie_cluster_generation PRIVATE ws2_32)
endif()
set_tests_properties(
  test_flowie_cluster_generation
  PROPERTIES LABELS "flowie;flowie-release;cluster;composition;ownership;recovery" TIMEOUT 30)

cmake_add_test(
  test_flowie_cluster_redis_bus
  SOURCES test_flowie_cluster_redis_bus.c
  LIBS flowie_cluster_runtime TurboUtils::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowie/cluster ${PROJECT_SOURCE_DIR}/flowie/src
  FOLDER "flowie/cluster/tests")
cmake_target_enable_boringssl_linkage(test_flowie_cluster_redis_bus)
if(WIN32)
  target_link_libraries(test_flowie_cluster_redis_bus PRIVATE ws2_32)
endif()
set_tests_properties(
  test_flowie_cluster_redis_bus
  PROPERTIES LABELS "flowie;flowie-release;cluster;redis-transport")

cmake_add_test(
  test_flowie_cluster_node
  SOURCES test_flowie_cluster_node.c
  LIBS flowie_cluster_runtime TurboUtils::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowie/cluster ${PROJECT_SOURCE_DIR}/flowie/src
  FOLDER "flowie/cluster/tests")
cmake_target_enable_boringssl_linkage(test_flowie_cluster_node)
if(WIN32)
  target_link_libraries(test_flowie_cluster_node PRIVATE ws2_32)
endif()
set_tests_properties(
  test_flowie_cluster_node
  PROPERTIES LABELS "flowie;flowie-release;cluster;composition;ownership")

cmake_add_test(
  test_flowie_cluster_peer_authority
  SOURCES test_flowie_cluster_peer_authority.c
  LIBS flowie_cluster_runtime TurboUtils::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowie/cluster ${PROJECT_SOURCE_DIR}/flowie/src
  FOLDER "flowie/cluster/tests")
cmake_target_enable_boringssl_linkage(test_flowie_cluster_peer_authority)
if(WIN32)
  target_link_libraries(test_flowie_cluster_peer_authority PRIVATE ws2_32)
endif()
set_tests_properties(
  test_flowie_cluster_peer_authority
  PROPERTIES LABELS "flowie;flowie-release;cluster;membership;security-contract")

cmake_add_test(
  test_flowie_cluster_owner_directory
  SOURCES test_flowie_cluster_owner_directory.c
  LIBS flowie_cluster_runtime TurboUtils::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowie/cluster ${PROJECT_SOURCE_DIR}/flowie/src
  FOLDER "flowie/cluster/tests")
cmake_target_enable_boringssl_linkage(test_flowie_cluster_owner_directory)
if(WIN32)
  target_link_libraries(test_flowie_cluster_owner_directory PRIVATE ws2_32)
endif()
set_tests_properties(
  test_flowie_cluster_owner_directory
  PROPERTIES LABELS "flowie;flowie-release;cluster;ownership;security-contract")

cmake_add_test(
  test_flowie_cluster_owner_directory_runtime
  SOURCES test_flowie_cluster_owner_directory_runtime.c
  LIBS flowie_cluster_runtime TurboUtils::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowie/cluster ${PROJECT_SOURCE_DIR}/flowie/src
  FOLDER "flowie/cluster/tests")
cmake_target_enable_boringssl_linkage(test_flowie_cluster_owner_directory_runtime)
if(WIN32)
  target_link_libraries(test_flowie_cluster_owner_directory_runtime PRIVATE ws2_32)
endif()
set_tests_properties(
  test_flowie_cluster_owner_directory_runtime
  PROPERTIES LABELS "flowie;flowie-release;cluster;ownership;recovery")

cmake_add_test(
  test_flowie_cluster_membership_runtime
  SOURCES test_flowie_cluster_membership_runtime.c
  LIBS flowie_cluster_runtime TurboUtils::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowie/cluster ${PROJECT_SOURCE_DIR}/flowie/src
  FOLDER "flowie/cluster/tests")
cmake_target_enable_boringssl_linkage(test_flowie_cluster_membership_runtime)
if(WIN32)
  target_link_libraries(test_flowie_cluster_membership_runtime PRIVATE ws2_32)
endif()
set_tests_properties(
  test_flowie_cluster_membership_runtime
  PROPERTIES LABELS "flowie;flowie-release;cluster;membership;recovery")

cmake_add_test(
  test_flowie_cluster_member_directory
  SOURCES test_flowie_cluster_member_directory.c
  LIBS flowie_cluster_runtime TurboUtils::TinyTest
  INCLUDES ${PROJECT_SOURCE_DIR}/flowie/cluster ${PROJECT_SOURCE_DIR}/flowie/src
  FOLDER "flowie/cluster/tests")
set_tests_properties(
  test_flowie_cluster_member_directory
  PROPERTIES LABELS "flowie;flowie-release;cluster;membership;route;concurrency")
