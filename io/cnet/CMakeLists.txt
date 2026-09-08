add_library(tf_cnet_adapter SHARED
  src/turbo_flow_cnet_datagram_sink.c
  src/turbo_flow_cnet_stream_sink.c
  src/turbo_flow_cnet_listener_source.c
  src/turbo_flow_cnet_packet_sink.c
  src/turbo_flow_cnet_packet_source.c
  src/turbo_flow_cnet_stream_source.c
  include/turbo_flow_cnet.h)
cmake_config_target(
  tf_cnet_adapter
  ALIAS TurboFlow::CNetAdapter
  FOLDER "io/cnet"
  EXPORT_NAME CNetAdapter)
target_compile_definitions(tf_cnet_adapter PRIVATE TURBO_FLOW_BUILD)
target_include_directories(
  tf_cnet_adapter
  PRIVATE $<TARGET_PROPERTY:xxHash::xxhash,INTERFACE_INCLUDE_DIRECTORIES>
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)
target_link_libraries(tf_cnet_adapter PUBLIC TurboFlow::Graph Salts::CNet Salts::CFlow Salts::CSTL)

install(FILES include/turbo_flow_cnet.h DESTINATION include)

add_library(tf_cnet_plugin SHARED
  src/turbo_flow_cnet_plugin.c
  src/turbo_flow_cnet_plugin_config.c
  src/turbo_flow_cnet_plugin_internal.h)
cmake_config_target(tf_cnet_plugin FOLDER "io/cnet")
target_compile_definitions(tf_cnet_plugin PRIVATE TURBO_FLOW_PLUGIN_BUILD)
target_include_directories(tf_cnet_plugin PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include
                                                  ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(tf_cnet_plugin PRIVATE tf_cnet_adapter TurboFlow::Product Salts::CSTL
                                             Salts::UriParser)
if(WIN32)
  find_package(OpenSSL REQUIRED)
  target_link_libraries(tf_cnet_plugin PRIVATE ws2_32)
endif()
set_target_properties(tf_cnet_plugin PROPERTIES C_VISIBILITY_PRESET hidden
                                               VISIBILITY_INLINES_HIDDEN YES)
install(TARGETS tf_cnet_plugin RUNTIME DESTINATION bin LIBRARY DESTINATION lib)
if(WIN32)
  if(NOT CMAKE_BUILD_TYPE STREQUAL "Debug" AND NOT CMAKE_BUILD_TYPE STREQUAL "Release")
    message(FATAL_ERROR "CNet plugin packaging requires an explicit Debug or Release build")
  endif()
  if(NOT DEFINED VCPKG_INSTALLED_DIR OR NOT DEFINED VCPKG_TARGET_TRIPLET)
    message(FATAL_ERROR "CNet plugin packaging requires explicit vcpkg runtime roots")
  endif()
  if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    set(cnet_tls_runtime_dir
        "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/debug/bin")
    set(cnet_tls_runtime_names ssld.dll cryptod.dll)
  else()
    set(cnet_tls_runtime_dir "${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/bin")
    set(cnet_tls_runtime_names ssl.dll crypto.dll)
  endif()
  set(cnet_tls_runtime_files)
  foreach(cnet_tls_runtime_name IN LISTS cnet_tls_runtime_names)
    set(cnet_tls_runtime_file "${cnet_tls_runtime_dir}/${cnet_tls_runtime_name}")
    if(NOT EXISTS "${cnet_tls_runtime_file}")
      message(FATAL_ERROR "Required CNet TLS runtime is missing: ${cnet_tls_runtime_file}")
    endif()
    list(APPEND cnet_tls_runtime_files "${cnet_tls_runtime_file}")
  endforeach()
  install(FILES $<TARGET_RUNTIME_DLLS:tf_cnet_plugin> ${cnet_tls_runtime_files} DESTINATION bin)
endif()

if(BUILD_TESTING)
  add_subdirectory(tests)
endif()

if(BUILD_TESTING AND BUILD_BENCHMARKS)
  add_subdirectory(benchmarks)
endif()
