set(TARGET_NAME tf_pgsql)
file(GLOB TURBO_FLOW_PGSQL_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)
set(TURBO_FLOW_PGSQL_HEADERS
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_pgsql.h
    ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_pgsql_storage_backend.h)
add_library(${TARGET_NAME} SHARED ${TURBO_FLOW_PGSQL_SOURCES} ${TURBO_FLOW_PGSQL_HEADERS})
cmake_config_target(${TARGET_NAME} ALIAS TurboFlow::PostgreSQL FOLDER "io/pgsql"
                    EXPORT_NAME PostgreSQL)
# The provider sources are C, but libpq/BoringSSL and tf_executor_common bring C++ ABI
# objects into the dependency closure. Select the C++ linker so libstdc++ is recorded.
cmake_target_enable_boringssl_linkage(${TARGET_NAME})
target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(${TARGET_NAME} PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                                 $<INSTALL_INTERFACE:include>)
target_link_libraries(${TARGET_NAME} PUBLIC TurboFlow::Flow TurboFlow::StorageBackend
                                    PRIVATE tf_executor_common PostgreSQL::PostgreSQL TurboUtils::Core
                                            TurboUtils::Parser)
install(FILES ${TURBO_FLOW_PGSQL_HEADERS} DESTINATION include)
install(FILES ${CMAKE_CURRENT_SOURCE_DIR}/examples/pgsql.yml
        DESTINATION share/turboflow/examples/pgsql)
if(BUILD_TESTS)
  add_subdirectory(tests)
endif()

