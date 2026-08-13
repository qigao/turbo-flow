set(TARGET_NAME tf_flow_store_databind)

add_library(${TARGET_NAME} SHARED src/flow_databind_store.c
                                 include/turbo_flow_databind_store.h)
cmake_config_target(${TARGET_NAME}
                    ALIAS TurboFlow::FlowStoreDataBind
                    FOLDER "flowstore/databind"
                    EXPORT_NAME FlowStoreDataBind)
target_compile_definitions(${TARGET_NAME} PRIVATE SHARED_CXX)
target_include_directories(${TARGET_NAME}
                           PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                                  $<INSTALL_INTERFACE:include>
                           PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src)
target_link_libraries(${TARGET_NAME}
                      PUBLIC TurboFlow::FlowStore TurboParser::DataBind TurboParser::QueryVM)
install(FILES include/turbo_flow_databind_store.h DESTINATION include)
