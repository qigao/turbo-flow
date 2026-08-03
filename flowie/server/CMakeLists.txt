if(NOT TARGET flowie_control_runtime)
  message(FATAL_ERROR
          "flowie_server requires the Auth/ACL Control runtime (SQLite, HTTP server, and RPC)")
endif()

add_library(flowie_server_application STATIC
            flowie_server_application.c
            flowie_server_application_internal.h
            flowie_worker_runtime.c
            flowie_worker_runtime_internal.h)
cmake_config_target(flowie_server_application FOLDER "flowie/server")
target_link_libraries(flowie_server_application PUBLIC Flowie::App TurboFlow::Socket
                                                       TurboFlow::LocalStorage TurboFlow::Redis
                                                       TurboFlow::PostgreSQL TurboFlow::SQLiteStorage
                                                       TurboFlow::HttpClient
                                               TurboFlow::HttpServer flowie_control_runtime
                                               TurboUtils::Core
                                               PRIVATE flowie_cluster_runtime)

add_executable(flowie_server flowie_server.c)
cmake_config_target(flowie_server FOLDER "flowie/server")
target_link_libraries(flowie_server PRIVATE flowie_server_application TurboUtils::Core)
cmake_target_enable_boringssl_linkage(flowie_server)

add_executable(flowie_supervisor flowie_supervisor.c flowie_supervisor_runtime.c
                                 flowie_supervisor_runtime_internal.h)
cmake_config_target(flowie_supervisor FOLDER "flowie/server")
target_link_libraries(flowie_supervisor PRIVATE TurboUtils::Core)

install(TARGETS flowie_server flowie_supervisor RUNTIME DESTINATION bin)
install(FILES "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.yml"
              "${PROJECT_SOURCE_DIR}/flowie/examples/flowie.flow"
        DESTINATION share/turboflow/examples/flowie)
install(DIRECTORY "${PROJECT_SOURCE_DIR}/flowie/deploy/nginx/"
        DESTINATION share/turboflow/deploy/flowie-nginx)
install(DIRECTORY "${PROJECT_SOURCE_DIR}/flowie/deploy/haproxy/"
        DESTINATION share/turboflow/deploy/flowie-haproxy)
install(DIRECTORY "${PROJECT_SOURCE_DIR}/flowie/deploy/server/"
        DESTINATION share/turboflow/deploy/flowie-server)
install(FILES "${PROJECT_SOURCE_DIR}/flowie/deploy/provision-root.ps1"
        DESTINATION share/turboflow/deploy)
install(
  FILES "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-dev.yml"
        "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-dev.flow"
        "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-smb.yml"
        "${PROJECT_SOURCE_DIR}/flowie/examples/products/flowie-smb.flow"
  DESTINATION share/turboflow/examples/flowie/products)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
