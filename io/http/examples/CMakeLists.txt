add_executable(turbo_flow_http_rulesforge_todo http_rulesforge_todo.c)
cmake_config_target(turbo_flow_http_rulesforge_todo
                    FOLDER "io/http/examples")

target_compile_definitions(
  turbo_flow_http_rulesforge_todo
  PRIVATE TODO_ASSET_DIR="${CMAKE_CURRENT_SOURCE_DIR}"
          TODO_RULE_PATH="${CMAKE_CURRENT_SOURCE_DIR}/public_todo.rfl")

target_link_libraries(
  turbo_flow_http_rulesforge_todo
  PRIVATE TurboFlow::HttpClient
          TurboFlow::Codec
          TurboFlow::Flow
          TurboUtils::Core)
