set(TARGET_NAME tf_codec)

file(GLOB TURBO_FLOW_CODEC_SOURCES CONFIGURE_DEPENDS ${CMAKE_CURRENT_SOURCE_DIR}/src/*.c)
set(TURBO_FLOW_CODEC_HEADERS ${CMAKE_CURRENT_SOURCE_DIR}/include/turbo_flow_codec.h)

add_library(${TARGET_NAME} SHARED
  ${TURBO_FLOW_CODEC_SOURCES}
  ${TURBO_FLOW_CODEC_HEADERS})

cmake_config_target(
  ${TARGET_NAME}
  ALIAS TurboFlow::Codec
  FOLDER "codec"
  EXPORT_NAME Codec)

target_compile_definitions(${TARGET_NAME} PRIVATE TURBO_FLOW_BUILD)

target_include_directories(
  ${TARGET_NAME}
  PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
         $<INSTALL_INTERFACE:include>)

target_link_libraries(${TARGET_NAME}
  PUBLIC TurboFlow::Graph Salts::Core Salts::DataBind
  PRIVATE Salts::CsvParser)

install(FILES ${TURBO_FLOW_CODEC_HEADERS} DESTINATION include)

if(BUILD_TESTS)
  add_subdirectory(tests)
endif()
