add_executable(c2m "c2mir-driver.c")
target_include_directories(c2m PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(c2m mir ${CMAKE_DL_LIBS})
set_target_properties(c2m PROPERTIES FOLDER vendor/mir/utils)
