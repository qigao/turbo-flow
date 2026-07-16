add_executable(m2b "m2b.c")
target_include_directories(m2b PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(m2b mir)
set_target_properties(m2b PROPERTIES FOLDER vendor/mir/utils)

add_executable(b2m "b2m.c")
target_include_directories(b2m PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(b2m mir)
set_target_properties(b2m PROPERTIES FOLDER vendor/mir/utils)

add_executable(b2ctab "b2ctab.c")
target_include_directories(b2ctab PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(b2ctab mir)
set_target_properties(b2ctab PROPERTIES FOLDER vendor/mir/utils)
