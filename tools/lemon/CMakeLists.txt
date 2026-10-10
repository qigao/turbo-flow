# Parser generation runs on the build host. The target-ABI executable must
# never be invoked during a cross build.
if(CMAKE_CROSSCOMPILING)
  return()
endif()

add_executable(lemon lemon.c)
target_compile_features(lemon PRIVATE c_std_11)

set_target_properties(lemon PROPERTIES FOLDER tools)
