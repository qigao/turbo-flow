# Monocypher - A high-quality cryptographic library https://monocypher.org/

add_library(monocypher STATIC monocypher.c monocypher.h)

target_include_directories(
  monocypher PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

set_target_properties(monocypher PROPERTIES FOLDER "vendor")
