add_library(reed STATIC gf256.c gf256.h)

target_include_directories(
  reed PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>)

find_path(SIMDE_INCLUDE_DIRS "simde/simde-common.h" REQUIRED)
target_include_directories(reed PRIVATE ${SIMDE_INCLUDE_DIRS})

set_target_properties(
  reed
  PROPERTIES C_STANDARD 11
             C_STANDARD_REQUIRED ON
             FOLDER vendor/reed)

cmake_add_test(
  SOURCES tests/test_gf256.c
  LIBS reed TurboUtils::TinyTest
  FOLDER "vendor/reed/test")
