find_program(RE2C_EXECUTABLE NAMES re2c REQUIRED)
if(NOT EXISTS "${RE2C_EXECUTABLE}")
    message(FATAL_ERROR "FindTools: re2c executable does not exist: ${RE2C_EXECUTABLE}")
endif()

if(NOT TARGET lemon)
    message(FATAL_ERROR
            "FindTools: project-provided lemon target is required; system lemon fallback is not supported")
endif()
set(LEMON_EXECUTABLE $<TARGET_FILE:lemon>)
set(LEMON_DEPENDS lemon)

get_filename_component(
    LEMPAR "${CMAKE_CURRENT_LIST_DIR}/../tools/lemon/lempar.c" ABSOLUTE)
if(NOT EXISTS "${LEMPAR}")
    message(FATAL_ERROR "FindTools: project lemon template does not exist: ${LEMPAR}")
endif()

# Note: These variables are set in the root scope and will be inherited 
# by all subdirectories added via add_subdirectory().

message(STATUS "Tools detection:")
message(STATUS "  re2c: ${RE2C_EXECUTABLE}")
message(STATUS "  lemon: ${LEMON_EXECUTABLE} (project target)")
message(STATUS "  lempar: ${LEMPAR}")
