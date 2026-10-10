find_program(RE2C_EXECUTABLE NAMES re2c REQUIRED)
if(NOT EXISTS "${RE2C_EXECUTABLE}")
    message(FATAL_ERROR "FindTools: re2c executable does not exist: ${RE2C_EXECUTABLE}")
endif()

if(CMAKE_CROSSCOMPILING)
    # CI builds lemon.c with the native host compiler, separately from the
    # target CMake toolchain. No target-ABI tool execution or system fallback.
    if(NOT DEFINED TURBO_FLOW_LEMON_HOST_EXECUTABLE OR
       NOT IS_ABSOLUTE "${TURBO_FLOW_LEMON_HOST_EXECUTABLE}" OR
       NOT EXISTS "${TURBO_FLOW_LEMON_HOST_EXECUTABLE}")
        message(FATAL_ERROR
                "FindTools: cross builds require absolute TURBO_FLOW_LEMON_HOST_EXECUTABLE")
    endif()
    set(LEMON_EXECUTABLE "${TURBO_FLOW_LEMON_HOST_EXECUTABLE}")
    set(LEMON_DEPENDS "${TURBO_FLOW_LEMON_HOST_EXECUTABLE}")
else()
    if(NOT TARGET lemon)
        message(FATAL_ERROR
                "FindTools: project-provided lemon target is required; system lemon fallback is not supported")
    endif()
    set(LEMON_EXECUTABLE $<TARGET_FILE:lemon>)
    set(LEMON_DEPENDS lemon)
endif()

get_filename_component(
    LEMPAR "${CMAKE_CURRENT_LIST_DIR}/../tools/lemon/lempar.c" ABSOLUTE)
if(NOT EXISTS "${LEMPAR}")
    message(FATAL_ERROR "FindTools: project lemon template does not exist: ${LEMPAR}")
endif()

# Note: These variables are set in the root scope and will be inherited 
# by all subdirectories added via add_subdirectory().

message(STATUS "Tools detection:")
message(STATUS "  re2c: ${RE2C_EXECUTABLE}")
message(STATUS "  lemon: ${LEMON_EXECUTABLE} (qualified host tool)")
message(STATUS "  lempar: ${LEMPAR}")
