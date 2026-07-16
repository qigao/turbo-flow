cmake_minimum_required(VERSION 3.12)
project(uuid LANGUAGES C)

# Create a static library target
add_library(uuid STATIC uuid.c uuid.h)

# Include the current directory so that the header can be found
target_include_directories(uuid
    PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        $<INSTALL_INTERFACE:include>
)

# Set the C standard to C99
set_target_properties(uuid PROPERTIES
    C_STANDARD 99
    C_STANDARD_REQUIRED ON
    POSITION_INDEPENDENT_CODE ON
     FOLDER "vendor/uuid"
)

if(BUILD_TESTING)
    add_executable(uuid_test tests/uuid_test.c)
    target_link_libraries(uuid_test PRIVATE uuid TurboUtils::TinyTest)
    add_test(NAME uuid_test COMMAND uuid_test)
    set_target_properties(uuid_test PROPERTIES FOLDER "vendor/uuid")
endif()
