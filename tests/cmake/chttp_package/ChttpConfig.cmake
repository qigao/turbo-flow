include(CMakeFindDependencyMacro)
find_dependency(Salts CONFIG REQUIRED PATHS "$ENV{SALTS_ROOT}" NO_DEFAULT_PATH)
include("${CMAKE_CURRENT_LIST_DIR}/ChttpTargets.cmake")
