project("MIR")

if(NOT DEFINED CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE "RelWithDebInfo")
endif()

# set(CMAKE_VERBOSE_MAKEFILE ON)

if(NOT WIN32)
    add_compile_options(
        $<$<CONFIG:RELEASE>:-O3> $<$<CONFIG:RelWithDebInfo>:-O3>
    )
    add_compile_options(-std=gnu11 -Wno-abi -fsigned-char -fPIC)
endif()

include(CheckCCompilerFlag)
if(CMAKE_COMPILER_IS_GNUCC)
    check_c_compiler_flag(-fno-tree-sra NO_TREE_SRA)
    if(NO_TREE_SRA)
        add_compile_options(-fno-tree-sra)
    endif()
    check_c_compiler_flag(-fno-ipa-cp-clone NO_IPA_CP_CLONE)
    if(NO_IPA_CP_CLONE)
        add_compile_options(-fno-ipa-cp-clone)
    endif()
endif()

include(FindThreads)
if(Threads_FOUND)
    link_libraries(${CMAKE_THREAD_LIBS_INIT})
endif()

message("C compiler flags: ${CMAKE_C_FLAGS}")

option(BUILD_TESTING "" ON)
include(CTest)

add_library(
    mir OBJECT mir.c mir-gen.c c2mir/c2mir.c mir.h mir-gen.h c2mir/c2mir.h
)
target_include_directories(mir PUBLIC ${PROJECT_SOURCE_DIR})
if(Threads_FOUND)
    target_compile_definitions(mir PUBLIC "MIR_PARALLEL_GEN")
endif()
if(UNIX)
    target_link_libraries(mir m)
endif()
set_target_properties(mir PROPERTIES FOLDER vendor/mir)

# ------------------ LIBMIR -----------------------
add_library(mir_static STATIC)
target_link_libraries(mir_static PRIVATE mir)
set_target_properties(mir_static PROPERTIES FOLDER vendor/mir)

# ------------------ LIBMIR SO --------------------
add_library(mir_shared SHARED)
if(WIN32)
    set_target_properties(mir_shared PROPERTIES WINDOWS_EXPORT_ALL_SYMBOLS TRUE)
endif()
target_link_libraries(mir_shared PRIVATE mir)
set_target_properties(mir_shared PROPERTIES FOLDER vendor/mir)

option(MIR_BUILD_TOOLS "Build MIR tools" OFF)
if(MIR_BUILD_TOOLS)
    add_subdirectory(c2mir)

    # ------------------ L2M --------------------------
    # find_package(LLVM) if(LLVM_FOUND) add_executable (l2m "llvm2mir/llvm2mir.c"
    # "llvm2mir/llvm2mir-driver.c" "llvm2mir/llvm2mir.h")
    # target_include_directories(l2m PRIVATE ${PROJECT_SOURCE_DIR}
    # ${LLVM_INCLUDE_DIRS}) target_link_libraries(l2m mir m ${CMAKE_DL_LIBS} LLVM)
    # set_target_properties(l2m PROPERTIES FOLDER vendor/mir/utils) endif()

    add_subdirectory(mir-utils)
endif()

option(MIR_BUILD_TESTS "Build MIR tests" OFF)
if(MIR_BUILD_TESTS)
    add_subdirectory(mir-tests)
endif()
