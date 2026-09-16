add_executable(simplify_test "simplify.c")
target_link_libraries(simplify_test mir)
set_target_properties(simplify_test PROPERTIES FOLDER vendor/mir/tests)

add_executable(scan_test "scan-test.c")
target_link_libraries(scan_test mir)
set_target_properties(scan_test PROPERTIES FOLDER vendor/mir/tests)

add_executable(io_test "io.c")
target_link_libraries(io_test mir)
set_target_properties(io_test PROPERTIES FOLDER vendor/mir/tests)

add_test(simplify-test simplify_test)
add_test(scan-test scan_test)
add_test(io-test io_test)

add_executable(run_test "run-test.c")
target_include_directories(run_test PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(run_test mir)
set_target_properties(run_test PROPERTIES FOLDER vendor/mir/tests)

add_executable(interp_loop "loop-interp.c")
target_include_directories(interp_loop PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_definitions(interp_loop PRIVATE MIR_INTERP_DEBUG=1)
target_link_libraries(interp_loop mir)
set_target_properties(interp_loop PROPERTIES FOLDER vendor/mir/tests)

add_executable(interp_loop_c "loop-interp.c")
target_include_directories(interp_loop_c PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_definitions(
    interp_loop_c PRIVATE MIR_INTERP_DEBUG=1 MIR_C_INTERFACE=1
)
target_link_libraries(interp_loop_c mir)
set_target_properties(interp_loop_c PROPERTIES FOLDER vendor/mir/tests)

add_executable(interp_sieve "sieve-interp.c")
target_include_directories(interp_sieve PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_definitions(interp_sieve PRIVATE MIR_INTERP_DEBUG=1)
target_link_libraries(interp_sieve mir)
set_target_properties(interp_sieve PROPERTIES FOLDER vendor/mir/tests)

add_executable(interp_sieve_c "sieve-interp.c")
target_include_directories(interp_sieve_c PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_definitions(
    interp_sieve_c PRIVATE MIR_INTERP_DEBUG=1 MIR_C_INTERFACE=1
)
target_link_libraries(interp_sieve_c mir)
set_target_properties(interp_sieve_c PROPERTIES FOLDER vendor/mir/tests)

add_executable(interp_hi "hi-interp.c")
target_include_directories(interp_hi PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_definitions(interp_hi PRIVATE MIR_INTERP_DEBUG=1)
target_link_libraries(interp_hi mir)
set_target_properties(interp_hi PROPERTIES FOLDER vendor/mir/tests)

add_executable(interp_args "args-interp.c")
target_include_directories(interp_args PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(interp_args mir)
set_target_properties(interp_args PROPERTIES FOLDER vendor/mir/tests)

add_executable(interp_args_c "args-interp.c")
target_include_directories(interp_args_c PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_definitions(interp_args_c PRIVATE MIR_C_INTERFACE=1)
target_link_libraries(interp_args_c mir)
set_target_properties(interp_args_c PROPERTIES FOLDER vendor/mir/tests)

add_executable(run_test_d "run-test.c")
target_include_directories(run_test_d PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_definitions(run_test_d PRIVATE TEST_GEN_DEBUG=1)
target_link_libraries(run_test_d mir)
set_target_properties(run_test_d PROPERTIES FOLDER vendor/mir/tests)

add_test(interp-test1 interp_loop)
add_test(interp-test2 interp_loop_c)
add_test(interp-test3 interp_sieve)
add_test(interp-test4 interp_sieve_c)
add_test(interp-test5 interp_hi)
add_test(interp-test6 interp_args)
add_test(interp-test7 interp_args_c)

foreach(num 8 9 10)
    add_test(interp-test${num} run_test -i
             ${PROJECT_SOURCE_DIR}/mir-tests/test${num}.mir
    )
endforeach()

if(NOT WIN32)
    add_test(interp-test11 run_test -i
             ${PROJECT_SOURCE_DIR}/mir-tests/test11.mir
    ) 
    add_test(interp-test12 run_test -i
             ${PROJECT_SOURCE_DIR}/mir-tests/test12.mir
    ) 
endif()

foreach(num 13 14 15 16)
    add_test(interp-test${num} run_test -i
             ${PROJECT_SOURCE_DIR}/mir-tests/test${num}.mir
    )
endforeach()

foreach(
    num
    1
    2
    3
    4
    5
    6
    7
)
    add_test(gen-test${num} run_test -d
             ${PROJECT_SOURCE_DIR}/mir-tests/test${num}.mir
    )
endforeach()

foreach(num 8 9 10)
    add_test(gen-test${num} run_test -g
             ${PROJECT_SOURCE_DIR}/mir-tests/test${num}.mir
    )
endforeach()

if(NOT WIN32)
    add_test(gen-test11 run_test -g ${PROJECT_SOURCE_DIR}/mir-tests/test11.mir
    )
    add_test(gen-test12 run_test -g ${PROJECT_SOURCE_DIR}/mir-tests/test12.mir
    )
endif()

foreach(num 13 14 15 16)
    add_test(gen-test${num} run_test -g
             ${PROJECT_SOURCE_DIR}/mir-tests/test${num}.mir
    )
endforeach()

add_executable(gen_loop "loop-sieve-gen.c")
target_include_directories(gen_loop PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_definitions(gen_loop PRIVATE TEST_GEN_DEBUG=1 TEST_GEN_LOOP)
target_link_libraries(gen_loop mir)
set_target_properties(gen_loop PROPERTIES FOLDER vendor/mir/tests)

add_executable(gen_sieve "loop-sieve-gen.c")
target_include_directories(gen_sieve PRIVATE ${PROJECT_SOURCE_DIR})
target_compile_definitions(gen_sieve PRIVATE TEST_GEN_DEBUG=1 TEST_GEN_SIEVE)
target_link_libraries(gen_sieve mir)
set_target_properties(gen_sieve PROPERTIES FOLDER vendor/mir/tests)

add_test(gen-test-loop gen_loop)
add_test(gen-test-sieve gen_sieve)

add_executable(readme_example "readme-example.c")
target_include_directories(readme_example PRIVATE ${PROJECT_SOURCE_DIR})
target_link_libraries(readme_example mir)
set_target_properties(readme_example PROPERTIES FOLDER vendor/mir/examples)

add_test(readme-example-test readme_example)
