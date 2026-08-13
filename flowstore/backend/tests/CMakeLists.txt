cmake_add_test(
  test_storage_backend_registry
  SOURCES test_storage_backend_registry.c
  LIBS TurboFlow::StorageBackend TurboUtils::TinyTest
  FOLDER "flowstore/backend/tests")

set_tests_properties(test_storage_backend_registry
                     PROPERTIES LABELS "storage-contract;plugin-contract")
