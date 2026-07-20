cmake_add_test(
  test_flow_security_sqlite
  SOURCES test_flow_security_sqlite.c
  LIBS TurboFlow::SecuritySQLite TurboFlow::Flow TurboUtils::TinyTest
  FOLDER "security/tests")
