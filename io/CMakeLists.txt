if(TURBO_FLOW_BUILD_SOCKET
   OR TURBO_FLOW_BUILD_HTTP_CLIENT
   OR TURBO_FLOW_BUILD_HTTP_SERVER
   OR TURBO_FLOW_BUILD_RPC
   OR TURBO_FLOW_BUILD_S3
   OR TURBO_FLOW_BUILD_EMAIL
   OR TURBO_FLOW_BUILD_FMQ
   OR TURBO_FLOW_BUILD_REDIS)
  add_subdirectory(common)
endif()
if(TURBO_FLOW_BUILD_HTTP_CLIENT OR TURBO_FLOW_BUILD_HTTP_SERVER)
  add_subdirectory(http)
endif()
if(TURBO_FLOW_BUILD_RPC)
  add_subdirectory(rpc)
endif()
if(TURBO_FLOW_BUILD_S3)
  add_subdirectory(s3)
endif()
if(TURBO_FLOW_BUILD_REDIS)
  add_subdirectory(redis)
endif()
if(TURBO_FLOW_BUILD_PGSQL)
  add_subdirectory(pgsql)
endif()
if(TURBO_FLOW_BUILD_SOCKET)
  add_subdirectory(socket)
endif()
if(TURBO_FLOW_BUILD_EMAIL)
  add_subdirectory(email)
endif()
