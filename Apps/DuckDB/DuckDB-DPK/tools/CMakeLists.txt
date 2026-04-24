if(NOT SUN AND BUILD_SHELL)
  add_subdirectory(sqlite3_api_wrapper)
  add_subdirectory(shell)
endif()

# DDS parquet preload utility (requires DDS POSIX frontend).
if(DUCKDB_USE_DDS_POSIX)
  add_executable(dds_parquet_loader utils/dds_parquet_loader.cpp)
  target_include_directories(dds_parquet_loader
                             PRIVATE
                             ${DUCKDB_DDS_POSIX_DIR}
                             ${DUCKDB_DDS_COMMON_INCLUDE_DIR}
                             ${DUCKDB_DDS_COMMON_INCLUDE_DIR}/Host)
  target_link_libraries(dds_parquet_loader PRIVATE ${DUCKDB_DDS_POSIX_LIBS} ${DUCKDB_SYSTEM_LIBS})
endif()

# RDMA query prototype tools.
if(BUILD_RDMA_QUERY_TOOLS)
  find_library(DUCKDB_RDMA_IBVERBS_LIB NAMES ibverbs)
  find_library(DUCKDB_RDMA_RDMACM_LIB NAMES rdmacm PATHS /lib/x86_64-linux-gnu)
  if(NOT DUCKDB_RDMA_IBVERBS_LIB)
    message(FATAL_ERROR "ibverbs not found but BUILD_RDMA_QUERY_TOOLS is ON")
  endif()
  if(NOT DUCKDB_RDMA_RDMACM_LIB)
    message(FATAL_ERROR "rdmacm not found but BUILD_RDMA_QUERY_TOOLS is ON")
  endif()

  add_library(rdma_query_transport STATIC utils/rdma_query_transport.cpp)
  target_include_directories(rdma_query_transport PRIVATE ${PROJECT_SOURCE_DIR}/src/include)
  target_link_libraries(rdma_query_transport PUBLIC Threads::Threads ${DUCKDB_RDMA_IBVERBS_LIB} ${DUCKDB_RDMA_RDMACM_LIB})

  add_executable(duckdb_rdma_server utils/rdma_query_server.cpp)
  target_include_directories(duckdb_rdma_server PRIVATE ${PROJECT_SOURCE_DIR}/src/include)
  target_link_libraries(duckdb_rdma_server PRIVATE duckdb rdma_query_transport)

  add_executable(duckdb_rdma_client utils/rdma_query_client.cpp)
  target_link_libraries(duckdb_rdma_client PRIVATE rdma_query_transport)
endif()
