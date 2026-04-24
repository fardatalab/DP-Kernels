add_definitions(-DDUCKDB)

if(${DISABLE_THREADS})
  add_definitions(-DDUCKDB_NO_THREADS)
endif()

add_extension_definitions()

if(DUCKDB_USE_DDS_POSIX)
  add_definitions(-DDUCKDB_USE_DDS_POSIX)
  include_directories(${DUCKDB_DDS_POSIX_DIR} ${DUCKDB_DDS_COMMON_INCLUDE_DIR}
                      ${DUCKDB_DDS_COMMON_INCLUDE_DIR}/Host)
endif()

if(NOT MSVC)
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wextra -Wno-unused-parameter -Wno-redundant-move"
  )
  if(CMAKE_COMPILER_IS_GNUCC)
    if(CMAKE_CXX_COMPILER_VERSION VERSION_GREATER 6)
      set(CMAKE_CXX_FLAGS_DEBUG
          "${CMAKE_CXX_FLAGS_DEBUG} -Wimplicit-fallthrough")
    endif()
  else()
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -Wimplicit-fallthrough")
  endif()
endif()
set(EXIT_TIME_DESTRUCTORS_WARNING FALSE)
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang" OR "${CMAKE_CXX_COMPILER_ID}"
                                                  STREQUAL "AppleClang")
  set(EXIT_TIME_DESTRUCTORS_WARNING TRUE)
  set(CMAKE_CXX_FLAGS_DEBUG
      "${CMAKE_CXX_FLAGS_DEBUG} -Wexit-time-destructors -Wimplicit-int-conversion -Wshorten-64-to-32 -Wnarrowing -Wsign-conversion -Wsign-compare -Wconversion"
  )
endif()

set(DUCKDB_SYSTEM_LIBS ${CMAKE_DL_LIBS})

if(MSVC OR MINGW)
  set(DUCKDB_SYSTEM_LIBS ${DUCKDB_SYSTEM_LIBS} ws2_32 rstrtmgr)
endif()
if(MSVC)
  set(DUCKDB_SYSTEM_LIBS ${DUCKDB_SYSTEM_LIBS} bcrypt)
endif()

# DDS POSIX frontend (static lib) for Parquet I/O.
set(DUCKDB_DDS_POSIX_LIBS "")
if(DUCKDB_USE_DDS_POSIX)
  if(NOT EXISTS "${DUCKDB_DDS_POSIX_DIR}/DDSPosix.cpp")
    message(FATAL_ERROR "DDS POSIX frontend not found at ${DUCKDB_DDS_POSIX_DIR}")
  endif()
  # Original single-source build (kept for reference).
  # add_library(duckdb_dds_posix STATIC "${DUCKDB_DDS_POSIX_DIR}/DDSPosix.cpp")
  # target_include_directories(duckdb_dds_posix
  #                            PUBLIC
  #                            "${DUCKDB_DDS_POSIX_DIR}"
  #                            "${DUCKDB_DDS_COMMON_INCLUDE_DIR}"
  #                            "${DUCKDB_DDS_COMMON_INCLUDE_DIR}/Host")

  # Build DDS POSIX frontend with the same sources/includes as the Meson tests.
  set(DUCKDB_DDS_POSIX_SOURCES
      "${DUCKDB_DDS_POSIX_DIR}/DDSPosix.cpp"
      "${DUCKDB_DDS_COMMON_SOURCE_HOST_DIR}/LinkedList.cpp"
      "${DUCKDB_DDS_COMMON_SOURCE_HOST_DIR}/DMABuffer.cpp"
      "${DUCKDB_DDS_COMMON_SOURCE_HOST_DIR}/RingBufferProgressive.cpp"
      "${DUCKDB_DDS_COMMON_SOURCE_HOST_DIR}/Profiler.cpp"
      "${DUCKDB_DDS_FRONTEND_DIR}/DDSFrontEnd.cpp"
      "${DUCKDB_DDS_FRONTEND_DIR}/DDSBackEndBridge.cpp"
      "${DUCKDB_DDS_FRONTEND_DIR}/DDSDir.cpp"
      "${DUCKDB_DDS_FRONTEND_DIR}/DDSFile.cpp")
  add_library(duckdb_dds_posix STATIC ${DUCKDB_DDS_POSIX_SOURCES})
  target_include_directories(duckdb_dds_posix
                             PUBLIC
                             $<BUILD_INTERFACE:${DUCKDB_DDS_POSIX_DIR}>
                             $<BUILD_INTERFACE:${DUCKDB_DDS_COMMON_INCLUDE_DIR}>
                             $<BUILD_INTERFACE:${DUCKDB_DDS_COMMON_INCLUDE_DIR}/Host>
                             $<BUILD_INTERFACE:${DUCKDB_DDS_COMMON_SOURCE_HOST_DIR}>
                             $<BUILD_INTERFACE:${DUCKDB_DDS_FRONTEND_DIR}>
                             $<BUILD_INTERFACE:${DUCKDB_DDS_BACKEND_LIB_DIR}>)
  # Mirror Meson: threads + optional ibverbs/rdmacm linking.
  set(DUCKDB_DDS_POSIX_LINK_LIBS Threads::Threads)
  find_library(DUCKDB_DDS_IBVERBS_LIB NAMES ibverbs)
  if(DUCKDB_DDS_IBVERBS_LIB)
    list(APPEND DUCKDB_DDS_POSIX_LINK_LIBS ${DUCKDB_DDS_IBVERBS_LIB})
  else()
    list(APPEND DUCKDB_DDS_POSIX_LINK_LIBS ibverbs)
  endif()
  find_library(DUCKDB_DDS_RDMACM_LIB NAMES rdmacm PATHS /lib/x86_64-linux-gnu)
  if(DUCKDB_DDS_RDMACM_LIB)
    list(APPEND DUCKDB_DDS_POSIX_LINK_LIBS ${DUCKDB_DDS_RDMACM_LIB})
  else()
    list(APPEND DUCKDB_DDS_POSIX_LINK_LIBS rdmacm)
  endif()
  target_link_libraries(duckdb_dds_posix PUBLIC ${DUCKDB_DDS_POSIX_LINK_LIBS})
  # set(DUCKDB_DDS_POSIX_LIBS duckdb_dds_posix)
  # Updated: keep a local value and also export it to parent scope so tools/ can link it.
  set(DUCKDB_DDS_POSIX_LIBS duckdb_dds_posix)
  set(DUCKDB_DDS_POSIX_LIBS duckdb_dds_posix PARENT_SCOPE)
endif()

if(MSVC)
  add_compile_options("/bigobj")
endif()

function(ensure_variable_is_number INPUT_VERSION OUT_RESULT)
  if(NOT "${${INPUT_VERSION}}" MATCHES "^[0-9]+$")
    message(
      WARNING
        "VERSION PARAMETER ${INPUT_VERSION} \"${${INPUT_VERSION}}\" IS NOT A NUMBER - SETTING TO 0"
    )
    set(${OUT_RESULT}
        0
        PARENT_SCOPE)
  else()
    set(${OUT_RESULT}
        ${${INPUT_VERSION}}
        PARENT_SCOPE)
  endif()
endfunction()

if(AMALGAMATION_BUILD)

  add_library(duckdb SHARED "${PROJECT_SOURCE_DIR}/src/amalgamation/duckdb.cpp")
  set_target_properties(
    duckdb PROPERTIES VERSION ${DUCKDB_VERSION_NUMBER}
                      SOVERSION ${DUCKDB_MAJOR_VERSION}.${DUCKDB_MINOR_VERSION})
  target_link_libraries(duckdb ${DUCKDB_SYSTEM_LIBS})
  link_threads(duckdb PUBLIC)
  link_extension_libraries(duckdb PRIVATE)
  if(DUCKDB_USE_DDS_POSIX)
    target_link_libraries(duckdb PUBLIC ${DUCKDB_DDS_POSIX_LIBS})
    target_include_directories(duckdb PUBLIC $<BUILD_INTERFACE:${DUCKDB_DDS_POSIX_DIR}>
                                              $<BUILD_INTERFACE:${DUCKDB_DDS_COMMON_INCLUDE_DIR}>
                                              $<BUILD_INTERFACE:${DUCKDB_DDS_COMMON_INCLUDE_DIR}/Host>)
    target_compile_definitions(duckdb PUBLIC DUCKDB_USE_DDS_POSIX)
  endif()

  add_library(duckdb_static STATIC
              "${PROJECT_SOURCE_DIR}/src/amalgamation/duckdb.cpp")
  target_link_libraries(duckdb_static ${DUCKDB_SYSTEM_LIBS})
  link_threads(duckdb_static PUBLIC)
  link_extension_libraries(duckdb_static PUBLIC)
  if(DUCKDB_USE_DDS_POSIX)
    target_link_libraries(duckdb_static PUBLIC ${DUCKDB_DDS_POSIX_LIBS})
    target_include_directories(duckdb_static PUBLIC $<BUILD_INTERFACE:${DUCKDB_DDS_POSIX_DIR}>
                                                    $<BUILD_INTERFACE:${DUCKDB_DDS_COMMON_INCLUDE_DIR}>
                                                    $<BUILD_INTERFACE:${DUCKDB_DDS_COMMON_INCLUDE_DIR}/Host>)
    target_compile_definitions(duckdb_static PUBLIC DUCKDB_USE_DDS_POSIX)
  endif()

  install(FILES "${PROJECT_SOURCE_DIR}/src/amalgamation/duckdb.hpp"
                "${PROJECT_SOURCE_DIR}/src/include/duckdb.h"
          DESTINATION "${INSTALL_INCLUDE_DIR}")
  install(FILES "${PROJECT_SOURCE_DIR}/src/include/duckdb/common/winapi.hpp"
          DESTINATION "${INSTALL_INCLUDE_DIR}/duckdb/common")

else()

  add_definitions(-DDUCKDB_MAIN_LIBRARY)

  add_subdirectory(optimizer)
  add_subdirectory(planner)
  add_subdirectory(parser)
  add_subdirectory(function)
  add_subdirectory(catalog)
  add_subdirectory(common)
  add_subdirectory(logging)
  add_subdirectory(execution)
  add_subdirectory(main)
  add_subdirectory(parallel)
  add_subdirectory(storage)
  add_subdirectory(transaction)
  add_subdirectory(verification)

  set(DUCKDB_LINK_LIBS
      ${DUCKDB_SYSTEM_LIBS}
      duckdb_fsst
      duckdb_fmt
      duckdb_pg_query
      duckdb_re2
      duckdb_miniz
      duckdb_utf8proc
      duckdb_hyperloglog
      duckdb_fastpforlib
      duckdb_skiplistlib
      duckdb_mbedtls
      duckdb_yyjson
      duckdb_zstd)
  if(DUCKDB_USE_DDS_POSIX)
    list(APPEND DUCKDB_LINK_LIBS ${DUCKDB_DDS_POSIX_LIBS})
  endif()

  add_library(duckdb SHARED ${ALL_OBJECT_FILES})
  set_target_properties(
    duckdb PROPERTIES VERSION ${DUCKDB_VERSION_NUMBER}
                      SOVERSION ${DUCKDB_MAJOR_VERSION}.${DUCKDB_MINOR_VERSION})

  if(WIN32 AND NOT MINGW)
    ensure_variable_is_number(DUCKDB_MAJOR_VERSION RC_MAJOR_VERSION)
    ensure_variable_is_number(DUCKDB_MINOR_VERSION RC_MINOR_VERSION)
    ensure_variable_is_number(DUCKDB_PATCH_VERSION RC_PATCH_VERSION)
    ensure_variable_is_number(DUCKDB_DEV_ITERATION RC_DEV_ITERATION)

    set(CMAKE_RC_FLAGS
        "${CMAKE_RC_FLAGS} -D DUCKDB_VERSION=\"${DUCKDB_VERSION}\"")
    set(CMAKE_RC_FLAGS
        "${CMAKE_RC_FLAGS} -D DUCKDB_MAJOR_VERSION=\"${RC_MAJOR_VERSION}\"")
    set(CMAKE_RC_FLAGS
        "${CMAKE_RC_FLAGS} -D DUCKDB_MINOR_VERSION=\"${RC_MINOR_VERSION}\"")
    set(CMAKE_RC_FLAGS
        "${CMAKE_RC_FLAGS} -D DUCKDB_PATCH_VERSION=\"${RC_PATCH_VERSION}\"")
    set(CMAKE_RC_FLAGS
        "${CMAKE_RC_FLAGS} -D DUCKDB_DEV_ITERATION=\"${RC_DEV_ITERATION}\"")

    target_sources(duckdb PRIVATE version.rc)
  endif()

  target_link_libraries(duckdb PUBLIC ${DUCKDB_LINK_LIBS})
  link_threads(duckdb PUBLIC)
  link_extension_libraries(duckdb PRIVATE)
  if(DUCKDB_USE_DDS_POSIX)
    target_compile_definitions(duckdb PUBLIC DUCKDB_USE_DDS_POSIX)
    target_include_directories(duckdb PUBLIC $<BUILD_INTERFACE:${DUCKDB_DDS_POSIX_DIR}>
                                              $<BUILD_INTERFACE:${DUCKDB_DDS_COMMON_INCLUDE_DIR}>
                                              $<BUILD_INTERFACE:${DUCKDB_DDS_COMMON_INCLUDE_DIR}/Host>)
    # DDS debug: link DDS POSIX frontend so DDS symbols resolve.
    target_link_libraries(duckdb PUBLIC ${DUCKDB_DDS_POSIX_LIBS})
  endif()

  add_library(duckdb_static STATIC ${ALL_OBJECT_FILES})
  target_link_libraries(duckdb_static PUBLIC ${DUCKDB_LINK_LIBS})
  link_threads(duckdb_static PUBLIC)
  link_extension_libraries(duckdb_static PUBLIC)
  if(DUCKDB_USE_DDS_POSIX)
    target_compile_definitions(duckdb_static PUBLIC DUCKDB_USE_DDS_POSIX)
    target_include_directories(duckdb_static PUBLIC $<BUILD_INTERFACE:${DUCKDB_DDS_POSIX_DIR}>
                                                    $<BUILD_INTERFACE:${DUCKDB_DDS_COMMON_INCLUDE_DIR}>
                                                    $<BUILD_INTERFACE:${DUCKDB_DDS_COMMON_INCLUDE_DIR}/Host>)
    # DDS debug: link DDS POSIX frontend so DDS symbols resolve.
    target_link_libraries(duckdb_static PUBLIC ${DUCKDB_DDS_POSIX_LIBS})
  endif()

  target_include_directories(
    duckdb PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                  $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

  target_include_directories(
    duckdb_static PUBLIC $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}/include>
                         $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>)

  install(
    DIRECTORY "${PROJECT_SOURCE_DIR}/src/include/duckdb"
    DESTINATION "${INSTALL_INCLUDE_DIR}"
    FILES_MATCHING
    PATTERN "*.hpp"
    PATTERN "*.ipp")
  install(FILES "${PROJECT_SOURCE_DIR}/src/include/duckdb.hpp"
                "${PROJECT_SOURCE_DIR}/src/include/duckdb.h"
          DESTINATION "${INSTALL_INCLUDE_DIR}")

endif()

install(
  TARGETS duckdb duckdb_static
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}"
  RUNTIME DESTINATION "${INSTALL_BIN_DIR}")
if(DUCKDB_USE_DDS_POSIX)
  install(
    TARGETS duckdb_dds_posix
    EXPORT "${DUCKDB_EXPORT_SET}"
    ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
endif()
