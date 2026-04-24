cmake_minimum_required(VERSION 3.16 FATAL_ERROR)
project(spinlock-buffer VERSION 0.1.0)

# Optional: enable static linking (may require static libs to be installed)
# Default to ON to make it easier to build a static binary. Change with -DSTATIC_LINK=OFF if not desired.
option(STATIC_LINK "Build fully static binary" ON)

# Enable compilation database
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Set output directories for all builds (Debug, Release, etc.)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)  # For executables
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)  # For shared libraries (.so, .dll)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)  # For static libraries (.a, .lib)

# Ensure the paths are set for multi-configuration generators (like Visual Studio)
foreach(OUTPUTCONFIG ${CMAKE_CONFIGURATION_TYPES})
    string(TOUPPER ${OUTPUTCONFIG} OUTPUTCONFIG)
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/bin)
    set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/lib)
    set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${OUTPUTCONFIG} ${CMAKE_BINARY_DIR}/lib)
endforeach()

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

# Toggle whether to use POSIX pthread spin lock instead of custom SpinLock.
option(USE_POSIX_SPINLOCK "Use pthread_spinlock_t-backed SpinLock" OFF)

# Add O2 optimization globally
add_compile_options(-O2)

set(PROJECT_ROOT ${CMAKE_CURRENT_SOURCE_DIR})

find_package(Threads REQUIRED)

add_executable(spinlock-buffer main.cpp buffer.cpp buffer.hpp types.hpp spinlock.hpp)

if(STATIC_LINK)
  target_link_options(spinlock-buffer PRIVATE -static -static-libgcc -static-libstdc++)
endif()

target_link_libraries(spinlock-buffer PRIVATE Threads::Threads)

target_compile_definitions(spinlock-buffer PRIVATE
  $<$<BOOL:${USE_POSIX_SPINLOCK}>:USE_POSIX_SPINLOCK>
)

if (USE_POSIX_SPINLOCK)
  message(STATUS "SpinlockBuffer: USE_POSIX_SPINLOCK is ON")
else()
  message(STATUS "SpinlockBuffer: using custom SpinLock")
endif()
