project('dds-posix', 'cpp',
  version : '0.1',
  default_options : [
    'warning_level=3',
    'cpp_std=c++11',
    'buildtype=debug',
  ]
)
# Find required dependencies
thread_dep = dependency('threads')
# Create a compiler object
cc = meson.get_compiler('cpp')
# For ibverbs, try to find it through various means
ibverbs_dep = dependency('libibverbs', required: false)
if not ibverbs_dep.found()
  ibverbs_dep = cc.find_library('ibverbs', required: false)
  if not ibverbs_dep.found()
    # Fallback to compiler args if library cannot be found via Meson
    message('Falling back to direct link arguments for ibverbs')
    ibverbs_args = ['-libverbs']
  else
    ibverbs_args = []
  endif
else
  ibverbs_args = []
endif
# For rdmacm - explicitly specify the library path based on ldconfig output
rdmacm_path = '/lib/x86_64-linux-gnu'
rdmacm_dep = cc.find_library('rdmacm', dirs: [rdmacm_path], required: false)
if not rdmacm_dep.found()
  # Fallback to direct compiler args
  message('Falling back to direct link arguments for rdmacm')
  rdmacm_args = ['-lrdmacm']
else
  rdmacm_args = []
endif
# Include directories
inc_dirs = include_directories(
  '.',
  '../',
  '../../',
  '../../../',
  '../../../../',
  '../../../Common/Include',
  '../../../Common/Include/Host',
  '../../../Common/Source/Host',
  '../../DDSFrontEnd',
  '../../BackEndLib'
)
# Combine all link arguments
link_args = ['-Wl,--no-as-needed'] + ibverbs_args + rdmacm_args
# Common source files
common_sources = [
  '../../../Common/Source/Host/LinkedList.cpp',
  '../../../Common/Source/Host/DMABuffer.cpp',
  '../../../Common/Source/Host/RingBufferProgressive.cpp',
  '../../../Common/Source/Host/Profiler.cpp'
]
# Frontend sources
frontend_sources = [
  '../../DDSFrontEnd/DDSFrontEnd.cpp',
  '../../DDSFrontEnd/DDSBackEndBridge.cpp',
  '../../DDSFrontEnd/DDSDir.cpp',
  '../../DDSFrontEnd/DDSFile.cpp'
]
# Source files for the DDSPosix library
lib_sources = [
  '../DDSPosix.cpp',
] + frontend_sources + common_sources
# Collect all dependencies
all_deps = [thread_dep]
if ibverbs_dep.found()
  all_deps += [ibverbs_dep]
endif
if rdmacm_dep.found()
  all_deps += [rdmacm_dep]
endif
# Create the DDSPosix library
dds_posix_lib = static_library('dds_posix',
  lib_sources,
  include_directories : inc_dirs,
  dependencies: all_deps,
  link_args: link_args
)
# Create an internal dependency
dds_posix_dep = declare_dependency(
  link_with : dds_posix_lib,
  include_directories : inc_dirs,
  dependencies: all_deps,
  link_args: link_args
)
# Basic test executable
test_exe = executable('test_dds_posix',
  'test_compile.cpp',
  dependencies : [dds_posix_dep],
  link_args : link_args
)
# Add a basic compilation test
test('basic test', test_exe)
# Simple unit tests without GTest
simple_test_exe = executable('simple_dds_posix_tests',
  'simple_dds_posix_tests.cpp',
  dependencies : [dds_posix_dep],
  include_directories : inc_dirs,
  link_args : link_args
)
# Add the simple test
test('simple_dds_posix_tests', simple_test_exe, 
  timeout: 120,  # Allow up to 2 minutes for the tests to complete
  is_parallel: false  # Run tests serially to avoid file conflicts
)

# Concurrent POSIX tests
concurrent_test_exe = executable('concurrent_dds_posix_tests',
  'concurrent_dds_posix_tests.cpp',
  dependencies : [dds_posix_dep],
  include_directories : inc_dirs,
  link_args : link_args
)
test('concurrent_dds_posix_tests', concurrent_test_exe,
  timeout: 120,
  is_parallel: false
)

# Benchmark executable
benchmark_exe = executable('dds_benchmark',
  'benchmarking.cpp',
  dependencies : [dds_posix_dep],
  include_directories : inc_dirs,
  link_args : link_args
)

# Add a run target for benchmarking
# run_target('benchmark', 
#   command : [benchmark_exe, '512'],  # Run with 512MB file by default
#   depends : benchmark_exe
# )
