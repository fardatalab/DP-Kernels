project('dds-posix', 'cpp',
  version : '0.1.0',
  default_options : [
    'warning_level=3',
    'cpp_std=c++14',
    'buildtype=release',
  ]
)

fs = import('fs')

# Create a compiler object
cpp = meson.get_compiler('cpp')

# Find required dependencies
thread_dep = dependency('threads')

# For ibverbs
ibverbs_dep = dependency('libibverbs', required: false)
if not ibverbs_dep.found()
  ibverbs_dep = cpp.find_library('ibverbs', required: false)
  if not ibverbs_dep.found()
    message('Falling back to direct link arguments for ibverbs')
    ibverbs_args = ['-libverbs']
  else
    ibverbs_args = []
  endif
else
  ibverbs_args = []
endif

# For rdmacm
rdmacm_dep = cpp.find_library('rdmacm', required: false)
if not rdmacm_dep.found()
  # Try to find it in common locations
  rdmacm_paths = [
    '/lib/x86_64-linux-gnu',
    '/usr/lib/x86_64-linux-gnu',
    '/usr/local/lib'
  ]
  
  foreach path : rdmacm_paths
    rdmacm_dep = cpp.find_library('rdmacm', dirs: [path], required: false)
    if rdmacm_dep.found()
      break
    endif
  endforeach
  
  if not rdmacm_dep.found()
    message('Falling back to direct link arguments for rdmacm')
    rdmacm_args = ['-lrdmacm']
  else
    rdmacm_args = []
  endif
else
  rdmacm_args = []
endif

# Define include directories
inc_dirs = include_directories(
  '.',
  '../../Common/Include',
  '../../Common/Include/Host',
  '../../DDSFrontEnd',
  '..'
)

# Combine all link arguments
link_args = ['-Wl,--no-as-needed'] + ibverbs_args + rdmacm_args

# Common source files
common_sources = files(
  '../../Common/Source/Host/LinkedList.cpp',
  '../../Common/Source/Host/DMABuffer.cpp',
  '../../Common/Source/Host/RingBufferProgressive.cpp',
  '../../Common/Source/Host/Profiler.cpp'
)

# Frontend sources
frontend_sources = files(
  '../DDSFrontEnd/DDSFrontEnd.cpp',
  '../DDSFrontEnd/DDSBackEndBridge.cpp',
  '../DDSFrontEnd/DDSDir.cpp',
  '../DDSFrontEnd/DDSFile.cpp'
)

# Main DDSPosix sources
posix_sources = files(
  'DDSPosix.cpp'
)

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
  posix_sources + frontend_sources + common_sources,
  include_directories: inc_dirs,
  dependencies: all_deps,
  link_args: link_args,
  install: true
)

# Create pkg-config file
pkg = import('pkgconfig')
pkg.generate(
  name: 'dds-posix',
  description: 'DDSPosix Interface Library',
  version: meson.project_version(),
  libraries: dds_posix_lib,
  requires: [],
  requires_private: []
)

# Create an internal dependency that other parts of the project can use
dds_posix_dep = declare_dependency(
  link_with: dds_posix_lib,
  include_directories: inc_dirs,
  dependencies: all_deps,
  link_args: link_args
)

# Install headers
install_headers(
  'DDSPosix.h',
  'DDSPosixTypes.h',
  subdir: 'dds-posix'
)

# Only build tests if this is the main project (not a subproject)
if not meson.is_subproject()
  # Add tests subdir if it exists
  test_dir = 'tests'
  if fs.exists(test_dir)
    subdir(test_dir)
  endif
  
  # Simple test executable for basic compilation test
  test_exe = executable('test_dds_posix',
    'test/test_compile.cpp',
    dependencies: [dds_posix_dep],
    install: false
  )
  
  # Add basic compilation test
  test('basic_compilation_test', test_exe)
endif

# Print summary
summary = [
  '',
  '---------- DDSPosix Build Configuration ----------',
  'Version: @0@'.format(meson.project_version()),
  'Build type: @0@'.format(get_option('buildtype')),
  'C++ compiler: @0@'.format(cpp.get_id()),
  'ibverbs found: @0@'.format(ibverbs_dep.found()),
  'rdmacm found: @0@'.format(rdmacm_dep.found()),
  '',
]

message('\n'.join(summary))