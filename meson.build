project(
    'DPKernel',
    'C',
    'CPP',
    version: '0.1.0',
    default_options: ['buildtype=release'] #debugoptimized
)

add_project_arguments('-Wno-deprecated-declarations', language: 'cpp')
add_project_arguments('-Wno-deprecated-declarations', language: 'c')
add_project_arguments('-Wno-unknown-pragmas', language: 'cpp')
# add_project_arguments('-std=c++20', language: 'cpp')


common_dir = 'Common/'
dpm_dir = 'DPManager/'
kernel_dir = 'Kernels/'
device_specific_dir = dpm_dir + 'device/'

null_kernel_dir = 'Kernels/null/'
bf3v2_2_kernel_dir = 'Kernels/bf3v2.2/'
bf3_kernel_dir = 'Kernels/bf3/'
bf2_kernel_dir = 'Kernels/bf2/'
all_kernels_dirs = [null_kernel_dir, bf3_kernel_dir, bf2_kernel_dir, bf3v2_2_kernel_dir]

srcs = [
    dpm_dir + 'main.cpp',
    dpm_dir + 'dp_manager_msgq.cpp',
    dpm_dir + 'executor.cpp',
    dpm_dir + 'memory.cpp',
    dpm_dir + 'ring_buffer.cpp',
    device_specific_dir + 'device_specific.cpp',
]

inc_dirs = [
    'DPManager',
    'DPManager/device',
    'Common',
    '/usr/local/include/mimalloc-1.9',
    '/opt/mellanox/doca/include',
]

DOCA_VERSION = run_command(find_program('cat'),
                files('/opt/mellanox/doca/applications/VERSION'), check: true).stdout().strip().split('.')
DOCA_VERSION_STR = '.'.join(DOCA_VERSION)
message('DOCA version: ' + DOCA_VERSION_STR)



c_srcs = []

# Define version-specific flags
doca_version_defines = []
if DOCA_VERSION[0] == '2' and DOCA_VERSION[1] == '5'
    message('assuming DOCA 2.5 (BF-3)')
    # doca_version_defines += ['-DDOCA_VER_2_5']
    c_srcs += [common_dir + 'doca_common_v_2_5.c']
    message('building bf3 v2.5 lib')
    inc_dirs += device_specific_dir + 'bf3/'
elif DOCA_VERSION[0] == '2' and DOCA_VERSION[1] == '2'
    message('assuming DOCA 2.2 (BF-3)')
    # doca_version_defines += ['-DDOCA_VER_2_2']
    c_srcs += [common_dir + 'doca_common_v_2_2.c']
    message('building bf3 v2.2 lib')
    inc_dirs += device_specific_dir + 'bf3v2.2/'
elif DOCA_VERSION[0] == '1'
    message('assuming DOCA 1.5 (BF-2)')
    # doca_version_defines += ['-DDOCA_VER_1_5']
    c_srcs += [common_dir + 'doca_common_v_1_5.c']
    message('building bf2 lib')
    inc_dirs += device_specific_dir + 'bf2/'
else
    error('Unknown DOCA version: ' + DOCA_VERSION[0])
endif

inc_dirs += device_specific_dir



inc_dirs += all_kernels_dirs

incs = include_directories(inc_dirs)

# incs += include_directories(all_kernels_dirs)
# incs += include_directories('/usr/local/include/mimalloc-2.2')
deps = [
    # dependency('boost'),
    dependency('mimalloc'),
    dependency('threads'),
]
# Add the dl library for loading device libraries
dl_dep = meson.get_compiler('cpp').find_library('dl', required: true)
deps += [dl_dep]
# Add the rt library for POSIX shared memory functions like shm_open
rt_dep = meson.get_compiler('cpp').find_library('rt', required: true)
deps += [rt_dep]

# Check if DOCA is available as a single dependency (host has this), or we need to link separately (on DPU)
doca_dep = dependency('doca', required: false)
if doca_dep.found()
    deps += [doca_dep]
    message('DOCA dependency found, using -ldoca')
else
    doca_argp = dependency('doca-argp', required: false)
    if doca_argp.found()
        deps += [doca_argp]
    else
        warning('DOCA ARGP not found, building without DOCA support')
    endif

    doca_common = dependency('doca-common', required: false)
    if doca_common.found()
        deps += [doca_common]
    else
        warning('DOCA Common not found, building without DOCA support')
    endif

    doca_compress = dependency('doca-compress', required: false)
    if doca_compress.found()
        deps += [doca_compress]
    else
        warning('DOCA Compress not found, building without DOCA support')
    endif

    doca_regex = dependency('doca-regex', required: false)
    if doca_regex.found()
        deps += [doca_regex]
    else
        warning('DOCA Regex not found, building without DOCA support')
    endif
endif

doca_common_lib = static_library('doca_common_lib', c_srcs, include_directories: incs, pic: true)

if DOCA_VERSION[0] == '2' and DOCA_VERSION[1] == '5'
   
    dpm_bf3_lib = shared_module('dpm_bf3', sources: [
        device_specific_dir + 'bf3/bf3_device.cpp',
        # kernel_dir + 'bf3/kernels.cpp',
        kernel_dir + 'bf3/decompress_deflate_bf3.cpp',
        kernel_dir + 'null/null_kernel.cpp',
    ], include_directories: incs, dependencies: deps, link_with: doca_common_lib)
elif DOCA_VERSION[0] == '2' and DOCA_VERSION[1] == '2'
    
    dpm_bf2_lib = shared_module('dpm_bf3v2.2', sources: [
        device_specific_dir + 'bf3v2.2/bf3v2.2_device.cpp',
        kernel_dir + 'bf3v2.2/decompress_deflate_bf3v2.2.cpp',
        kernel_dir + 'null/null_kernel.cpp',
    ], include_directories: incs, dependencies: deps, link_with: doca_common_lib)
elif DOCA_VERSION[0] == '1'
    
    dpm_bf2_lib = shared_module('dpm_bf2', sources: [
        device_specific_dir + 'bf2/bf2_device.cpp',
        kernel_dir + 'bf2/decompress_deflate_bf2.cpp',
        kernel_dir + 'null/null_kernel.cpp',
    ], include_directories: incs, dependencies: deps, link_with: doca_common_lib)
else
    error('Unknown DOCA version: ' + DOCA_VERSION_STR)
endif


exe = executable(
    'dp_manager',
    srcs,
    include_directories: incs,
    dependencies: deps,
    install: false,
    link_with: doca_common_lib,
    cpp_args: ['-std=c++17'],
    export_dynamic: true,
)

# cleint = executable(
#     'client',
#     'test/client.cpp',
#     dpm_dir + 'dpm_interface.cpp',
#     dpm_dir + 'memory.cpp',
#     include_directories: incs,
#     dependencies: deps,
#     install: false,
#     link_with: doca_common_lib # TODO: ughh, REFACTOR THIS!!! NO REASON FOR FRONT END TO LINK WITH DOCA,,,,,,,,
# )

null_client = executable(
    'null_client',
    'test/null_kernel_client.cpp',
    dpm_dir + 'dpm_interface.cpp',
    dpm_dir + 'memory.cpp',
    dpm_dir + 'ring_buffer.cpp',
    include_directories: incs,
    dependencies: deps,
    install: false,
    cpp_args: ['-std=c++17'],
    link_with: doca_common_lib # TODO: ughh, REFACTOR THIS!!! NO REASON FOR FRONT END TO LINK WITH DOCA,,,,,,,,,,,,,,,
)

bf3_deflate_client = executable(
    'bf3_deflate_client',
    'test/bf3_deflate_client.cpp',
    dpm_dir + 'dpm_interface.cpp',
    dpm_dir + 'memory.cpp',
    dpm_dir + 'ring_buffer.cpp',
    include_directories: incs,
    dependencies: deps,
    install: false,
    cpp_args: ['-std=c++17'],
    link_with: doca_common_lib
)