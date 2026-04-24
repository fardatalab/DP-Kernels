project(
    'DPKernel',
    'C',
    'CPP',
    version: '0.1.0',
    default_options: ['buildtype=release'] #debugoptimized
)
add_project_arguments('-mcpu=cortex-a72', language: 'c')
add_project_arguments('-mcpu=cortex-a72', language: 'cpp')
add_project_arguments('-Wno-deprecated-declarations', language: 'cpp')
add_project_arguments('-Wno-deprecated-declarations', language: 'c')
add_project_arguments('-Wno-unknown-pragmas', language: 'cpp')
# add_project_arguments('-std=c++20', language: 'cpp')


common_dir = 'Common/'
dpm_dir = 'DPManager/'
kernel_dir = 'Kernels/'
device_specific_dir = dpm_dir + 'device/'

# kernels directory
null_kernel_dir = 'Kernels/null/'
sw_kernel_dir = 'Kernels/sw/'

all_kernels_dirs = [null_kernel_dir, sw_kernel_dir]

sw_kernel_srcs = [
    sw_kernel_dir + 'decompress_deflate_sw.cpp',
    sw_kernel_dir + 'regex_sw.cpp',
    device_specific_dir + 'sw/' + 'sw_device.cpp',
]

srcs = [
    dpm_dir + 'main.cpp',
    dpm_dir + 'dp_manager_msgq.cpp',
    dpm_dir + 'kernel_interface.cpp',
    dpm_dir + 'memory.cpp',
    dpm_dir + 'ring_buffer.cpp',
    dpm_dir + 'scheduling.cpp',
    # dpm_dir + 'coalescing.cpp',
    device_specific_dir + 'device_specific.cpp',
    device_specific_dir + 'elba/elba_device.cpp',
    kernel_dir + 'null/null_kernel.cpp',
]
srcs += sw_kernel_srcs

inc_dirs = [
    'DPManager',
    'DPManager/device',
    'Common',
    'FrontEnd',
    '/usr/local/include/mimalloc-1.9',
]


c_srcs = []

# add the sw kernels device dir
inc_dirs += device_specific_dir + 'sw/'
inc_dirs += device_specific_dir + 'elba/'
inc_dirs += device_specific_dir



inc_dirs += all_kernels_dirs

inc_dirs += [
    'test/page_server'
]

incs = include_directories(inc_dirs)

# incs += include_directories(all_kernels_dirs)
# incs += include_directories('/usr/local/include/mimalloc-2.2')

# re2_dep = meson.get_compiler('cpp').find_library('re2', dirs: '/usr/local/lib', required: true)
re2_dep = dependency('re2', required: true, static: true)
# absl_dep = meson.get_compiler('cpp').find_library('absl', dirs: '/usr/local/lib', required: true)
# absl_dep = dependency('absl', required: true)
sw_kernel_deps = [dependency('libdeflate', static: true), re2_dep]#, absl_dep

client_deps = [dependency('mimalloc', static: true), dependency('threads', static: true), ]

deps = [
    # dependency('boost'),
    dependency('mimalloc', static: true),
    dependency('threads', static: true),
]
deps += sw_kernel_deps

# # Add the dl library for loading device libraries
# dl_dep = meson.get_compiler('cpp').find_library('dl', required: true, static: true)
# deps += [dl_dep]
# Add the rt library for POSIX shared memory functions like shm_open
rt_dep = meson.get_compiler('cpp').find_library('rt', required: true, static: true)
deps += [rt_dep]
# client_deps += [dl_dep, rt_dep]
client_deps += [rt_dep]

# dpm_elba_lib = shared_module('dpm_elba', sources: [
#         device_specific_dir + 'elba/elba_device.cpp',
#         kernel_dir + 'null/null_kernel.cpp',
#         dpm_dir + 'memory.cpp',
#     ], include_directories: incs, dependencies: deps)

exe = executable(
    'dp_manager',
    srcs,
    include_directories: incs,
    dependencies: deps,
    install: false,
    cpp_args: ['-std=c++17'],
    export_dynamic: true,
    link_args: ['-static'],
)

fe_srcs = [
    dpm_dir + 'dpm_interface.cpp',
    dpm_dir + 'memory.cpp',
    dpm_dir + 'ring_buffer.cpp',
    'FrontEnd/fe_interface.cpp'
]
dpk_fe = static_library('dpk_fe', fe_srcs, include_directories: incs, pic: true, cpp_args: ['-std=c++17'],)

null_client = executable(
    'null_client',
    'test/null_kernel_client copy.cpp',
    include_directories: incs,
    dependencies: deps,
    install: false,
    cpp_args: ['-std=c++17'],
    link_with: [dpk_fe],
    link_args: ['-static'],
)
