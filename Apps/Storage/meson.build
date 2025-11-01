project('PageServer', 'c', 'cpp',
        # Get version number from file.
        version: run_command(find_program('cat'), files('/opt/mellanox/doca/applications/VERSION'), check: true).stdout().strip(),
        license: 'Proprietary',
        default_options: ['buildtype=release'],
        meson_version: '>= 0.61.2'
)

languages = ['c', 'cpp']

install_apps = false

base_cpp_args = ['-std=c++17']
base_c_args = ['-flax-vector-conversions']

# Comment this line to restore warnings of experimental DOCA features
add_project_arguments('-D DOCA_ALLOW_EXPERIMENTAL_API', language: languages)

# Ensure mlnx-dpdk will manage to find our libbsd
add_project_arguments('-D RTE_USE_LIBBSD', language: languages)

# Resolve irrelevant compiler warnings
add_project_arguments('-Wno-format-zero-length', language: languages)
add_project_arguments('-Wno-address-of-packed-member', language: languages)
add_project_arguments('-Wno-deprecated-declarations', language: languages)

###############################################################
#           DPK         									  #
###############################################################
common_dir = '../../Common/'
dpm_dir = '../../DPManager/'
kernel_dir = '../../Kernels/'
device_specific_dir = dpm_dir + 'device/'

# kernels directory
null_kernel_dir = '../../Kernels/null/'
sw_kernel_dir = '../../Kernels/sw/'
bf3v2_2_kernel_dir = '../../Kernels/bf3v2.2/'
bf3_kernel_dir = '../../Kernels/bf3/'
bf2_kernel_dir = '../../Kernels/bf2/'
all_kernels_dirs = [null_kernel_dir, sw_kernel_dir, bf3_kernel_dir, bf2_kernel_dir, bf3v2_2_kernel_dir]

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
    dpm_dir + 'coalescing.cpp',
    device_specific_dir + 'device_specific.cpp',
]
srcs += sw_kernel_srcs
inc_dirs = [
    '../../DPManager',
    '../../DPManager/device',
    '../../Common',
    '../../FrontEnd',
    '/usr/local/include/mimalloc-1.9',
    '/opt/mellanox/doca/include',
]

dds_inc_dirs = [
    'DDS/Common/Include',
    'DDS/Common/Source/Host',
    'DDS/Common/Include/Host',
    'DDS/StorageEngine/DDSFrontEnd',
    'DDS/StorageEngine/BackEndLib'
]
inc_dirs += dds_inc_dirs
dds_incs = include_directories(dds_inc_dirs)

common_sources = [
    'DDS/Common/Source/Host/DMABuffer.cpp',  
    'DDS/Common/Source/Host/LinkedList.cpp',
    'DDS/Common/Source/Host/RingBufferProgressive.cpp',
    'DDS/Common/Source/Host/Profiler.cpp'
]

link_args = ['-O3', '-Wl,--no-as-needed']
link_args += ['-lrdmacm', '-libverbs']

dds_frontend_lib = library('ddsfrontend',
    sources: [
       'DDS/StorageEngine/DDSFrontEnd/DDSFrontEnd.cpp',
        'DDS/StorageEngine/DDSFrontEnd/DDSBackEndBridge.cpp',
        'DDS/StorageEngine/DDSFrontEnd/DDSDir.cpp',
        'DDS/StorageEngine/DDSFrontEnd/DDSFile.cpp'
    ] + common_sources, 
    include_directories: dds_incs,
    link_args: link_args
)

DOCA_VERSION = run_command(find_program('cat'),
                files('/opt/mellanox/doca/applications/VERSION'), check: true).stdout().strip().split('.')
DOCA_VERSION_STR = '.'.join(DOCA_VERSION)
message('DOCA version: ' + DOCA_VERSION_STR)

c_srcs = []
# add the sw kernels device dir
inc_dirs += device_specific_dir + 'sw/'
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
# inc_dirs += ['test/page_server']

incs = include_directories(inc_dirs)

# dpkfe_lib = meson.get_compiler('cpp').find_library('dpk_fe', dirs: '/home/ubuntu/DP-Kernels/build', static: true)

dpkfe_dep = [
    dependency('libdeflate'),
    dependency('mimalloc'),
    dependency('threads'),
]

deps = []
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

client_deps = [dependency('mimalloc'), dependency('threads'), ]
page_server_dep = client_deps + [dependency('libdeflate'), doca_dep]

###############################################################
#           Page server 									  #
###############################################################
page_server = executable(
     'page_server',
     'server.cpp',
     'page_server.cpp',
     dpm_dir + 'dpm_interface.cpp',
     dpm_dir + 'memory.cpp',
     dpm_dir + 'ring_buffer.cpp',
     include_directories: incs,
     link_with: dds_frontend_lib,
     dependencies: page_server_dep,
     install: false,
     cpp_args: ['-std=c++17'],
)


###############################################################
#           Baseline Client									  #
###############################################################
client_src = ['client.cpp']
executable('client', client_src, include_directories: incs)

###############################################################
#           DPK Client.  									  #
###############################################################
# TODO
