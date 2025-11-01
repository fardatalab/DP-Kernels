project('DDSBOWPipeline', 'c', 'cpp',
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
#           Network Engine									  #
###############################################################
base_app_dependencies = []
base_app_dependencies += dependency('threads')
base_app_dependencies += dependency('json-c')
base_app_dependencies += dependency('libbsd')
base_app_dependencies += dependency('libdpdk')

common_path = '/opt/mellanox/doca/applications/common/src'
tldk_path = 'NetworkEngine/TLDK/'
offload_engine_path = '../OffloadEngine/DPU/'

base_app_inc_dirs = [
        include_directories(common_path),
        include_directories('NetworkEngine/Include'),
	include_directories(tldk_path + 'libtle_timer'),
	include_directories(tldk_path + 'libtle_misc'),
	include_directories(tldk_path + 'libtle_memtank'),
	include_directories(tldk_path + 'libtle_dring'),
	include_directories(tldk_path + 'libtle_l4p'),
	# include_directories(offload_engine_path + 'Include')
]

app_dependencies = base_app_dependencies
app_inc_dirs = base_app_inc_dirs
app_srcs = []

#app_dependencies += dependency('doca-argp')
#app_dependencies += dependency('doca-flow')
#app_dependencies += dependency('doca-common')
app_dependencies += dependency('doca')

src = 'NetworkEngine/Source/'

app_srcs += [
	src + 'Main.c',
	src + 'DDSBOWPipeline.c',
	src + 'DDSBOWCore.c',
	src + 'DDSTrafficDirecting.c',
	src + 'AppEchoTCPDirect.c',
	src + 'ActionRedirectPackets.c',
	src + 'ActionModifyHeaders.c',
	src + 'PEPOLinuxTCP.c',
	src + 'PEPOTLDKTCP.c',
	src + 'PacketProcessor.c',
	tldk_path + 'libtle_timer/timer.c',
	tldk_path + 'libtle_memtank/memtank.c',
	tldk_path + 'libtle_memtank/misc.c',
	tldk_path + 'libtle_dring/dring.c',
	tldk_path + 'libtle_l4p/ctx.c',
	tldk_path + 'libtle_l4p/event.c',
	tldk_path + 'libtle_l4p/stream_table.c',
	tldk_path + 'libtle_l4p/tcp_ofo.c',
	tldk_path + 'libtle_l4p/tcp_rxtx.c',
	tldk_path + 'libtle_l4p/tcp_stream.c',
	tldk_path + 'libtle_l4p/udp_rxtx.c',
	tldk_path + 'libtle_l4p/udp_stream.c',
	common_path + '/dpdk_utils.c',
	# common_path + '/offload_rules.c',
	common_path + '/utils.c',
	# offload_engine_path + 'Source/UDF.c'
]

# Add -ldl dependency
dl_dep = meson.get_compiler('c').find_library('dl', required : true)
re2_dep = meson.get_compiler('cpp').find_library('re2', dirs: '/usr/local/lib', required: true)
absl_dep = meson.get_compiler('cpp').find_library('absl', dirs: '/usr/local/lib', required: true)

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

dpkfe_lib = meson.get_compiler('cpp').find_library('dpk_fe', dirs: '/home/ubuntu/DP-Kernels/build', static: true)

dpkfe_dep = [
    dependency('libdeflate'),
    dependency('mimalloc'),
    dependency('threads'),
]

###############################################################
#           DuckDB Parser									  #
###############################################################
parser_src = ['Parser/src/include/decoder/decoder.cpp',
            'Parser/src/block/metadata_block.cpp',
            'Parser/src/block/storage_block.cpp',
            'Parser/src/statistics/base_statistics.cpp',
            'Parser/src/statistics/distinct_statistics.cpp',
            'Parser/src/row_group/row_group.cpp',
            'Parser/src/schema/schema.cpp',
            'Parser/src/table/table.cpp',
            'Parser/src/type_info/type_info.cpp',
            'Parser/src/header/main_header.cpp',
            'Parser/src/header/database_header.cpp',
            'Parser/src/database/database.cpp',
            'Parser/src/interface.cpp']

executable('network_engine',
	app_srcs + parser_src,
	c_args : base_c_args,
	cpp_args : base_cpp_args,
	dependencies : [app_dependencies, dl_dep, re2_dep, absl_dep, dpkfe_lib, dpkfe_dep],
	include_directories : [app_inc_dirs, incs],
	install: install_apps,
)


###############################################################
#           Baseline Client									  #
###############################################################
client_src = ['client.cpp', 'socket.cpp']
executable('client', client_src)

###############################################################
#           DPK Client.  									  #
###############################################################
# TODO
