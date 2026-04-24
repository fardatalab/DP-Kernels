project('DDSBOWPipeline', 'C', 'CPP',
        # Get version number from file.
        version: run_command(find_program('cat'), files('/opt/mellanox/doca/applications/VERSION'), check: true).stdout().strip(),
        license: 'Proprietary',
        default_options: ['buildtype=release'],
        meson_version: '>= 0.61.2'
)

languages = ['c', 'cpp']

install_apps = false

base_cpp_args = [
        '-std=c++11',
]

base_c_args = [
	'-flax-vector-conversions',
]

# Comment this line to restore warnings of experimental DOCA features
add_project_arguments('-D DOCA_ALLOW_EXPERIMENTAL_API', language: languages)

# Ensure mlnx-dpdk will manage to find our libbsd
add_project_arguments('-D RTE_USE_LIBBSD', language: languages)

# Resolve irrelevant compiler warnings
add_project_arguments('-Wno-format-zero-length', language: languages)
add_project_arguments('-Wno-address-of-packed-member', language: languages)
add_project_arguments('-Wno-deprecated-declarations', language: languages)

base_app_dependencies = []
base_app_dependencies += dependency('threads')
base_app_dependencies += dependency('json-c')
base_app_dependencies += dependency('libbsd')
base_app_dependencies += dependency('libdpdk')

common_path = '/opt/mellanox/doca/applications/common/src'
tldk_path = 'TLDK/'
offload_engine_path = '../OffloadEngine/DPU/'

base_app_inc_dirs = [
        include_directories(common_path),
        include_directories('./Include'),
	include_directories(tldk_path + 'libtle_timer'),
	include_directories(tldk_path + 'libtle_misc'),
	include_directories(tldk_path + 'libtle_memtank'),
	include_directories(tldk_path + 'libtle_dring'),
	include_directories(tldk_path + 'libtle_l4p'),
	# include_directories(offload_engine_path + 'Include')
]

APP_NAME = 'DDSPipeline'

app_dependencies = base_app_dependencies
app_inc_dirs = base_app_inc_dirs
app_srcs = []

#app_dependencies += dependency('doca-argp')
#app_dependencies += dependency('doca-flow')
#app_dependencies += dependency('doca-common')
app_dependencies += dependency('doca')

src = 'Source/'

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

executable(APP_NAME,
	app_srcs,
	c_args : base_c_args,
	cpp_args : base_cpp_args,
	dependencies : [app_dependencies, dl_dep],
	include_directories : app_inc_dirs,
	install: install_apps)
