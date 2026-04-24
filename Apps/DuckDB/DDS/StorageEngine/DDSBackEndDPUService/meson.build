project('DDSFileBackEnd', 'C', 'CPP',
        version: '0.1',
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

##
# Resolve irrelevant compiler warnings
##
add_project_arguments('-Wno-format-zero-length', language: languages)
add_project_arguments('-Wno-address-of-packed-member', language: languages)
add_project_arguments('-Wno-deprecated-declarations', language: languages)

##
# DPDK & SPDK paths
##
#spdk_inc_path = get_option('SpdkInc')
spdk_inc_path = '/opt/mellanox/spdk/include'
#spdk_lib_path = get_option('SpdkLib')
spdk_lib_path = '/opt/mellanox/spdk/lib'
#dpdk_lib_path = get_option('DpdkLib')
dpdk_lib_path = '/opt/mellanox/dpdk/lib/aarch64-linux-gnu'
dpk_lib_path = '/home/ubuntu/jason/data/dpk/DP-Kernels/build'

message('SPDK include path =', spdk_inc_path)
message('SPDK library path =', spdk_lib_path)
message('DPDK library path =', dpdk_lib_path)
message('DPK library path =', dpk_lib_path)

##
# Build
##
base_app_inc_dirs = [
        include_directories('./Include/'),
        include_directories('../../Common/Include/'),
        include_directories('../../Common/Include/DPU'),
	include_directories(spdk_inc_path)
]

APP_NAME = 'DDSFileBackEnd'

app_inc_dirs = base_app_inc_dirs
app_srcs = []
app_srcs += [
        'Source/Main.c',
        'Source/FileBackEnd.c',
        'Source/bdev.c',
        'Source/ControlPlaneHandlers.c',
        'Source/DataPlaneHandlers.c',
        'Source/DPUBackEndDir.c',
        'Source/DPUBackEndFile.c',
        'Source/DPUBackEndStorage.c',
        'Source/Zmalloc.c',
        '../../Common/Source/DPU/BackEndControl.c',
        '../../Common/Source/DPU/FileService.c',
        '../../Common/Source/DPU/RingBufferPolling.c',
        '../../Common/Source/DPU/CacheTable.c'
]

##
# Link
##
app_link_args = ['-O3', '-Wl,--no-as-needed']
# Include the NVMe bdev RPC module so bdev_nvme_attach_controller is registered at startup.
app_link_args += ['-L' + spdk_lib_path, '-L' + dpdk_lib_path, '-lspdk_bdev_malloc', '-lspdk_bdev', '-lspdk_bdev_nvme', '-lspdk_notify', '-lspdk_bdev_error', '-lspdk_bdev_gpt', '-lspdk_bdev_split', '-lspdk_bdev_delay', '-lspdk_bdev_zone_block', '-lspdk_thread', '-lspdk_trace', '-lspdk_rpc', '-lspdk_jsonrpc', '-lspdk_json', '-lspdk_util', '-lspdk_ioat', '-lspdk_dma', '-lspdk_log', '-lspdk_event', '-lspdk_env_dpdk_rpc', '-lspdk_event_bdev', '-lspdk_init', '-lisal', '-lisal_crypto']
app_link_args += ['-lspdk_env_dpdk', '-lspdk_util', '-lspdk_log', '-lrte_eal', '-lrte_mempool', '-lrte_ring', '-lrte_mbuf', '-lrte_bus_pci', '-lrte_pci', '-lrte_mempool_ring', '-lrte_telemetry', '-lrte_kvargs', '-lrte_rcu', '-lrte_power', '-lrte_ethdev', '-lrte_vhost', '-lrte_net', '-lrte_cryptodev', '-lrte_hash']
app_link_args += ['-lrt', '-luuid', '-lssl', '-lcrypto', '-lm', '-lbsd', '-lnuma', '-ldl']
# jason: software raw-deflate diagnostic probe in DataPlaneHandlers uses zlib on failure path.
app_link_args += ['-lz']
app_link_args += ['-libverbs', '-lrdmacm']

##
# Dependencies
##
base_app_dependencies = []
base_app_dependencies += dependency('threads')
app_dependencies = base_app_dependencies
cpp = meson.get_compiler('cpp')
dpk_embed_dep = cpp.find_library('dpk_embed_runtime', dirs: [dpk_lib_path], required: true)
app_dependencies += [dpk_embed_dep]

##
# Final executable
##
executable(APP_NAME,
        app_srcs,
        c_args : base_c_args,
        cpp_args : base_cpp_args,
        dependencies : app_dependencies,
        include_directories : app_inc_dirs,
	link_args : app_link_args,
        link_language: 'cpp',
        install: install_apps,
	build_rpath: spdk_lib_path + ':' + dpdk_lib_path + ':' + dpk_lib_path
)
