project(
    'Faster',
    'c',
    'cpp',
    version: '0.1',
    default_options: [
        'cpp_std=c++17',
        # 'warning_level=3',
        'buildtype=debugoptimized',
    ],
)

faster_incs = []
faster_incs += include_directories('./FASTER/cc/src')
faster_incs += include_directories('./FASTER/cc/src/core')
# faster_incs += include_directories('./FASTER/cc/src/common')
# faster_incs += include_directories('./FASTER/cc/src/device')
# faster_incs += include_directories('./FASTER/cc/src/environment')
# faster_incs += include_directories('./FASTER/cc/benchmark-dir')

cxx = meson.get_compiler('cpp')

faster_lib_dir = meson.current_source_dir() + '/FASTER/cc/build/Release'
faster_dep = cxx.find_library(
    'faster',
    dirs: [faster_lib_dir],
    required: true,
)

uuid_dep = dependency(
    'uuid',
    required: true,
)
tbb_dep = dependency(
    'tbb',
    required: true,
)

rdmacm_dep = dependency('librdmacm', required: true)
ibverbs_dep = dependency('libibverbs', required: true)

deflate_dep = dependency(
    'libdeflate',
    required: true,
)

deps = [
    faster_dep, uuid_dep, tbb_dep, deflate_dep, rdmacm_dep, ibverbs_dep,
]

incs = []
incs += faster_incs
incs += include_directories('.')

common_srcs = ['helpers.cpp', 'rdma_comm.cpp']

server_srcs = [
    'server.cpp',
] + common_srcs

client_srcs = [
    'client.cpp',
] + common_srcs

dpu_app_srcs = [
    'dpu_app.cpp',
] + common_srcs

server = executable(
    'faster_server',
    server_srcs,
    dependencies: deps,
    include_directories: incs,
    install: false,
)

client = executable(
    'faster_client',
    client_srcs,
    dependencies: deps,
    include_directories: incs,
    install: false,
)

dpu_app = executable(
    'faster_dpu_app',
    dpu_app_srcs,
    dependencies: deps,
    include_directories: incs,
    install: false,
)