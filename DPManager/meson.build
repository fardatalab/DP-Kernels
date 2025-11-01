project('DPKernel', 'C', 'CPP',
    version: '0.1.0',
    default_options: ['buildtype=release']#debugoptimized
)

add_project_arguments('-Wno-deprecated-declarations', language: 'cpp')

c_srcs = [
    'doca_common.c',
]

srcs = [
    'main.cpp',
    'dp_manager_msgq.cpp',
    # 'dpm_interface.cpp',
    'executor.cpp',
    'memory.cpp',
    # 'ring_buffer.cpp',
]

inc_dirs = [
    '.',
    '/usr/local/include/mimalloc-1.9',
    '/opt/mellanox/doca/include',

]
incs = include_directories(inc_dirs)
# incs += include_directories('/usr/local/include/mimalloc-1.9')
deps = [
    dependency('boost'),
    dependency('mimalloc'),
    # dependency('doca', required: false) >= 0 ? dependency('doca') : dependency('other')
]

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

doca_common_lib = static_library('doca_common_lib', c_srcs, include_directories: incs)

exe = executable('dp_manager',
    srcs,
    include_directories: incs,
    dependencies: deps,
    install: false,
    link_with: doca_common_lib
)

cleint = executable('client',
    '../test/client.cpp',
    'dpm_interface.cpp',
    'memory.cpp',
    include_directories: incs,
    dependencies: deps,
    install: false,
    link_with: doca_common_lib  # TODO: ughh, REFACTOR THIS!!! NO REASON FOR FRONT END TO LINK WITH DOCA
)

null_client = executable('null_client',
    '../test/null_kernel_client.cpp',
    'dpm_interface.cpp',
    'memory.cpp',
    include_directories: incs,
    dependencies: deps,
    install: false,
    link_with: doca_common_lib  # TODO: ughh, REFACTOR THIS!!! NO REASON FOR FRONT END TO LINK WITH DOCA
)