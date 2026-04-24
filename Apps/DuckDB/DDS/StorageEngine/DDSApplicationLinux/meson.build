project('DDSApplication', ['c', 'cpp'],
    version: '0.1',
    default_options: [
        'cpp_std=c++11',
        'buildtype=debug'
    ]
)

# Base arguments
languages = ['c', 'cpp']
base_cpp_args = ['-std=c++11']

# Include directories
include_dirs = [
    include_directories('.'),
    include_directories('../'),
    include_directories('../../'),
    include_directories('../../../'),
    include_directories('../../Common/Include'),
    include_directories('../../Common/Source/Host'),
    include_directories('../../Common/Include/Host'),
    include_directories('../DDSFrontEnd'),
    include_directories('../BackEndLib')
]

# Link arguments
link_args = ['-O3', '-Wl,--no-as-needed']
link_args += ['-lrdmacm', '-libverbs']

# Dependencies
thread_dep = dependency('threads')


common_sources = [
    '../../Common/Source/Host/DMABuffer.cpp',  
    '../../Common/Source/Host/LinkedList.cpp',
    '../../Common/Source/Host/RingBufferProgressive.cpp',
    '../../Common/Source/Host/Profiler.cpp'
]

# Frontend library
frontend_lib = library('ddsfrontend',
    sources: [
        '../DDSFrontEnd/DDSFrontEnd.cpp',
        '../DDSFrontEnd/DDSBackEndBridge.cpp',
        '../DDSFrontEnd/DDSDir.cpp',
        '../DDSFrontEnd/DDSFile.cpp'
    ] + common_sources,
    include_directories: include_dirs,
    link_args: link_args
)

# Application
executable('dds_application',
    sources: [
        'DDSApplication.cpp',
        'DDSEvent.cpp'
    ],
    include_directories: include_dirs,
    link_with: frontend_lib,
    dependencies: [thread_dep],
    link_args: link_args
)