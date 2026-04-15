project('re2_test', 'c', 'cpp')

re2_dep = dependency('re2', required: true)

parser_src = ['src/include/decoder/decoder.cpp',
            'src/block/metadata_block.cpp',
            'src/block/storage_block.cpp',
            'src/statistics/base_statistics.cpp',
            'src/statistics/distinct_statistics.cpp',
            'src/row_group/row_group.cpp',
            'src/schema/schema.cpp',
            'src/table/table.cpp',
            'src/type_info/type_info.cpp',
            'src/header/main_header.cpp',
            'src/header/database_header.cpp',
            'src/database/database.cpp',
            'src/interface.cpp',
            're2_test.cpp']

executable(
    're2_test', 
    sources: parser_src,
    dependencies: re2_dep,
    install: false,
    cpp_args: ['-std=c++17']
)