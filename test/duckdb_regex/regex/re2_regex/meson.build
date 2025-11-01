cmake_mod = import('cmake')

regex_proj = cmake_mod.project(meson.current_source_dir(),
  default_options: [
      'BUILD_SHARED_LIBS=OFF',
      'BUILD_TESTING=OFF',
      'BENCHMARK=OFF'
    ]
)

regex_dep = regex_proj.dependency()

regex_dep
