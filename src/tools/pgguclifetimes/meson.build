# Copyright (c) 2022-2023, PostgreSQL Global Development Group

pgguclifetimes_sources = files(
  'pgguclifetimes.c',
  'vector.c'
)

# Locate libclang (the C interface to clang) and the clang-c/ headers
# (clang-c/Index.h, clang-c/CXCompilationDatabase.h).
#
# Detection is layered because clang's packaging varies widely:
#   1. The CMake "Clang" package (carries link info; on some distros it also
#      carries the include dirs, on others -- notably ones that split the dev
#      headers into a separate output -- it does not).
#   2. The bare libclang shared library plus an explicitly computed include
#      directory.  We try, in order: the -Dclang_c_includedir build option, and
#      the LLVM config-tool's includedir (works on monolithic layouts such as
#      Debian's /usr/lib/llvm-NN/include).
#
# Whichever path is taken, we only enable the targets once cc.has_header has
# confirmed clang-c/Index.h is actually reachable with the chosen flags, so a
# half-found dependency never breaks the build.
libclang_dep = dependency(
    'Clang',
    method: 'cmake',
    modules: ['libclang'],
    required: false
)
if not libclang_dep.found()
  libclang_dep = cc.find_library('clang', required: false)
endif

libclang_incdirs = []
libclang_incargs = []

clang_c_includedir = get_option('clang_c_includedir')
if clang_c_includedir != ''
  libclang_incdirs += include_directories(clang_c_includedir, is_system: true)
  libclang_incargs += '-I' + clang_c_includedir
elif llvm.found()
  llvm_incdir = llvm.get_variable(configtool: 'includedir', default_value: '')
  if llvm_incdir != ''
    libclang_incdirs += include_directories(llvm_incdir, is_system: true)
    libclang_incargs += '-I' + llvm_incdir
  endif
endif

# Confirm the headers are genuinely reachable before committing to the targets.
have_clang_c_headers = false
if libclang_dep.found()
  have_clang_c_headers = cc.has_header('clang-c/Index.h',
    args: libclang_incargs,
    dependencies: libclang_dep)
endif

if libclang_dep.found() and have_clang_c_headers
if host_system == 'windows'
  pgguclifetimes_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'pgguclifetimes',
    '--FILEDESC', 'pgguclifetimes - check GUC lifetime annotations'])
endif

pgguclifetimes = executable('pgguclifetimes',
  pgguclifetimes_sources,
  dependencies: [frontend_code, libclang_dep],
  include_directories: [include_directories('.')] + libclang_incdirs,
  kwargs: default_bin_args + {
    'install': false,
  },
)
bin_targets += pgguclifetimes

# pg_static_vars is the C++/LibTooling companion that finds function-scope
# statics.  It is a build-time-only developer utility (not part of the server),
# so its use of C++ does not affect the all-C policy for PostgreSQL proper.
# Only build it when a C++ compiler is available (have_cxx is set by the
# top-level meson.build) and the clang-c headers were found.
if have_cxx
  pg_static_vars = executable('pg_static_vars',
    files('pg_static_vars.cpp'),
    dependencies: [libclang_dep],
    include_directories: [include_directories('.')] + libclang_incdirs,
    override_options: ['cpp_std=c++20'],
    kwargs: default_bin_args + {
      'install': false,
    },
  )
  bin_targets += pg_static_vars
endif

tests += {
  'name': 'pgguclifetimes',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'tap': {
    'tests': [
      't/001_pgguclifetimes.pl',
    ],
  },
}
endif
