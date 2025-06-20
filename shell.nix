{ pkgs, system }:

let
  # Use LLVM 19 for modern PostgreSQL development
  llvmPkgs = pkgs.llvmPackages_19;

  # Configuration constants
  config = {
    pgSourceDir = "$HOME/ws/postgresql";
    pgBuildDir = "$HOME/ws/postgresql/build";
    pgInstallDir = "$HOME/ws/postgresql/install";
    pgDataDir = "/tmp/test-db";
    pgBenchDir = "/tmp/pgbench-results";
    pgFlameDir = "/tmp/flame-graphs";
  };

  # Single dependency function that can be used for all environments
  getPostgreSQLDeps = muslLibs: with pkgs; [
    # Build system (always use host tools)
    meson ninja pkg-config autoconf libtool git which
    binutils gnumake

    # Parser/lexer tools
    bison flex

    # Perl with required packages
    (perl.withPackages (ps: with ps; [ IPCRun ]))

    # Documentation
    docbook_xml_dtd_45 docbook-xsl-nons libxslt libxml2 fop

    # Development tools (always use host tools)
    coreutils shellcheck ripgrep valgrind curl
    gdb lldb strace ltrace
    perf-tools linuxPackages.perf flamegraph
    htop iotop sysstat
    ccache clang-tools cppcheck

    # LLVM toolchain
    llvmPkgs.llvm llvmPkgs.llvm.dev

    # Language support
    (python3.withPackages (ps: with ps; [ requests browser-cookie3 ]))
    tcl
  ] ++ (if muslLibs then [
    # Musl target libraries for cross-compilation
    pkgs.pkgsMusl.readline
    pkgs.pkgsMusl.zlib
    pkgs.pkgsMusl.openssl
    pkgs.pkgsMusl.icu
    pkgs.pkgsMusl.lz4
    pkgs.pkgsMusl.zstd
    pkgs.pkgsMusl.libuuid
    pkgs.pkgsMusl.libkrb5
    pkgs.pkgsMusl.linux-pam
    pkgs.pkgsMusl.libxcrypt
  ] else [
    # Glibc target libraries
    readline zlib openssl icu lz4 zstd libuuid libkrb5
    linux-pam libxcrypt numactl openldap
    liburing libselinux
    glibc glibc.dev
  ]);

  # GDB configuration for PostgreSQL debugging
  gdbConfig = pkgs.writeText "gdbinit-postgres" ''
    # PostgreSQL-specific GDB configuration

    # Pretty-print PostgreSQL data structures
    define print_node
      if $arg0
        printf "Node type: %s\n", nodeTagNames[$arg0->type]
        print *$arg0
      else
        printf "NULL node\n"
      end
    end
    document print_node
    Print a PostgreSQL Node with type information
    Usage: print_node <node_pointer>
    end

    define print_list
      set $list = (List*)$arg0
      if $list
        printf "List length: %d\n", $list->length
        set $cell = $list->head
        set $i = 0
        while $cell && $i < $list->length
          printf "  [%d]: ", $i
          print_node $cell->data.ptr_value
          set $cell = $cell->next
          set $i = $i + 1
        end
      else
        printf "NULL list\n"
      end
    end
    document print_list
    Print a PostgreSQL List structure
    Usage: print_list <list_pointer>
    end

    define print_query
      set $query = (Query*)$arg0
      if $query
        printf "Query type: %d, command type: %d\n", $query->querySource, $query->commandType
        print *$query
      else
        printf "NULL query\n"
      end
    end
    document print_query
    Print a PostgreSQL Query structure
    Usage: print_query <query_pointer>
    end

    define print_relcache
      set $rel = (Relation)$arg0
      if $rel
        printf "Relation: %s.%s (OID: %u)\n", $rel->rd_rel->relnamespace, $rel->rd_rel->relname.data, $rel->rd_id
        printf "  natts: %d, relkind: %c\n", $rel->rd_rel->relnatts, $rel->rd_rel->relkind
      else
        printf "NULL relation\n"
      end
    end
    document print_relcache
    Print relation cache entry information
    Usage: print_relcache <relation_pointer>
    end

    define print_tupdesc
      set $desc = (TupleDesc)$arg0
      if $desc
        printf "TupleDesc: %d attributes\n", $desc->natts
        set $i = 0
        while $i < $desc->natts
          set $attr = $desc->attrs[$i]
          printf "  [%d]: %s (type: %u, len: %d)\n", $i, $attr->attname.data, $attr->atttypid, $attr->attlen
          set $i = $i + 1
        end
      else
        printf "NULL tuple descriptor\n"
      end
    end
    document print_tupdesc
    Print tuple descriptor information
    Usage: print_tupdesc <tupledesc_pointer>
    end

    define print_slot
      set $slot = (TupleTableSlot*)$arg0
      if $slot
        printf "TupleTableSlot: %s\n", $slot->tts_ops->name
        printf "  empty: %d, shouldFree: %d\n", $slot->tts_empty, $slot->tts_shouldFree
        if $slot->tts_tupleDescriptor
          print_tupdesc $slot->tts_tupleDescriptor
        end
      else
        printf "NULL slot\n"
      end
    end
    document print_slot
    Print tuple table slot information
    Usage: print_slot <slot_pointer>
    end

    # Memory context debugging
    define print_mcxt
      set $context = (MemoryContext)$arg0
      if $context
        printf "MemoryContext: %s\n", $context->name
        printf "  type: %s, parent: %p\n", $context->methods->name, $context->parent
        printf "  total: %zu, free: %zu\n", $context->mem_allocated, $context->freep - $context->freeptr
      else
        printf "NULL memory context\n"
      end
    end
    document print_mcxt
    Print memory context information
    Usage: print_mcxt <context_pointer>
    end

    # Process debugging
    define print_proc
      set $proc = (PGPROC*)$arg0
      if $proc
        printf "PGPROC: pid=%d, database=%u\n", $proc->pid, $proc->databaseId
        printf "  waiting: %d, waitStatus: %d\n", $proc->waiting, $proc->waitStatus
      else
        printf "NULL process\n"
      end
    end
    document print_proc
    Print process information
    Usage: print_proc <pgproc_pointer>
    end

    # Set useful defaults
    set print pretty on
    set print object on
    set print static-members off
    set print vtbl on
    set print demangle on
    set demangle-style gnu-v3
    set print sevenbit-strings off
    set history save on
    set history size 1000
    set history filename ~/.gdb_history_postgres

    # Common breakpoints for PostgreSQL debugging
    define pg_break_common
      break elog
      break errfinish
      break ExceptionalCondition
      break ProcessInterrupts
    end
    document pg_break_common
    Set common PostgreSQL debugging breakpoints
    end

    printf "PostgreSQL GDB configuration loaded.\n"
    printf "Available commands: print_node, print_list, print_query, print_relcache,\n"
    printf "                   print_tupdesc, print_slot, print_mcxt, print_proc, pg_break_common\n"
  '';

  # Flame graph generation script
  flameGraphScript = pkgs.writeScriptBin "pg-flame-generate" ''
    #!${pkgs.bash}/bin/bash
    set -euo pipefail

    DURATION=''${1:-30}
    OUTPUT_DIR=''${2:-${config.pgFlameDir}}
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)

    mkdir -p "$OUTPUT_DIR"

    echo "Generating flame graph for PostgreSQL (duration: ''${DURATION}s)"

    # Find PostgreSQL processes
    PG_PIDS=$(pgrep -f "postgres.*-D.*${config.pgDataDir}" || true)

    if [ -z "$PG_PIDS" ]; then
      echo "Error: No PostgreSQL processes found"
      exit 1
    fi

    echo "Found PostgreSQL processes: $PG_PIDS"

    # Record perf data
    PERF_DATA="$OUTPUT_DIR/perf_$TIMESTAMP.data"
    echo "Recording perf data to $PERF_DATA"

    ${pkgs.linuxPackages.perf}/bin/perf record \
      -F 997 \
      -g \
      --call-graph dwarf \
      -p "$(echo $PG_PIDS | tr ' ' ',')" \
      -o "$PERF_DATA" \
      sleep "$DURATION"

    # Generate flame graph
    FLAME_SVG="$OUTPUT_DIR/postgres_flame_$TIMESTAMP.svg"
    echo "Generating flame graph: $FLAME_SVG"

    ${pkgs.linuxPackages.perf}/bin/perf script -i "$PERF_DATA" | \
      ${pkgs.flamegraph}/bin/stackcollapse-perf.pl | \
      ${pkgs.flamegraph}/bin/flamegraph.pl \
        --title "PostgreSQL Flame Graph ($TIMESTAMP)" \
        --width 1200 \
        --height 800 \
        > "$FLAME_SVG"

    echo "Flame graph generated: $FLAME_SVG"
    echo "Perf data saved: $PERF_DATA"

    # Generate summary report
    REPORT="$OUTPUT_DIR/report_$TIMESTAMP.txt"
    echo "Generating performance report: $REPORT"

    {
      echo "PostgreSQL Performance Analysis Report"
      echo "Generated: $(date)"
      echo "Duration: ''${DURATION}s"
      echo "Processes: $PG_PIDS"
      echo ""
      echo "=== Top Functions ==="
      ${pkgs.linuxPackages.perf}/bin/perf report -i "$PERF_DATA" --stdio --sort comm,dso,symbol | head -50
      echo ""
      echo "=== Call Graph ==="
      ${pkgs.linuxPackages.perf}/bin/perf report -i "$PERF_DATA" --stdio -g --sort comm,dso,symbol | head -100
    } > "$REPORT"

    echo "Report generated: $REPORT"
    echo ""
    echo "Files created:"
    echo "  Flame graph: $FLAME_SVG"
    echo "  Perf data: $PERF_DATA"
    echo "  Report: $REPORT"
  '';

  # pgbench wrapper script
  pgbenchScript = pkgs.writeScriptBin "pg-bench-run" ''
    #!${pkgs.bash}/bin/bash
    set -euo pipefail

    # Default parameters
    CLIENTS=''${1:-10}
    THREADS=''${2:-2}
    TRANSACTIONS=''${3:-1000}
    SCALE=''${4:-10}
    DURATION=''${5:-60}
    TEST_TYPE=''${6:-tpcb-like}

    OUTPUT_DIR="${config.pgBenchDir}"
    TIMESTAMP=$(date +%Y%m%d_%H%M%S)

    mkdir -p "$OUTPUT_DIR"

    echo "=== PostgreSQL Benchmark Configuration ==="
    echo "Clients: $CLIENTS"
    echo "Threads: $THREADS"
    echo "Transactions: $TRANSACTIONS"
    echo "Scale factor: $SCALE"
    echo "Duration: ''${DURATION}s"
    echo "Test type: $TEST_TYPE"
    echo "Output directory: $OUTPUT_DIR"
    echo "============================================"

    # Check if PostgreSQL is running
    if ! pgrep -f "postgres.*-D.*${config.pgDataDir}" >/dev/null; then
      echo "Error: PostgreSQL is not running. Start it with 'pg-start'"
      exit 1
    fi

    PGBENCH="${config.pgInstallDir}/bin/pgbench"
    PSQL="${config.pgInstallDir}/bin/psql"
    CREATEDB="${config.pgInstallDir}/bin/createdb"
    DROPDB="${config.pgInstallDir}/bin/dropdb"

    DB_NAME="pgbench_test_$TIMESTAMP"
    RESULTS_FILE="$OUTPUT_DIR/results_$TIMESTAMP.txt"
    LOG_FILE="$OUTPUT_DIR/pgbench_$TIMESTAMP.log"

    echo "Creating test database: $DB_NAME"
    "$CREATEDB" -h "${config.pgDataDir}" "$DB_NAME" || {
      echo "Failed to create database"
      exit 1
    }

    # Initialize pgbench tables
    echo "Initializing pgbench tables (scale factor: $SCALE)"
    "$PGBENCH" -h "${config.pgDataDir}" -i -s "$SCALE" "$DB_NAME" || {
      echo "Failed to initialize pgbench tables"
      "$DROPDB" -h "${config.pgDataDir}" "$DB_NAME" 2>/dev/null || true
      exit 1
    }

    # Run benchmark based on test type
    echo "Running benchmark..."

    case "$TEST_TYPE" in
      "tpcb-like"|"default")
        BENCH_ARGS=""
        ;;
      "select-only")
        BENCH_ARGS="-S"
        ;;
      "simple-update")
        BENCH_ARGS="-N"
        ;;
      "read-write")
        BENCH_ARGS="-b select-only@70 -b tpcb-like@30"
        ;;
      *)
        echo "Unknown test type: $TEST_TYPE"
        echo "Available types: tpcb-like, select-only, simple-update, read-write"
        "$DROPDB" -h "${config.pgDataDir}" "$DB_NAME" 2>/dev/null || true
        exit 1
        ;;
    esac

    {
      echo "PostgreSQL Benchmark Results"
      echo "Generated: $(date)"
      echo "Test type: $TEST_TYPE"
      echo "Clients: $CLIENTS, Threads: $THREADS"
      echo "Transactions: $TRANSACTIONS, Duration: ''${DURATION}s"
      echo "Scale factor: $SCALE"
      echo "Database: $DB_NAME"
      echo ""
      echo "=== System Information ==="
      echo "CPU: $(nproc) cores"
      echo "Memory: $(free -h | grep '^Mem:' | awk '{print $2}')"
      echo "Compiler: $CC"
      echo "PostgreSQL version: $("$PSQL" --no-psqlrc -h "${config.pgDataDir}" -d "$DB_NAME" -t -c "SELECT version();" | head -1)"
      echo ""
      echo "=== Benchmark Results ==="
    } > "$RESULTS_FILE"

    # Run the actual benchmark
    "$PGBENCH" \
      -h "${config.pgDataDir}" \
      -c "$CLIENTS" \
      -j "$THREADS" \
      -T "$DURATION" \
      -P 5 \
      --log \
      --log-prefix="$OUTPUT_DIR/pgbench_$TIMESTAMP" \
      $BENCH_ARGS \
      "$DB_NAME" 2>&1 | tee -a "$RESULTS_FILE"

    # Collect additional statistics
    {
      echo ""
      echo "=== Database Statistics ==="
      "$PSQL" --no-psqlrc -h "${config.pgDataDir}" -d "$DB_NAME" -c "
        SELECT
          schemaname,
          relname,
          n_tup_ins as inserts,
          n_tup_upd as updates,
          n_tup_del as deletes,
          n_live_tup as live_tuples,
          n_dead_tup as dead_tuples
        FROM pg_stat_user_tables;
      "

      echo ""
      echo "=== Index Statistics ==="
      "$PSQL" --no-psqlrc -h "${config.pgDataDir}" -d "$DB_NAME" -c "
        SELECT
          schemaname,
          relname,
          indexrelname,
          idx_scan,
          idx_tup_read,
          idx_tup_fetch
        FROM pg_stat_user_indexes;
      "
    } >> "$RESULTS_FILE"

    # Clean up
    echo "Cleaning up test database: $DB_NAME"
    "$DROPDB" -h "${config.pgDataDir}" "$DB_NAME" 2>/dev/null || true

    echo ""
    echo "Benchmark completed!"
    echo "Results saved to: $RESULTS_FILE"
    echo "Transaction logs: $OUTPUT_DIR/pgbench_$TIMESTAMP*"

    # Show summary
    echo ""
    echo "=== Quick Summary ==="
    grep -E "(tps|latency)" "$RESULTS_FILE" | tail -5
  '';

  # Development shell (GCC + glibc)
  devShell = pkgs.mkShell {
    name = "postgresql-dev";
    buildInputs = (getPostgreSQLDeps false) ++ [
      flameGraphScript
      pgbenchScript
    ];

    shellHook = ''
      # History configuration
      export HISTFILE=.history
      export HISTSIZE=1000000
      export HISTFILESIZE=1000000

      # Clean environment
      unset LD_LIBRARY_PATH LD_PRELOAD LIBRARY_PATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH

      # Essential tools in PATH
      export PATH="${pkgs.which}/bin:${pkgs.coreutils}/bin:$PATH"

      # Ccache configuration
      export PATH=${pkgs.ccache}/bin:$PATH
      export CCACHE_COMPILERCHECK=content
      export CCACHE_DIR=$HOME/.ccache_pg_dev
      mkdir -p "$CCACHE_DIR"

      # LLVM configuration
      export LLVM_CONFIG="${llvmPkgs.llvm}/bin/llvm-config"
      export PATH="${llvmPkgs.llvm}/bin:$PATH"
      export PKG_CONFIG_PATH="${llvmPkgs.llvm.dev}/lib/pkgconfig:$PKG_CONFIG_PATH"
      export LLVM_DIR="${llvmPkgs.llvm.dev}/lib/cmake/llvm"
      export LLVM_ROOT="${llvmPkgs.llvm}"

      # Development tools in PATH
      export PATH=${pkgs.clang-tools}/bin:$PATH
      export PATH=${pkgs.cppcheck}/bin:$PATH

      # Development CFLAGS
      # -DRELCACHE_FORCE_RELEASE -DCATCACHE_FORCE_RELEASE -fno-omit-frame-pointer -fno-stack-protector -DUSE_VALGRIND
      export CFLAGS=""
      export CXXFLAGS=""

      # GCC configuration (default compiler)
      export CC="${pkgs.gcc}/bin/gcc"
      export CXX="${pkgs.gcc}/bin/g++"

      # PostgreSQL environment
      export PG_SOURCE_DIR="${config.pgSourceDir}"
      export PG_BUILD_DIR="${config.pgBuildDir}"
      export PG_INSTALL_DIR="${config.pgInstallDir}"
      export PG_DATA_DIR="${config.pgDataDir}"
      export PG_BENCH_DIR="${config.pgBenchDir}"
      export PG_FLAME_DIR="${config.pgFlameDir}"
      export PERL_CORE_DIR=$(find ${pkgs.perl} -maxdepth 5 -path "*/CORE" -type d)

      # GDB configuration
      export GDBINIT="${gdbConfig}"

      # Performance tools in PATH
      export PATH="${flameGraphScript}/bin:${pgbenchScript}/bin:$PATH"

      # Create output directories
      mkdir -p "$PG_BENCH_DIR" "$PG_FLAME_DIR"

      # Compiler verification
      echo "Environment configured:"
      echo "  Compiler: $CC"
      echo "  LibC: glibc"
      echo "  LLVM: $(llvm-config --version 2>/dev/null || echo 'not available')"
      echo "  Ccache: enabled ($CCACHE_DIR)"

      # Load PostgreSQL development aliases
      if [ -f ./pg-aliases.sh ]; then
        source ./pg-aliases.sh
      else
        echo "Warning: pg-aliases.sh not found in current directory"
      fi

      echo ""
      echo "PostgreSQL Development Environment Ready (GCC + glibc)"
      echo "Run 'pg-info' for available commands"
    '';
  };

  # Clang + glibc variant
  clangDevShell = pkgs.mkShell {
    name = "postgresql-clang-glibc";
    buildInputs = (getPostgreSQLDeps false) ++ [
      llvmPkgs.clang
      llvmPkgs.lld
      llvmPkgs.compiler-rt
      flameGraphScript
      pgbenchScript
    ];

    shellHook = ''
    export HISTFILE=.history
    export HISTSIZE=1000000
    export HISTFILESIZE=1000000

    unset LD_LIBRARY_PATH LD_PRELOAD LIBRARY_PATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH

    export PATH="${pkgs.which}/bin:${pkgs.coreutils}/bin:$PATH"

    # Ccache configuration
    export PATH=${pkgs.ccache}/bin:$PATH
    export CCACHE_COMPILERCHECK=content
    export CCACHE_DIR=$HOME/.ccache_pg_dev_clang
    mkdir -p "$CCACHE_DIR"

    # LLVM configuration
    export LLVM_CONFIG="${llvmPkgs.llvm}/bin/llvm-config"
    export PATH="${llvmPkgs.llvm}/bin:$PATH"
    export PKG_CONFIG_PATH="${llvmPkgs.llvm.dev}/lib/pkgconfig:$PKG_CONFIG_PATH"
    export LLVM_DIR="${llvmPkgs.llvm.dev}/lib/cmake/llvm"
    export LLVM_ROOT="${llvmPkgs.llvm}"

    # Development tools in PATH
    export PATH=${pkgs.clang-tools}/bin:$PATH
    export PATH=${pkgs.cppcheck}/bin:$PATH

    # Clang + glibc configuration - use system linker instead of LLD for compatibility
    export CC="${llvmPkgs.clang}/bin/clang"
    export CXX="${llvmPkgs.clang}/bin/clang++"

    # Use system linker and standard runtime
    #export CFLAGS=""
    #export CXXFLAGS=""
    #export LDFLAGS=""

    # PostgreSQL environment
    export PG_SOURCE_DIR="${config.pgSourceDir}"
    export PG_BUILD_DIR="${config.pgBuildDir}-clang"
    export PG_INSTALL_DIR="${config.pgInstallDir}-clang"
    export PG_DATA_DIR="${config.pgDataDir}-clang"
    export PG_BENCH_DIR="${config.pgBenchDir}"
    export PG_FLAME_DIR="${config.pgFlameDir}"
    export PERL_CORE_DIR=$(find ${pkgs.perl} -maxdepth 5 -path "*/CORE" -type d)

    # GDB configuration
    export GDBINIT="${gdbConfig}"

    # Performance tools in PATH
    export PATH="${flameGraphScript}/bin:${pgbenchScript}/bin:$PATH"

    # Create output directories
    mkdir -p "$PG_BENCH_DIR" "$PG_FLAME_DIR"

    # Compiler verification
    echo "Environment configured:"
    echo "  Compiler: $CC"
    echo "  LibC: glibc"
    echo "  LLVM: $(llvm-config --version 2>/dev/null || echo 'not available')"
    echo "  Ccache: enabled ($CCACHE_DIR)"

    # Load PostgreSQL development aliases
    if [ -f ./pg-aliases.sh ]; then
      source ./pg-aliases.sh
    else
      echo "Warning: pg-aliases.sh not found in current directory"
    fi

    echo ""
    echo "PostgreSQL Development Environment Ready (Clang + glibc)"
    echo "Run 'pg-info' for available commands"
  '';
  };

  # GCC + musl variant (cross-compilation)
  muslDevShell = pkgs.mkShell {
    name = "postgresql-gcc-musl";
    buildInputs = (getPostgreSQLDeps true) ++ [
      pkgs.gcc
      flameGraphScript
      pgbenchScript
    ];

    shellHook = ''
      # Same base configuration as main shell
      export HISTFILE=.history
      export HISTSIZE=1000000
      export HISTFILESIZE=1000000

      unset LD_LIBRARY_PATH LD_PRELOAD LIBRARY_PATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH

      export PATH="${pkgs.which}/bin:${pkgs.coreutils}/bin:$PATH"

      # Cross-compilation to musl
      export CC="${pkgs.gcc}/bin/gcc"
      export CXX="${pkgs.gcc}/bin/g++"

      # Point to musl libraries for linking
      export PKG_CONFIG_PATH="${pkgs.pkgsMusl.openssl.dev}/lib/pkgconfig:${pkgs.pkgsMusl.zlib.dev}/lib/pkgconfig:${pkgs.pkgsMusl.icu.dev}/lib/pkgconfig"
      export CFLAGS="-ggdb -Og -fno-omit-frame-pointer -DUSE_VALGRIND -D_FORTIFY_SOURCE=1 -I${pkgs.pkgsMusl.stdenv.cc.libc}/include"
      export CXXFLAGS="-ggdb -Og -fno-omit-frame-pointer -DUSE_VALGRIND -D_FORTIFY_SOURCE=1 -I${pkgs.pkgsMusl.stdenv.cc.libc}/include"
      export LDFLAGS="-L${pkgs.pkgsMusl.stdenv.cc.libc}/lib -static-libgcc"

      # PostgreSQL environment
      export PG_SOURCE_DIR="${config.pgSourceDir}"
      export PG_BUILD_DIR="${config.pgBuildDir}-musl"
      export PG_INSTALL_DIR="${config.pgInstallDir}-musl"
      export PG_DATA_DIR="${config.pgDataDir}-musl"
      export PG_BENCH_DIR="${config.pgBenchDir}"
      export PG_FLAME_DIR="${config.pgFlameDir}"
      export PERL_CORE_DIR=$(find ${pkgs.perl} -maxdepth 5 -path "*/CORE" -type d)

      export GDBINIT="${gdbConfig}"
      export PATH="${flameGraphScript}/bin:${pgbenchScript}/bin:$PATH"

      mkdir -p "$PG_BENCH_DIR" "$PG_FLAME_DIR"

      echo "GCC + musl environment configured"
      echo "  Compiler: $CC"
      echo "  LibC: musl (cross-compilation)"

      if [ -f ./pg-aliases.sh ]; then
        source ./pg-aliases.sh
      fi

      echo "PostgreSQL Development Environment Ready (GCC + musl)"
    '';
  };

  # Clang + musl variant (cross-compilation)
  clangMuslDevShell = pkgs.mkShell {
    name = "postgresql-clang-musl";
    buildInputs = (getPostgreSQLDeps true) ++ [
      llvmPkgs.clang
      llvmPkgs.lld
      flameGraphScript
      pgbenchScript
    ];

    shellHook = ''
      export HISTFILE=.history
      export HISTSIZE=1000000
      export HISTFILESIZE=1000000

      unset LD_LIBRARY_PATH LD_PRELOAD LIBRARY_PATH C_INCLUDE_PATH CPLUS_INCLUDE_PATH

      export PATH="${pkgs.which}/bin:${pkgs.coreutils}/bin:$PATH"

      # Cross-compilation to musl with clang
      export CC="${llvmPkgs.clang}/bin/clang"
      export CXX="${llvmPkgs.clang}/bin/clang++"

      # Point to musl libraries for linking
      export PKG_CONFIG_PATH="${pkgs.pkgsMusl.openssl.dev}/lib/pkgconfig:${pkgs.pkgsMusl.zlib.dev}/lib/pkgconfig:${pkgs.pkgsMusl.icu.dev}/lib/pkgconfig"
      export CFLAGS="--target=x86_64-linux-musl -ggdb -Og -fno-omit-frame-pointer -DUSE_VALGRIND -D_FORTIFY_SOURCE=1 -I${pkgs.pkgsMusl.stdenv.cc.libc}/include"
      export CXXFLAGS="--target=x86_64-linux-musl -ggdb -Og -fno-omit-frame-pointer -DUSE_VALGRIND -D_FORTIFY_SOURCE=1 -I${pkgs.pkgsMusl.stdenv.cc.libc}/include"
      export LDFLAGS="--target=x86_64-linux-musl -L${pkgs.pkgsMusl.stdenv.cc.libc}/lib -fuse-ld=lld"

      # PostgreSQL environment
      export PG_SOURCE_DIR="${config.pgSourceDir}"
      export PG_BUILD_DIR="${config.pgBuildDir}-clang-musl"
      export PG_INSTALL_DIR="${config.pgInstallDir}-clang-musl"
      export PG_DATA_DIR="${config.pgDataDir}-clang-musl"
      export PG_BENCH_DIR="${config.pgBenchDir}"
      export PG_FLAME_DIR="${config.pgFlameDir}"
      export PERL_CORE_DIR=$(find ${pkgs.perl} -maxdepth 5 -path "*/CORE" -type d)

      export GDBINIT="${gdbConfig}"
      export PATH="${flameGraphScript}/bin:${pgbenchScript}/bin:$PATH"

      mkdir -p "$PG_BENCH_DIR" "$PG_FLAME_DIR"

      echo "Clang + musl environment configured"
      echo "  Compiler: $CC"
      echo "  LibC: musl (cross-compilation)"

      if [ -f ./pg-aliases.sh ]; then
        source ./pg-aliases.sh
      fi

      echo "PostgreSQL Development Environment Ready (Clang + musl)"
    '';
  };

in {
  inherit devShell clangDevShell muslDevShell clangMuslDevShell gdbConfig flameGraphScript pgbenchScript;
}
