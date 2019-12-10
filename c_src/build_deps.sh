#!/bin/sh

# /bin/sh on Solaris is not a POSIX compatible shell, but /usr/bin/ksh is.
if [ `uname -s` = 'SunOS' -a "${POSIX_SHELL}" != "true" ]; then
    POSIX_SHELL="true"
    export POSIX_SHELL
    exec /usr/bin/ksh $0 $@
fi
unset POSIX_SHELL # clear it so if we invoke other scripts, they run as ksh as well

LEVELDB_VSN=""

SNAPPY_VSN="1.0.4"

set -e

if [ `basename $PWD` != "c_src" ]; then
    # originally "pushd c_src" of bash
    # but no need to use directory stack push here
    cd c_src
fi

BASEDIR="$PWD"

# detecting gmake and if exists use it
# if not use make
# (code from github.com/tuncer/re2/c_src/build_deps.sh
which gmake 1>/dev/null 2>/dev/null && MAKE=gmake
MAKE=${MAKE:-make}

# Changed "make" to $MAKE

case "$1" in
    rm-deps)
        rm -rf leveldb system snappy-$SNAPPY_VSN
        ;;

    clean)
        rm -rf system snappy-$SNAPPY_VSN
        if [ -d leveldb ]; then
            (cd leveldb && $MAKE clean)
        fi
        rm -f ../priv/leveldb_repair ../priv/sst_scan ../priv/sst_rewrite ../priv/perf_dump
        ;;

    test)
        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export CXXFLAGS="$CXXFLAGS -I $BASEDIR/system/include"
        export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/system/lib:$LD_LIBRARY_PATH"
        export LEVELDB_VSN="$LEVELDB_VSN"

        (cd leveldb && $MAKE check)

        ;;

    get-deps)
        if [ ! -d leveldb ]; then
            git clone git://github.com/basho/leveldb
            (cd leveldb && git checkout $LEVELDB_VSN)
            if [ "$BASHO_EE" = "1" ]; then
                (cd leveldb && git submodule update --init)
            fi
        fi
        ;;

    *)
        export MACOSX_DEPLOYMENT_TARGET=10.8

        if [ ! -d snappy-$SNAPPY_VSN ]; then
            tar -xzf snappy-$SNAPPY_VSN.tar.gz
            (cd snappy-$SNAPPY_VSN && ./configure --disable-shared --prefix=$BASEDIR/system --libdir=$BASEDIR/system/lib --with-pic)
        fi

        if [ ! -f system/lib/libsnappy.a ]; then
            (cd snappy-$SNAPPY_VSN && $MAKE -stdlib=libc++ && $MAKE -stdlib=libc++ install)
        fi

        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export CXXFLAGS="$CXXFLAGS -I $BASEDIR/system/include"
        export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/system/lib:$LD_LIBRARY_PATH"
        export LEVELDB_VSN="$LEVELDB_VSN"

        if [ ! -d leveldb ]; then
            git clone git://github.com/basho/leveldb
            (cd leveldb && git checkout $LEVELDB_VSN)
            if [ $BASHO_EE = "1" ]; then
                (cd leveldb && git submodule update --init)
            fi
        fi

        # hack issue where high level make is running -j 4
        #  and causes build errors in leveldb
        export MAKEFLAGS=

        (cd leveldb && $MAKE -j 3 all)
        (cd leveldb && $MAKE -j 3 tools)
        (cp leveldb/perf_dump leveldb/sst_rewrite leveldb/sst_scan leveldb/leveldb_repair ../priv)

        ;;
esac
