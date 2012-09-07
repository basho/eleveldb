#!/bin/sh

# /bin/sh on Solaris is not a POSIX compatible shell, but /usr/bin/ksh is.
if [ `uname -s` = 'SunOS' -a "${POSIX_SHELL}" != "true" ]; then
    POSIX_SHELL="true"
    export POSIX_SHELL
    exec /usr/bin/ksh $0 $@
fi
unset POSIX_SHELL # clear it so if we invoke other scripts, they run as ksh as well

LEVELDB_VSN="1.2.2p4"                                  # June 25 merge w/ mv-write-throttle2
                                                       # June 29 merge w/ mv-compress-msg for
                                                       # compression LOG fix
                                                       # July 18 merge w/ BLOCK.bad fixes
                                                       # July 30 merge w/ cache lru fixes
                                                       # Aug  01 merge w/ LRUCache destructor
                                                       #         fixes
                                                       # Aug  10 merge w/ L0 numfiles limit
                                                       # Aug  24 merge w/ mv-compact-inversion
                                                       #                  mv-cache-release-fix
                                                       # Sept 6 Create new write throttle that
                                                       #    utilizes actual system write times
                                                       #    per key as part of its pushback calculations.
                                                       # Sept 6 Adjust the polling timeout from 5 to 1.
                                                       # Sept 7 Fix builds for bsd/osx
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
    clean)
        rm -rf leveldb system snappy-$SNAPPY_VSN
        ;;

    test)
        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export CXXFLAGS="$CXXFLAGS -I $BASEDIR/system/include"
        export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/system/lib:$LD_LIBRARY_PATH"

        (cd leveldb && $MAKE check)

        ;;

    get-deps)
        if [ ! -d leveldb ]; then
            git clone git://github.com/basho/leveldb
            (cd leveldb && git checkout $LEVELDB_VSN)
        fi
        ;;

    *)
        if [ ! -d snappy-$SNAPPY_VSN ]; then
            tar -xzf snappy-$SNAPPY_VSN.tar.gz
            (cd snappy-$SNAPPY_VSN && ./configure --prefix=$BASEDIR/system --with-pic)
        fi

        (cd snappy-$SNAPPY_VSN && $MAKE && $MAKE install)

        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export CXXFLAGS="$CXXFLAGS -I $BASEDIR/system/include"
        export LDFLAGS="$LDFLAGS -L$BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/system/lib:$LD_LIBRARY_PATH"

        if [ ! -d leveldb ]; then
            git clone git://github.com/basho/leveldb
            (cd leveldb && git checkout $LEVELDB_VSN)
        fi

        (cd leveldb && $MAKE all)

        ;;
esac
