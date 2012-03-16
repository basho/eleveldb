#!/bin/bash

LEVELDB_VSN="b921bc7197c50e47a01608d8728ff3dcacfe0c30" # Mar 15 merge of mainline
SNAPPY_VSN="1.0.4"

set -e

if [ `basename $PWD` != "c_src" ]; then
    pushd c_src
fi

BASEDIR="$PWD"

case "$1" in
    clean)
        rm -rf leveldb system snappy-$SNAPPY_VSN
        ;;

    test)
        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export LDFLAGS="$LDFLAGS -L $BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/system/lib:$LD_LIBRARY_PATH"

        (cd leveldb && make check)

        ;;
    *)
        if [ ! -d snappy-$SNAPPY_VSN ]; then
            tar -xzf snappy-$SNAPPY_VSN.tar.gz
            (cd snappy-$SNAPPY_VSN && ./configure --prefix=$BASEDIR/system --with-pic)
        fi

        (cd snappy-$SNAPPY_VSN && make && make install)

        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export LDFLAGS="$LDFLAGS -L $BASEDIR/system/lib"
        export LD_LIBRARY_PATH="$BASEDIR/system/lib:$LD_LIBRARY_PATH"

        if [ ! -d leveldb ]; then
            git clone git://github.com/basho/leveldb
            (cd leveldb && git checkout $LEVELDB_VSN)
        fi

        (cd leveldb && make all)

        ;;
esac

