#!/bin/bash

LEVELDB_VSN="c8c5866a86c8d4a3e80d8708d14a06776fb683d1"
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

    *)
        if [ ! -d snappy-$SNAPPY_VSN ]; then
            tar -xjf snappy-$SNAPPY_VSN.tar.gz
            (cd snappy-$SNAPPY_VSN && ./configure --prefix=$BASEDIR/system)
        fi

        (cd snappy-$SNAPPY_VSN && make && make install)

        export CFLAGS="$CFLAGS -I $BASEDIR/system/include"
        export LDFLAGS="$LDFLAGS -L $BASEDIR/system/lib"

        if [ ! -d leveldb ]; then
            tar -xjf leveldb.tar.bz2
            (cd leveldb && git checkout $VSN)

            for p in patches/*.patch; do
                echo "Applying $p"
                (cd leveldb && patch -p1 < ../$p)
            done

        fi

        (cd leveldb && make)

        ;;
esac

