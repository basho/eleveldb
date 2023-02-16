LEVELDB_VSN ?= "2.0.38"
BASEDIR = $(shell pwd)

LDFLAGS := $(LDFLAGS) -L$(BASEDIR)/system/lib
LD_LIBRARY_PATH := $(BASEDIR)/system/lib:$(LD_LIBRARY_PATH)
CFLAGS := $(CFLAGS) -I $(BASEDIR)/system/include -I. -I $(BASEDIR)/leveldb/include -fPIC
CXXFLAGS := $(CXXFLAGS) -I $(BASEDIR)/system/include -I. -I $(BASEDIR)/leveldb/include -fPIC

get-deps:
	git config --global --add safe.directory /__w/eleveldb/eleveldb
	echo "ubuntu-latest image with otp-22, are you happy now?"
	if [ ! -d leveldb ]; then \
	    git clone --depth=1 --branch=$(LEVELDB_VSN) https://github.com/basho/leveldb && \
	    (cd leveldb && git submodule update --depth 1 --init); \
	fi

compile: get-deps ldb
	cp leveldb/perf_dump leveldb/sst_rewrite leveldb/sst_scan leveldb/leveldb_repair ../priv

ldb:
	$(MAKE) LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb all
	$(MAKE) LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb tools

clean:
	$(MAKE) -C leveldb clean
	rm -rf system

test: compile
	$(MAKE) CXXFLAGS="$(CXXFLAGS) -Wno-narrowing" LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb test
