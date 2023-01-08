LEVELDB_VSN ?= "2.0.38"
SNAPPY_VSN ?= "1.1.9"
BASEDIR = $(shell pwd)

LDFLAGS := $(LDFLAGS) -L$(BASEDIR)/system/lib
LD_LIBRARY_PATH := $(BASEDIR)/system/lib:$(LD_LIBRARY_PATH)
CFLAGS := $(CFLAGS) -I $(BASEDIR)/system/include -I. -I $(BASEDIR)/leveldb/include -fPIC
CXXFLAGS := $(CXXFLAGS) -I $(BASEDIR)/system/include -I. -I $(BASEDIR)/leveldb/include -fPIC

get-deps:
	if [ ! -r snappy-$(SNAPPY_VSN).tar.gz ]; then \
	    wget -O snappy-$(SNAPPY_VSN).tar.gz https://github.com/google/snappy/archive/refs/tags/$(SNAPPY_VSN).tar.gz; \
	fi
	if [ ! -d leveldb ]; then \
	    git clone https://github.com/basho/leveldb && \
	    (cd leveldb && git checkout $(LEVELDB_VSN)) && \
	    (cd leveldb && git submodule update --init); \
	fi

compile: get-deps snappy ldb
	cp leveldb/perf_dump leveldb/sst_rewrite leveldb/sst_scan leveldb/leveldb_repair ../priv

ldb:
	$(MAKE) LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb all
	$(MAKE) LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb tools

snappy: system/lib/libsnappy.a

system/lib/libsnappy.a:
	tar -xzf snappy-$(SNAPPY_VSN).tar.gz && \
	(cd snappy-$(SNAPPY_VSN) && \
	 git submodule update --init && \
	 if [ -r autogen.sh ]; then \
	   ./autogen.sh && ./configure --prefix=$(BASEDIR)/system && $(MAKE) && $(MAKE) install; \
	 else \
	   mkdir build && cd build && \
	   mkdir -p $(BASEDIR)/system && \
	   cmake -D SNAPPY_BUILD_TESTS=0 -D SNAPPY_BUILD_BENCHMARKS=0 \
	         -D CMAKE_INSTALL_PREFIX=$(BASEDIR)/system \
	     ..; \
	  fi && \
	  $(MAKE) && $(MAKE) install)
	mv system/lib64 system/lib || true

clean:
	$(MAKE) -C leveldb clean
	rm -rf system snappy-$(SNAPPY_VSN)/build

test: compile
	$(MAKE) CXXFLAGS="$(CXXFLAGS) -Wno-narrowing" LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb test
