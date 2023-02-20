LEVELDB_VSN ?= "2.0.38"
BASEDIR = $(shell pwd)

LDFLAGS := $(LDFLAGS) -L$(BASEDIR)/system/lib
LD_LIBRARY_PATH=$(BASEDIR)/system/lib:$(LD_LIBRARY_PATH)
CFLAGS += -I $(BASEDIR)/leveldb/include -fPIC
CXXFLAGS += -I $(BASEDIR)/leveldb/include -fPIC
TEST_CXXFLAGS := $(CXXFLAGS) -Wno-narrowing

ifeq ($(shell uname -s), Darwin)

	# OSX with homebrew
  HAS_BREW := $(shell command -v brew;)

	ifdef HAS_BREW
		SNAPPY_DIR ?= $(shell brew --prefix snappy)
	endif

	# Default dir (Mac ports)
	SNAPPY_DIR ?= /usr/local/opt/snappy

  LDFLAGS += -L${SNAPPY_DIR}/lib

	# Resolves C++11 narrowing error on Mac OS
	TEST_CXXFLAGS += -Wno-c++11-narrowing
endif

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
	@echo "Building LevelDB..."
	$(MAKE) LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb all
	$(MAKE) LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb tools

clean:
	$(MAKE) -C leveldb clean

test: compile
	$(MAKE) PLATFORM_CXXFLAGS="$(TEST_CXXFLAGS)" LDFLAGS="$(LDFLAGS) -lsnappy -lpthread" LD_LIBRARY_PATH="$(LD_LIBRARY_PATH)" -C leveldb test
