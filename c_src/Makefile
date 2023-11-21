LEVELDB_VSN ?= "2.0.38"
C_SRC_DIR := $(shell pwd)
PROJECT_DIR := $(abspath $(C_SRC_DIR)/..)
TARGET_DIR := $(PROJECT_DIR)/priv/

# LevelDB tools
TOOLS = \
	leveldb_repair \
	perf_dump \
	sst_rewrite \
	sst_scan

LDFLAGS := $(LDFLAGS) -lsnappy -lpthread
CFLAGS := $(CFLAGS) -I. -I $(C_SRC_DIRC_SRC_DIR)/leveldb/include -fPIC
CXXFLAGS := $(CXXFLAGS) -I. -I $(C_SRC_DIR)/leveldb/include -fPIC
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
	if [ ! -d leveldb ]; then \
	    git clone --depth=1 --branch=$(LEVELDB_VSN) https://github.com/basho/leveldb && \
	    (cd leveldb && git submodule update --depth 1 --init); \
	fi

compile: get-deps ldb
	for tool in $(TOOLS); do cp leveldb/$$tool $(TARGET_DIR); done

ldb:
	@echo "Building LevelDB..."
	$(MAKE) LDFLAGS="" -C leveldb all
	$(MAKE) LDFLAGS="$(LDFLAGS)" -C leveldb tools

clean:
	$(MAKE) -C leveldb clean
	for tool in $(TOOLS); do rm -f $(TARGET_DIR)$$tool; done

test: compile
	$(MAKE) LDFLAGS="$(LDFLAGS)" CXXFLAGS="$(TEST_CXXFLAGS)" CFLAGS="$(CFLAGS)" -C leveldb test
