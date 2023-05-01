LEVELDB_SRC := $(REBAR_DEPS_DIR)/leveldb/c_src
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
CFLAGS := $(CFLAGS) -I. -I $(LEVELDB_SRC)/include -fPIC
CXXFLAGS := $(CXXFLAGS) -I. -I $(LEVELDB_SRC)/include -fPIC
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

compile: ldb
	for tool in $(TOOLS); do cp $(LEVELDB_SRC)/$$tool $(TARGET_DIR); done

ldb:
	@echo "Building LevelDB..."
	$(MAKE) LDFLAGS="" -C $(LEVELDB_SRC) all
	$(MAKE) LDFLAGS="$(LDFLAGS)" -C $(LEVELDB_SRC) tools

clean:
	$(MAKE) -C $(LEVELDB_SRC) clean
	for tool in $(TOOLS); do rm -f $(TARGET_DIR)$$tool; done

test: compile
	$(MAKE) LDFLAGS="$(LDFLAGS)" CXXFLAGS="$(TEST_CXXFLAGS)" CFLAGS="$(CFLAGS)" -C $(LEVELDB_SRC) test
