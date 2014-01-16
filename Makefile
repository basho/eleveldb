REBAR_BIN := $(shell which rebar)
ifeq ($(REBAR_BIN),)
REBAR_BIN = ./rebar
endif

all: compile

get-deps:
	./c_src/build_deps.sh get-deps

deps:
	$(REBAR_BIN) get-deps

rm-deps:
	./c_src/build_deps.sh rm-deps

compile: deps
	./rebar compile

clean:
	./rebar clean

include tools.mk
