all: compile

get-deps:
	./c_src/build_deps.sh get-deps

deps:
	rebar3 get-deps

rm-deps:
	./c_src/build_deps.sh rm-deps

compile: deps
	rebar3 compile

clean:
	${REBAR} clean

include tools.mk
