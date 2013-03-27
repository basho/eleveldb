
all: compile

get-deps:
	./c_src/build_deps.sh get-deps

rm-deps:
	./c_src/build_deps.sh rm-deps

compile:
	./rebar compile

test: compile
	./rebar eunit

clean:
	./rebar clean
