REBAR := ./rebar

all: compile

get-deps:
	./c_src/build_deps.sh get-deps

rm-deps:
	./c_src/build_deps.sh rm-deps

compile:
	$(REBAR) compile

test: compile
	$(REBAR) eunit

clean:
	$(REBAR) clean
