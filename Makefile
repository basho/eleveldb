.PHONY: compile rel cover test dialyzer get-deps
REBAR=./rebar3

all: compile

get-deps:
	$(REBAR) get-deps

compile: get-deps
	$(REBAR) compile

clean:
	$(REBAR) clean

cover: test
	$(REBAR) cover

test: compile
	$(MAKE) -C c_src test
	$(REBAR) as test do eunit

dialyzer:
	$(REBAR) dialyzer

xref:
	$(REBAR) xref

check: test dialyzer xref
