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
	#$(REBAR) as test do eunit
	$(MAKE) -C c_src test

dialyzer:
	$(REBAR) dialyzer

xref:
	$(REBAR) xref

check: test dialyzer xref
