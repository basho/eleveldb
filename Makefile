
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

APPS = kernel stdlib sasl erts ssl tools os_mon runtime_tools crypto inets \
	xmerl webtool eunit syntax_tools compiler
PLT = $(HOME)/.eleveldb_dialyzer_plt

check_plt: 
	dialyzer --check_plt --plt $(PLT) --apps $(APPS)

build_plt: 
	dialyzer --build_plt --output_plt $(PLT) --apps $(APPS)

dialyzer: 
	@echo
	@echo Use "'make check_plt'" to check PLT prior to using this target.
	@echo Use "'make build_plt'" to build PLT prior to using this target.
	@echo
	@sleep 1
	dialyzer -Wno_return -Wunmatched_returns -Wrace_conditions \
		--plt $(PLT) ebin | tee .dialyzer.raw-output

cleanplt:
	@echo
	@echo "Are you sure?  It takes about 1/2 hour to re-build."
	@echo Deleting $(PLT) in 5 seconds.
	@echo
	sleep 5
	rm $(PLT)
