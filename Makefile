
all: compile

compile:
	./rebar compile

test: compile
	./rebar eunit

clean:
	./rebar clean
