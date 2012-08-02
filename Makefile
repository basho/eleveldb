REBAR := ./rebar

all: compile

compile:
	$(REBAR) compile

test: compile
	$(REBAR) eunit

clean:
	$(REBAR) clean
