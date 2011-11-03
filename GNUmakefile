
all: compile

compile:
	./rebar compile

test: compile
	./rebar eunit

clean:
	./rebar clean

c_src:
	cd c_src/leveldb; $(MAKE)

c_src_clean:
	cd c_src/leveldb; $(MAKE) clean

.PHONY: c_src
