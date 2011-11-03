# This is a stub Makefile that invokes GNU make which will read the GNUmakefile
# instead of this file. This provides compatability on systems where GNU make is
# not the system 'make' (eg. most non-linux UNIXes).

all:
	@gmake all

verbose:
	@gmake verbose

test:
	@gmake test

docs:
	@gmake docs

c_src: FORCE
	@gmake c_src
FORCE:

c_src_clean:
	@gmake c_src_clean

clean:
	@gmake clean
