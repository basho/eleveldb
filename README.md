# `eleveldb` - Erlang bindings to LevelDB datastore 

[![Build Status](https://secure.travis-ci.org/basho/eleveldb.png?branch=master)](http://travis-ci.org/basho/eleveldb)

This repository follows the Basho standard for branch management 
as of November 28, 2013.  The standard is found here:

https://github.com/basho/riak/wiki/Basho-repository-management

In summary, the "develop" branch contains the most recently reviewed
engineering work.  The "master" branch contains the most recently
released work, i.e. distributed as part of a Riak release.

NOTE: When adding a new header dependency in the c_src directory,
make sure you run `make clean` and then `make` in order to ensure
rebar picks up your new header dependency.

