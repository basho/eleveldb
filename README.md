# `eleveldb` - Erlang bindings to LevelDB datastore 

[![Build Status](https://secure.travis-ci.org/basho/eleveldb.png?branch=master)](http://travis-ci.org/basho/eleveldb)

This repository follows the Basho standard for branch management 
as of November 28, 2013.  The standard is found here:

https://github.com/basho/riak/wiki/Basho-repository-management

In summary, the "develop" branch contains the most recently reviewed
engineering work.  The "master" branch contains the most recently
released work, i.e. distributed as part of a Riak release.

# Some Usage Information

## Iterator operations are:

- **next:** move forward one position and return the value; do nothing else

- **prefetch:** perform a `next` operation and then start a parallel call for the subsequent `next` while Erlang processes the current `next`.  The subsequent `prefetch` may return immediately with the value already retrieved.

- **prefetch-stop:** stop a sequence of `prefetch` calls.  if there is a parallel `prefetch` pending, cancel it, since we are about to move the pointer

- **seek:** move iterator to a new position

Either use `prefetch`/`prefetch_stop` or `next`.  Do not intermix `prefetch` and `next`.  You must `prefetch_stop` after one or more `prefetch` operations before using any of the other operations (`next`, `seek`, `prev`).

