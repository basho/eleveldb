%% -------------------------------------------------------------------
%%
%%  eleveldb: Erlang Wrapper for LevelDB (http://code.google.com/p/leveldb/)
%%
%% Copyright (c) 2010-2013 Basho Technologies, Inc. All Rights Reserved.
%%
%% This file is provided to you under the Apache License,
%% Version 2.0 (the "License"); you may not use this file
%% except in compliance with the License.  You may obtain
%% a copy of the License at
%%
%%   http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing,
%% software distributed under the License is distributed on an
%% "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
%% KIND, either express or implied.  See the License for the
%% specific language governing permissions and limitations
%% under the License.
%%
%% -------------------------------------------------------------------
-module(iterators).

-compile(export_all).

-include_lib("eunit/include/eunit.hrl").

prev_test() ->
    os:cmd("rm -rf ltest"),  % NOTE
    {ok, Ref} = eleveldb:open("ltest", [{create_if_missing, true}]),
    try
      eleveldb:put(Ref, <<"a">>, <<"x">>, []),
      eleveldb:put(Ref, <<"b">>, <<"y">>, []),
      {ok, I} = eleveldb:iterator(Ref, []),
        ?assertEqual({ok, <<"a">>, <<"x">>},eleveldb:iterator_move(I, <<>>)),
        ?assertEqual({ok, <<"b">>, <<"y">>},eleveldb:iterator_move(I, next)),
        ?assertEqual({ok, <<"a">>, <<"x">>},eleveldb:iterator_move(I, prev))
    after
      eleveldb:close(Ref)
    end.
