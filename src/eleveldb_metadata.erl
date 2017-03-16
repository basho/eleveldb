%% -------------------------------------------------------------------
%%
%%  eleveldb_metadata: Erlang interface to allow LevelDB to retrieve riak_core metadata
%%
%% Copyright (c) 2017 Basho Technologies, Inc. All Rights Reserved.
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
-module(eleveldb_metadata).

-export([handle_metadata_response/1]).

%% When eleveldb needs metadata, it routes through a `riak_core_info_service_process` process to get
%% the required data. This handles the response from that process, which will call the function given
%% in the initialization (currently done in `riak_core_app` - TODO: Make this more generic)
%% and call this function to return the results.
-spec handle_metadata_response({Props::proplists:proplist(), _SourceParams::list(), Key::term()}) -> ok.
handle_metadata_response({Props, _SourceParams, [Key]}) ->
    eleveldb:property_cache(Key, Props).
