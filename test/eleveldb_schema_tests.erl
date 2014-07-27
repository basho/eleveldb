-module(eleveldb_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-compile(export_all).


%% basic schema test will check to make sure that all defaults from
%% the schema make it into the generated app.config
basic_schema_test() ->
    %% The defaults are defined in ../priv/eleveldb.schema.
    %% it is the file under test.
    Config = cuttlefish_unit:generate_templated_config(
        ["../priv/eleveldb.schema"], [], context(), predefined_schema()),

    cuttlefish_unit:assert_config(Config, "eleveldb.data_root", "./data/leveldb"),
    cuttlefish_unit:assert_config(Config, "eleveldb.total_leveldb_mem_percent", 70),
    cuttlefish_unit:assert_not_configured(Config, "eleveldb.total_leveldb_mem"),
    cuttlefish_unit:assert_config(Config, "eleveldb.sync", false),
    cuttlefish_unit:assert_config(Config, "eleveldb.limited_developer_mem", false),
    cuttlefish_unit:assert_config(Config, "eleveldb.write_buffer_size_min", 31457280),
    cuttlefish_unit:assert_config(Config, "eleveldb.write_buffer_size_max", 62914560),
    cuttlefish_unit:assert_config(Config, "eleveldb.use_bloomfilter", true),
    cuttlefish_unit:assert_config(Config, "eleveldb.sst_block_size", 4096),
    cuttlefish_unit:assert_config(Config, "eleveldb.block_restart_interval", 16),
    cuttlefish_unit:assert_config(Config, "eleveldb.verify_checksums", true),
    cuttlefish_unit:assert_config(Config, "eleveldb.verify_compaction", true),
    cuttlefish_unit:assert_config(Config, "eleveldb.eleveldb_threads", 71),
    cuttlefish_unit:assert_config(Config, "eleveldb.fadvise_willneed", false),
    cuttlefish_unit:assert_config(Config, "eleveldb.delete_threshold", 1000),
    cuttlefish_unit:assert_config(Config, "eleveldb.compression", true),
    cuttlefish_unit:assert_config(Config, "eleveldb.tiered_slow_level", 0),
    cuttlefish_unit:assert_not_configured(Config, "eleveldb.tiered_fast_prefix"),
    cuttlefish_unit:assert_not_configured(Config, "eleveldb.tiered_slow_prefix"),

    %% Make sure no multi_backend
    %% Warning: The following line passes by coincidence. It's because the
    %% first mapping in the schema has no default defined. Testing strategy
    %% for multibackend needs to be revisited.
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.multi_backend"),
    ok.

override_schema_test() ->
    %% Conf represents the riak.conf file that would be read in by cuttlefish.
    %% this proplists is what would be output by the conf_parse module
    Conf = [
            {["leveldb", "data_root"], "/some/crazy/dir"},
            {["leveldb", "maximum_memory", "percent"], 50},
            {["leveldb", "maximum_memory"], "1KB"},
            {["leveldb", "sync_on_write"], on},
            {["leveldb", "limited_developer_mem"], on},
            {["leveldb", "write_buffer_size_min"], "10MB"},
            {["leveldb", "write_buffer_size_max"], "20MB"},
            {["leveldb", "bloomfilter"], off},
            {["leveldb", "block", "size"], "8KB"},
            {["leveldb", "block", "restart_interval"], 8},
            {["leveldb", "verify_checksums"], off},
            {["leveldb", "verify_compaction"], off},
            {["leveldb", "threads"], 7},
            {["leveldb", "fadvise_willneed"], true},
            {["leveldb", "compression"], off},
            {["leveldb", "compaction", "trigger", "tombstone_count"], off},
            {["leveldb", "tiered"], "2"},
            {["leveldb", "tiered", "path", "fast"], "/mnt/speedy"},
            {["leveldb", "tiered", "path", "slow"], "/mnt/slowpoke"}
           ],

    %% The defaults are defined in ../priv/eleveldb.schema.
    %% it is the file under test.
    Config = cuttlefish_unit:generate_templated_config(
        ["../priv/eleveldb.schema"], Conf, context(), predefined_schema()),

    cuttlefish_unit:assert_config(Config, "eleveldb.data_root", "/some/crazy/dir"),
    cuttlefish_unit:assert_config(Config, "eleveldb.total_leveldb_mem_percent", 50),
    cuttlefish_unit:assert_config(Config, "eleveldb.total_leveldb_mem", 1024),
    cuttlefish_unit:assert_config(Config, "eleveldb.sync", true),
    cuttlefish_unit:assert_config(Config, "eleveldb.limited_developer_mem", true),
    cuttlefish_unit:assert_config(Config, "eleveldb.write_buffer_size_min", 10485760),
    cuttlefish_unit:assert_config(Config, "eleveldb.write_buffer_size_max", 20971520),
    cuttlefish_unit:assert_config(Config, "eleveldb.use_bloomfilter", false),
    cuttlefish_unit:assert_config(Config, "eleveldb.sst_block_size", 8192),
    cuttlefish_unit:assert_config(Config, "eleveldb.block_restart_interval", 8),
    cuttlefish_unit:assert_config(Config, "eleveldb.verify_checksums", false),
    cuttlefish_unit:assert_config(Config, "eleveldb.verify_compaction", false),
    cuttlefish_unit:assert_config(Config, "eleveldb.eleveldb_threads", 7),
    cuttlefish_unit:assert_config(Config, "eleveldb.fadvise_willneed", true),
    cuttlefish_unit:assert_config(Config, "eleveldb.delete_threshold", 0),
    cuttlefish_unit:assert_config(Config, "eleveldb.compression", false),
    cuttlefish_unit:assert_config(Config, "eleveldb.tiered_slow_level", 2),
    cuttlefish_unit:assert_config(Config, "eleveldb.tiered_fast_prefix", "/mnt/speedy"),
    cuttlefish_unit:assert_config(Config, "eleveldb.tiered_slow_prefix", "/mnt/slowpoke"),

    %% Make sure no multi_backend
    %% Warning: The following line passes by coincidence. It's because the
    %% first mapping in the schema has no default defined. Testing strategy
    %% for multibackend needs to be revisited.
    cuttlefish_unit:assert_not_configured(Config, "riak_kv.multi_backend"),
    ok.

multi_backend_test() ->
    Conf = [
            {["multi_backend", "default", "storage_backend"], leveldb},
            {["multi_backend", "default", "leveldb", "data_root"], "/data/default_leveldb"}
           ],
    Config = cuttlefish_unit:generate_templated_config(
               ["../priv/eleveldb.schema", "../priv/eleveldb_multi.schema", "../test/multi_backend.schema"],
               Conf, context(), predefined_schema()),

    MultiBackendConfig = proplists:get_value(multi_backend, proplists:get_value(riak_kv, Config)),

    {<<"default">>, riak_kv_eleveldb_backend, DefaultBackend} = lists:keyfind(<<"default">>, 1, MultiBackendConfig),

    cuttlefish_unit:assert_config(DefaultBackend, "data_root", "/data/default_leveldb"),

    cuttlefish_unit:assert_config(DefaultBackend, "total_leveldb_mem_percent", 35),
    cuttlefish_unit:assert_not_configured(DefaultBackend, "total_leveldb_mem"),
    cuttlefish_unit:assert_config(DefaultBackend, "sync", false),
    cuttlefish_unit:assert_config(DefaultBackend, "limited_developer_mem", false),
    cuttlefish_unit:assert_config(DefaultBackend, "write_buffer_size_min", 15728640),
    cuttlefish_unit:assert_config(DefaultBackend, "write_buffer_size_max", 31457280),
    cuttlefish_unit:assert_config(DefaultBackend, "use_bloomfilter", true),
    cuttlefish_unit:assert_config(DefaultBackend, "sst_block_size", 4096),
    cuttlefish_unit:assert_config(DefaultBackend, "block_restart_interval", 16),
    cuttlefish_unit:assert_config(DefaultBackend, "verify_checksums", true),
    cuttlefish_unit:assert_config(DefaultBackend, "verify_compaction", true),
    cuttlefish_unit:assert_config(DefaultBackend, "eleveldb_threads", 71),
    cuttlefish_unit:assert_config(DefaultBackend, "fadvise_willneed", false),
    cuttlefish_unit:assert_config(DefaultBackend, "delete_threshold", 1000),
    ok.

%% this context() represents the substitution variables that rebar
%% will use during the build process.  riak_core's schema file is
%% written with some {{mustache_vars}} for substitution during
%% packaging cuttlefish doesn't have a great time parsing those, so we
%% perform the substitutions first, because that's how it would work
%% in real life.
context() -> [].

%% This predefined schema covers riak_kv's dependency on
%% platform_data_dir
predefined_schema() ->
    Mapping = cuttlefish_mapping:parse({mapping,
                                        "platform_data_dir",
                                        "riak_core.platform_data_dir", [
                                            {default, "./data"},
                                            {datatype, directory}
                                       ]}),
    {[], [Mapping], []}.
