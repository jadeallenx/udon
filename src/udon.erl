-module(udon).
-include("udon.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-export([
         ping/0,
         store/2,
         rename/2,
         fetch/1
        ]).

-ignore_xref([
              ping/0,
              store/2,
              rename/2,
              fetch/1
             ]).

%% Public API

%% @doc Stores a static file at the given path
store(Path, Data) ->
    N = 3,
    W = 3,
    Timeout = 5000, % millisecs

    PHash = path_to_hash(Path),
    PRec = #file{ request_path = Path, path_md5 = PHash, csum = erlang:adler32(Data) },

    {ok, ReqId} = udon_op_fsm:op(N, W, {store, PRec, Data}, ?KEY(PHash)),
    wait_for_reqid(ReqId, Timeout).

%% @private Given a path and a new path, update the metadata and write the redirect URL
%% as a new version of the data blob for the given path.
store(redirect, Path, NewPath) ->
    N = 3,
    W = 3,
    Timeout = 5000, % millisecs

    PHash = path_to_hash(Path),
    PRec = #file{ request_path = Path, path_md5 = PHash, redirect = true, 
                  csum = erlang:adler32(NewPath) },

    {ok, ReqId} = udon_op_fsm:op(N, W, {store, PRec, NewPath}, ?KEY(PHash)),
    wait_for_reqid(ReqId, Timeout).

%% @doc Retrieves a static file from the given path
fetch(Path) ->
    PHash = path_to_hash(Path),
    Idx = riak_core_util:chash_key(?KEY(PHash)),
    %% TODO: Get a preflist with more than one node
    [{Node, _Type}] = riak_core_apl:get_primary_apl(Idx, 1, udon),
    riak_core_vnode_master:sync_spawn_command(Node, {fetch, PHash}, udon_vnode_master).

%% @doc Move the data at Path to Newpath and mark Path (the old path) as a redirect
%% to the new URL.
rename(Path, NewPath) ->
    Data = case fetch(Path) of
              not_found -> throw({not_found, Path});
              {_, X} -> X
    end,
    store(NewPath, Data),
    store(redirect, Path, NewPath).
    
%% @doc Pings a random vnode to make sure communication is functional
ping() ->
    DocIdx = riak_core_util:chash_key({<<"ping">>, term_to_binary(now())}),
    PrefList = riak_core_apl:get_primary_apl(DocIdx, 1, udon),
    [{IndexNode, _Type}] = PrefList,
    riak_core_vnode_master:sync_spawn_command(IndexNode, ping, udon_vnode_master).

wait_for_reqid(Id, Timeout) ->
    receive {Id, Value} -> {ok, Value}
    after Timeout -> {error, timeout}
    end.

path_to_hash(Path) when is_list(Path) ->
    crypto:hash(md5, Path).

