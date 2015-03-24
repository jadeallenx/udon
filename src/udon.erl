-module(udon).
-include("udon.hrl").
-include_lib("riak_core/include/riak_core_vnode.hrl").

-define(BUCKET(X), {list_to_binary(X), list_to_binary(X)}).

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

    {ok, ReqId} = udon_op_fsm:op(N, W, {store, Path, Data}, ?BUCKET(Path)),
    wait_for_reqid(ReqId, Timeout).

store(redirect, Path, NewPath) ->
    N = 3,
    W = 3,
    Timeout = 5000, % millisecs

    {ok, ReqId} = udon_op_fsm:op(N, W, {redirect, Path, NewPath}, ?BUCKET(Path)),
    wait_for_reqid(ReqId, Timeout).

%% @doc Retrieves a static file from the given path
fetch(Path) ->
    Idx = riak_core_util:chash_key({list_to_binary(Path), list_to_binary(Path)}),
    %% TODO: Get a preflist with more than one node
    [{Node, _Type}] = riak_core_apl:get_primary_apl(Idx, 1, udon),
    riak_core_vnode_master:sync_spawn_command(Node, {fetch, Path}, udon_vnode_master).

rename(Path, NewPath) ->
    Data = fetch(Path),
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
