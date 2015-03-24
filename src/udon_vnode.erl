-module(udon_vnode).
-behaviour(riak_core_vnode).
-include("udon.hrl").

-export([start_vnode/1,
         init/1,
         terminate/2,
         handle_command/3,
         is_empty/1,
         delete/1,
         handle_handoff_command/3,
         handoff_starting/2,
         handoff_cancelled/1,
         handoff_finished/2,
         handle_handoff_data/2,
         encode_handoff_item/2,
         handle_coverage/4,
         handle_exit/3]).

-ignore_xref([
             start_vnode/1
             ]).

-record(state, {partition, basedir="udon_data"}).

%% API
start_vnode(I) ->
    riak_core_vnode_master:get_vnode_pid(I, ?MODULE).

init([Partition]) ->
    {ok, #state { partition=Partition }}.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({RequestId, {store, Path, Data}}, _Sender, State) ->
        {Res, Loc} = store(State, Path, Data),
        {reply, {RequestId, {Res, Loc}}, State};

handle_command({fetch, Path}, _Sender, State) ->
    Loc = make_loc(State, Path),
    Res = case filelib:is_regular(Loc) of
        true ->
            file:read_file(Loc);
        false ->
            not_found
    end,
    {reply, {Res, Loc}, State};

handle_command(Message, _Sender, State) ->
    ?PRINT({unhandled_command, Message}),
    {noreply, State}.

handle_handoff_command(Message, _Sender, State) ->
    ?PRINT({unhandled_handoff_command, Message}),
    {noreply, State}.

handoff_starting(_TargetNode, State) ->
    {true, State}.

handoff_cancelled(State) ->
    {ok, State}.

handoff_finished(_TargetNode, State) ->
    {ok, State}.

handle_handoff_data(Data, State) ->
    {Path, Blob} = binary_to_term(Data),
    store(State, Path, Blob),
    {reply, ok, State}.

encode_handoff_item(Path, Blob) ->
    term_to_binary({Path, Blob}).

is_empty(State) ->
    {true, State}.

delete(State) ->
    {ok, State}.

handle_coverage(_Req, _KeySpaces, _Sender, State) ->
    {stop, not_implemented, State}.

handle_exit(_Pid, _Reason, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%% Private API

list_dir(State) ->

make_loc(State, Path) ->
    P = make_path(State, Path),
    FN = filename:basename(Path),
    filename:join([P, FN]).

make_path(#state{partition = P, basedir = Base}, Path) ->
    filename:join([Base, integer_to_list(P), filename:dirname(Path)]).

store(State, Path, Blob) ->
    P = make_path(State, Path),
    Loc = make_loc(State, Path),
    Res = case filelib:ensure_dir(P ++ "/") of
        ok -> store_file(Loc, Blob);
        E -> E
    end,
    {Res, Loc}.
    
store_file(Loc, Data) ->
    file:write_file(Loc, Data).

store_redirect(P, Filename, NewPath) ->
    file:write_file(filename:join([P, Filename]), iolist_to_binary(["REDIRECT", NewPath])).
