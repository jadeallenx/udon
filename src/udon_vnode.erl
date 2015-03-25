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
    St = #state{ partition=Partition },
    Base = make_base_path(St),
    %% filelib:ensure_dir/1 you make me so sad. You won't make the parents
    %% unless there's a last element which need not exist and will not be
    %% created. Crazytown, baby.
    case filelib:ensure_dir(Base ++ "/dummy") of
        ok -> {ok, St};
        {error, Reason} -> {error, Reason}
    end.

%% Sample command: respond to a ping
handle_command(ping, _Sender, State) ->
    {reply, {pong, State#state.partition}, State};

handle_command({RequestId, {store, R, Data}}, _Sender, State) ->
        MetaPath = make_metadata_path(State, R),
        NewVersion = case filelib:is_regular(MetaPath) of
                 true -> 
                     OldMD = get_metadata(State, R),
                     OldMD#file.version + 1;
                 false ->
                      1
        end,
        {MetaResult, DataResult, Loc} = store(State, R#file{version=NewVersion}, Data),
        {reply, {RequestId, {MetaResult, DataResult, Loc}}, State};

handle_command({fetch, PHash}, _Sender, State) ->
    MetaPath = make_metadata_path(State, PHash),
    Res = case filelib:is_regular(MetaPath) of
        true ->
            MD = get_metadata(State, PHash),
            get_data(State, MD);
        false ->
            not_found
    end,
    {reply, {Res, filename:join([make_base_path(State), make_filename(PHash)])}, State};

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
    Result = case list_dir(State) of
        [] -> true; 
        {error, _Reason} -> true;
        _ -> false
    end,
    {Result, State}.

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
    case file:list_dir(make_base_path(State)) of
        {ok, Files} -> Files;
        Other -> Other
    end.

get_metadata(State = #state{}, #file{ path_md5 = Hash }) ->
    get_metadata(State, Hash);
get_metadata(State = #state{}, Hash) when is_binary(Hash) ->
    MDPath = make_metadata_path(State, Hash),
    {ok, Data} = file:read_file(MDPath),
    binary_to_term(Data).

get_data(State = #state{}, R = #file{ csum = Csum }) ->
    {ok, Data} = file:read_file(make_versioned_file_path(State, R)),
    case Csum =:= erlang:adler32(Data) of
	true -> {ok, Data};
	false -> {error, file_checksum_differs}
    end.

make_metadata_path(State = #state{}, #file{ path_md5 = Hash }) ->
    make_metadata_path(State, Hash);
make_metadata_path(State = #state{}, Hash) when is_binary(Hash) ->
    filename:join([make_base_path(State), make_metadata_filename(Hash)]).

make_versioned_file_path(State = #state{}, #file{ path_md5 = Hash, version = V} ) ->
    filename:join([make_base_path(State) , make_versioned_filename(Hash, V)]).

make_base_path(#state{partition = P, basedir = Base}) ->
    filename:join([Base, integer_to_list(P)]).

store(State = #state{}, R = #file{ path_md5 = PHash }, Blob) ->
    Base = make_base_path(State),
    Res0 = store_meta_file(make_metadata_path(State, R), R),
    Res1 = store_file(make_versioned_file_path(State, R), Blob),
    {Res0, Res1, filename:join([Base, make_filename(PHash)])}.

make_metadata_filename(Hash) when is_binary(Hash) ->
    make_filename(Hash) ++ ".meta".

make_filename(Hash) when is_binary(Hash) ->
    hexstring(Hash).

make_versioned_filename(Hash, Version) when is_integer(Version) andalso is_binary(Hash) ->
    make_filename(Hash) ++ "." ++ integer_to_list(Version).

store_meta_file(Loc, Rec) ->
    Bin = term_to_binary(Rec),
    store_file(Loc, Bin).
    
store_file(Loc, Data) ->
    file:write_file(Loc, Data).

%store_redirect(P, Filename, NewPath) ->
%    file:write_file(filename:join([P, Filename]), iolist_to_binary(["REDIRECT", NewPath])).

hexstring(<<X:128/big-unsigned-integer>>) ->
    lists:flatten(io_lib:format("~32.16.0b", [X])).
