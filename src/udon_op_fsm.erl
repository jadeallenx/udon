-module(udon_op_fsm).
-behavior(gen_fsm).
-include("udon.hrl").

%% API
-export([start_link/6, op/3, op/4]).

%% Callbacks
-export([init/1, code_change/4, handle_event/3, handle_info/3,
         handle_sync_event/4, terminate/3]).

%% States
-export([prepare/2, execute/2, waiting/2]).

%% req_id: The request id so the caller can verify the response.
%%
%% sender: The pid of the sender so a reply can be made.
%%
%% prelist: The preflist for the data.
%%
%% num_w: The number of successful write replies.
%%
%% op: must be a two item tuple with the command and the params
-record(state, {req_id :: pos_integer(),
                from :: pid(),
                n :: pos_integer(),
                w :: pos_integer(),
                op,
                % key used to calculate the hash
                key,
                accum,
                preflist :: riak_core_apl:preflist2(),
                num_w = 0 :: non_neg_integer()}).

%%%===================================================================
%%% API
%%%===================================================================

start_link(ReqID, From, Op, Key, N, W) ->
    gen_fsm:start_link(?MODULE, [ReqID, From, Op, Key, N, W], []).

op(N, W, Op) ->
    op(N, W, Op, Op).

op(N, W, Op, Key) ->
    ReqID = reqid(),
    udon_op_fsm_sup:start_write_fsm([ReqID, self(), Op, Key, N, W]),
    {ok, ReqID}.

%%%===================================================================
%%% States
%%%===================================================================

%% @doc Initialize the state data.
init([ReqID, From, Op, Key, N, W]) ->
    SD = #state{req_id=ReqID, from=From, n=N, w=W, op=Op, key=Key, accum=[]},
    {ok, prepare, SD, 0}.

%% @doc Prepare the write by calculating the _preference list_.
prepare(timeout, SD0=#state{n=N, key=Key}) ->
    DocIdx = riak_core_util:chash_key(Key),
    Preflist = riak_core_apl:get_apl(DocIdx, N, udon),
    SD = SD0#state{preflist=Preflist},
    {next_state, execute, SD, 0}.

%% @doc Execute the write request and then go into waiting state to
%% verify it has meets consistency requirements.
execute(timeout, SD0=#state{req_id=ReqID, op=Op, preflist=Preflist}) ->
    Command = {ReqID, Op},
    riak_core_vnode_master:command(Preflist, Command, {fsm, undefined, self()},
                                   udon_vnode_master),
    {next_state, waiting, SD0}.

%% @doc Wait for W write reqs to respond.
waiting({ReqID, Resp}, SD0=#state{from=From, num_w=NumW0, w=W, accum=Accum}) ->
    NumW = NumW0 + 1,
    NewAccum = [Resp|Accum],
    SD = SD0#state{num_w=NumW, accum=NewAccum},
    if
        NumW =:= W ->
            From ! {ReqID, NewAccum},
            {stop, normal, SD};
        true -> {next_state, waiting, SD}
    end.

handle_info(Info, _StateName, StateData) ->
    lager:warning("got unexpected info ~p", [Info]),
    {stop,badmsg,StateData}.

handle_event(Event, _StateName, StateData) ->
    lager:warning("got unexpected event ~p", [Event]),
    {stop,badmsg,StateData}.

handle_sync_event(Event, _From, _StateName, StateData) ->
    lager:warning("got unexpected sync event ~p", [Event]),
    {stop,badmsg,StateData}.

code_change(_OldVsn, StateName, State, _Extra) -> {ok, StateName, State}.

terminate(_Reason, _SN, _SD) ->
    ok.

%% Private API

reqid() -> erlang:phash2(erlang:now()).
