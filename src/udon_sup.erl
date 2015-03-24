-module(udon_sup).

-behaviour(supervisor).

%% API
-export([start_link/0]).

%% Supervisor callbacks
-export([init/1]).

%% ===================================================================
%% API functions
%% ===================================================================

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% ===================================================================
%% Supervisor callbacks
%% ===================================================================

init(_Args) ->
    VMaster = { udon_vnode_master,
                  {riak_core_vnode_master, start_link, [udon_vnode]},
                  permanent, 5000, worker, [riak_core_vnode_master]},

    OpFSMs = {udon_op_fsm_sup,
              {udon_op_fsm_sup, start_link, []},
              permanent, infinity, supervisor, [udon_op_fsm_sup]},

    { ok,
        { {one_for_one, 5, 10},
          [VMaster, OpFSMs]}}.
