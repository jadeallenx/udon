-module(udon_wm).

-export([dispatch_table/0
        ]).

dispatch_table() ->
    [
     {['*'], udon_wm_handler, []}
    ].
