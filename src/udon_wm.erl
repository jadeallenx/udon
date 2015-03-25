-module(udon_wm).

-export([dispatch_table/0
        ]).

dispatch_table() ->
    [
     % /post, 
     {[post], udon_wm_post_handler, []},
     {['*'], udon_wm_fetch_handler, []}
    ].
