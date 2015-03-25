-module(udon_wm_post_handler).
-export([
         init/1, 
         allowed_methods/2,
         content_types_accepted/2,
         accept_content/2,
         process_post/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").

init([]) -> {ok, undefined}.
    
allowed_methods(Req, State) ->
    {['PUT', 'POST'], Req, State}.

content_types_accepted(ReqData, State) ->
    CT = case wrq:get_req_header("content-type", ReqData) of
             undefined -> "application/octet-stream";
             X -> X
         end,
    {MT, _Params} = webmachine_util:media_type_to_detail(CT),
    {[{MT, accept_content}], ReqData, State}.

accept_content(ReqData, State) ->
    Path = wrq:get_req_header("x-udon-location", ReqData),
    Blob = wrq:req_body(ReqData),
    case udon:store(Path, Blob) of
        {ok, _} -> {true, wrq:set_resp_body(<<"success">>,ReqData), State};
        Err -> {{error, Err}, ReqData, State}
    end.

process_post(ReqData, State) ->
    accept_content(ReqData, State).
