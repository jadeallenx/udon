-module(udon_wm_handler).
-export([
         init/1, 
         provide_content/2,
         allowed_methods/2,
         content_types_accepted/2,
         content_types_provided/2,
         accept_content/2,
         process_post/2
        ]).

-include_lib("webmachine/include/webmachine.hrl").

-record(state, { ct = []}).

init([]) -> {ok, #state{}}.
    
allowed_methods(Req, State) ->
    %% TODO: Maybe add 'HEAD' and 'DELETE'
    {['GET', 'PUT', 'POST'], Req, State}.

content_types_accepted(ReqData, State) ->
    CT = case wrq:get_req_header("content-type", ReqData) of
             undefined -> "application/octet-stream";
             X -> X
         end,
    {MT, _Params} = webmachine_util:media_type_to_detail(CT),
    {[{MT, accept_content}], ReqData, State}.

content_types_provided(ReqData, State) ->
    CT = webmachine_util:guess_mime(wrq:disp_path(ReqData)),
    {[{CT, provide_content}], ReqData, State#state{ ct = [ CT | State#state.ct ]}}.

accept_content(ReqData, State) ->
    Path = list_to_binary(wrq:disp_path(ReqData)),
    Blob = wrq:req_body(ReqData),
    case udon:store(Path, Blob) of
        {ok, _} -> {true, wrq:set_resp_body(<<"success">>,ReqData), State};
        Err -> {{error, Err}, ReqData, State}
    end.

process_post(ReqData, State) ->
    accept_content(ReqData, State).

provide_content(ReqData, State) ->
    Path = list_to_binary(wrq:disp_path(ReqData)),
    Response = case udon:fetch(Path) of
        {ok, Data} -> {Data, ReqData, State};
        {redirect, NewPath} -> 
            NewReq0 = wrq:do_redirect(true, ReqData),
            {<<"">>, 
                wrq:set_resp_header("location", binary_to_list(NewPath), NewReq0),
                State};
        Error -> {Error, ReqData, State}
    end,
    Response.
