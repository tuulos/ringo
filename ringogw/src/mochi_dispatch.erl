-module(mochi_dispatch).
-export([request/1]).

% MAX_RECV_BODY can't be larger than 16M due to a gen_tcp:recv restriction in
% raw mode:
% http://www.erlang.org/pipermail/erlang-questions/2006-September/022907.html

-define(MAX_RECV_BODY, (10 * 1024 * 1024)).

request(Req) ->
        {ok, Dyn} = application:get_env(dynroot),
        {ok, Doc} = application:get_env(docroot),
        P = Req:get(path),
        case string:tokens(P, "/") of
                [Dyn, N, Script|_] -> serve_dynamic(Req, N, Script);
                [] -> Req:serve_file("", Doc);
                E -> Req:serve_file(lists:last(E), Doc)
        end.

serve_dynamic(Req, N, Script) ->
        Mod = list_to_existing_atom("handle_" ++ N),
        {ok, Res} = case Req:get(method) of 
                'GET' -> Mod:op(Script, Req:parse_qs());
                'POST' -> Mod:op(Script, Req:parse_qs(),
                                Req:recv_body(?MAX_RECV_BODY))
        end,
        Req:ok({"text/plain", json:encode(Res)}).

        
        
