-module(mochi_dispatch).
-export([request/1]).

% MAX_RECV_BODY can't be larger than 16M due to a gen_tcp:recv restriction in
% raw mode:
% http://www.erlang.org/pipermail/erlang-questions/2006-September/022907.html

-define(MAX_RECV_BODY, (10 * 1024 * 1024)).

request(Req) ->
        T = now(),
        {ok, Dyn} = application:get_env(dynroot),
        {ok, Doc} = application:get_env(docroot),
        P = Req:get(path),
        case string:tokens(P, "/") of
                [Dyn, N, Script] -> serve_dynamic(Req, N, Script);
                [Dyn, N, Script|R] -> serve_dynamic(Req, N, [Script|R]);
                [] -> Req:serve_file("", Doc);
                E -> Req:serve_file(lists:last(E), Doc)
        end,
        error_logger:info_report({"Request processed in ", 
                                round(timer:now_diff(now(), T))}).

serve_dynamic(Req, N, Script) ->
        Mod = list_to_existing_atom("handle_" ++ N),
        Q = Req:parse_qs(),
        case Req:get(method) of 
                'GET' -> catch_op(Req, Mod, [Script, Q]);
                'POST' -> catch_op(Req, Mod, [Script, Q,
                                Req:recv_body(?MAX_RECV_BODY)])
        end.

catch_op(Req, Mod, Args) ->
        case catch apply(Mod, op, Args) of
                {http_error, Code, Error} ->
                        error_logger:error_report({"HTTP error", Code, Error}),
                        Req:respond({Code, [{"Content-Type", "text/plain"}],
                                        json:encode(Error)});
                {'EXIT', Error} ->
                        error_logger:error_report({"Request failed", Error}),
                        Req:respond({500, [{"Content-Type", "text/plain"}],
                                <<"\"Internal server error\"">>});
                {ok, Res} ->
                        %error_logger:error_report({"OK Reply", Args, Res}),
                        Req:ok({"text/plain", json:encode(Res)})
        end.
