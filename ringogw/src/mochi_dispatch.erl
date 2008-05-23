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
        inet:setopts(Req:get(socket), [{nodelay, true}]),
        case string:tokens(P, "/") of
                [Dyn, N, Script] -> serve_dynamic(Req, N, Script);
                [Dyn, N, Script|R] -> serve_dynamic(Req, N, [Script|R]);
                [] -> Req:serve_file("", Doc);
                E -> Req:serve_file(lists:last(E), Doc)
        end,
        % If this is a keep-alive session, this process is likely to serve
        % many requests. By flushing the inbox we make sure that no spurious
        % and unhandled messages accumulate in the process' inbox.
        ringogw_util:flush_inbox().

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
                {json, Res} ->
                        Req:ok({"text/plain", json:encode(Res)});
                
                {data, Res} ->
			[_, Params] = Args,
			Req:ok({proplists:get_value("mime", 
				Params, "application/octet-stream"), Res});

                {chunked, ReplyGen} ->
                        Req:respond({200, [{"Content-type",
                                "application/octet-stream"}], chunked}),
                        ringogw_util:chunked_reply(
                                fun(B) -> Req:send(B) end, ReplyGen)
        end.



