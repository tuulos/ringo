-module(mochi_dispatch).
-export([request/1]).

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
        {ok, Res} = Mod:op(Script, Req:parse_qs()),
        Req:ok({"text/plain", json:encode(Res)}).

        
        
