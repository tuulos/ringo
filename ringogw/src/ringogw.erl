-module(ringogw).
-behaviour(supervisor).
-behaviour(application).

-compile([verbose, report_errors, report_warnings, trace, debug_info]).
-define(MAX_R, 10).
-define(MAX_T, 60).

-export([init/1, start/2, stop/1]).

conf(P) ->
        case application:get_env(P) of
                undefined -> exit(["Specify ", P]);
                {ok, Val} -> Val
        end.

start(_Type, _Args) ->
        HttpMode = conf(httpmode),
        Port = conf(port),
        supervisor:start_link(ringogw, [HttpMode, Port]).

init([mochi, Port]) ->
        error_logger:info_report([{"Ringo gateway starts (Mochi)"}]),
        {ok, {{one_for_one, ?MAX_R, ?MAX_T}, [
                 {mochiweb_http, {mochiweb_http, start, [[{port, Port},
                        {loop, {mochi_dispatch, request}}]]},
                        permanent, 10, worker, dynamic}
                ]
        }};

init([scgi, Port]) -> 
        error_logger:info_report([{"Ringo gateway starts (SCGI)"}]),
        {ok, {{one_for_one, ?MAX_R, ?MAX_T}, [
                 {scgi_server, {scgi_server, start_link, [Port]},
                        permanent, 10, worker, dynamic}
                ]
        }}.

stop(_State) ->
    ok.

