-module(ringo_main).
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
        Home = conf(ringo_home),
        [Id|_] = lists:reverse(string:tokens(Home, "/")),
        supervisor:start_link(ringo_main, [Id]).

init([Id]) -> 
        error_logger:info_report([{"RINGO NODE", Id, "BOOTS"}]),
        {ok, {{one_for_one, ?MAX_R, ?MAX_T},
                 [{ringo_node, {ringo_node, start_link, [Id]},
                        permanent, 10, worker, dynamic}]
        }}.

stop(_State) ->
    ok.

