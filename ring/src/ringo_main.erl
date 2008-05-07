-module(ringo_main).
-behaviour(supervisor).
-behaviour(application).

-compile([verbose, report_errors, report_warnings, trace, debug_info]).
-define(MAX_R, 10).
-define(MAX_T, 60).

-export([init/1, start/2, stop/1]).

conf(P) ->
        case application:get_env(P) of
                undefined -> error_logger:error_report(
                        [{"Parameter missing", P}]),
                        exit(normal);
                {ok, Val} -> Val
        end.

parse_path([Id|_]) -> Id;
parse_path(_) -> error_logger:error_report(
                {"Invalid ringo_home path", conf(ringo_home)}),
                 exit(normal).

check_nodename(Id) ->
        [T|_] = string:tokens(atom_to_list(node()), "@"),
        E = "ringo-" ++ Id,
        if T =/= E ->
                error_logger:error_report(
                        {"Node name should be", E, "not", T}),
                exit(normal);
        true -> ok
        end.

start(_Type, _Args) ->
        Home = conf(ringo_home),
        Id = parse_path(lists:reverse(string:tokens(Home, "/"))),
        check_nodename(Id),
        supervisor:start_link(ringo_main, [Home, erlang:list_to_integer(Id, 16)]).

init([Home, Id]) -> 
        error_logger:info_report([{"RINGO NODE", Id, "BOOTS"}]),
        bfile:load_driver(),
        {ok, {{one_for_one, ?MAX_R, ?MAX_T},
                 [{ringo_node, {ringo_node, start_link, [Home, Id]},
                        permanent, 10, worker, dynamic}]
        }}.

stop(_State) ->
    ok.

