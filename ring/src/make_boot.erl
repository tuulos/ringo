-module(make_boot).
-export([write_scripts/0]).

write_scripts() -> 
        Erts = erlang:system_info(version),
        {value, {kernel, _, Kernel}} = lists:keysearch(kernel, 1,
                application:loaded_applications()),
        {value, {stdlib, _, Stdlib}} = lists:keysearch(stdlib, 1,
                application:loaded_applications()),

        Rel = "{release, {\"Ringo\", \"1\"}, {erts, \"~s\"}, ["
               "{kernel, \"~s\"}, {stdlib, \"~s\"}, {ringo, \"1\"}]}.",

        {ok, Fs} = file:open("ringo.rel", [write]),
        io:format(Fs, Rel, [Erts, Kernel, Stdlib]),
        file:close(Fs),
        
        systools:make_script("ringo", [local]),
        halt().


