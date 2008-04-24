-module(make_boot).
-export([write_scripts/0]).

write_scripts() -> 
        Erts = erlang:system_info(version),
        {value, {kernel, _, Kernel}} = lists:keysearch(kernel, 1,
                application:loaded_applications()),
        {value, {stdlib, _, Stdlib}} = lists:keysearch(stdlib, 1,
                application:loaded_applications()),

        Rel = "{release, {\"Ringogw\", \"1\"}, {erts, \"~s\"}, ["
               "{kernel, \"~s\"}, {stdlib, \"~s\"}, {ringogw, \"1\"}]}.",

        {ok, Fs} = file:open("ringogw.rel", [write]),
        io:format(Fs, Rel, [Erts, Kernel, Stdlib]),
        file:close(Fs),
        
        systools:make_script("ringogw", [local]),
        halt().


