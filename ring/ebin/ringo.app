{application, ringo, [
        {description, "Ringo"},
        {vsn, "1"},
        {modules, [ringo_node,
                   ringo_util,
                   ringo_main]},
        {registered, [ringo_node]},
        {applications, [kernel, stdlib]},
        {mod, {ringo_main, []}}
]}.

