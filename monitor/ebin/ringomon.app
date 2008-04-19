{application, ringomon, [
        {description, "Ringomon"},
        {vsn, "1"},
        {modules, [ringomon,
                   scgi,
                   json,
                   scgi_server,
                   handle_ring,
                   trunc_io,
                   handle_domains,
                   handle_chunkstat]},
        {registered, []},
        {applications, [kernel, stdlib]},
        {mod, {ringomon, []}}
]}.

