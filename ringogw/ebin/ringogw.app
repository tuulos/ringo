{application, ringogw, [
        {description, "Ringogw"},
        {vsn, "1"},
        {modules, [ringogw,
                   scgi,
                   json,
                   scgi_server,
                   handle_ring,
                   trunc_io,
                   handle_domains,
                   handle_chunkstat]},
        {registered, []},
        {applications, [kernel, stdlib]},
        {mod, {ringogw, []}}
]}.

