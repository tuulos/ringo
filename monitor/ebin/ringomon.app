{application, ringomon, [
        {description, "Ringomon"},
        {vsn, "1"},
        {modules, [ringomon,
                   scgi,
                   scgi_server,
                   handle_ring,
                   trunc_io,
                   handle_chunks,
                   handle_chunkstat]},
        {registered, []},
        {applications, [kernel, stdlib]},
        {mod, {ringomon, []}}
]}.

