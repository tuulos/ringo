%%%
%%% File    : bfile.erl
%%% Author  : Claes Wikstrom <klacke@kaja.klacke.net>
%%% Purpose : Interface to stdio buffered FILE io 
%%% Created : 22 Nov 1999 by Claes Wikstrom <klacke@kaja.klacke.net>
%%%----------------------------------------------------------------------

-module(bfile).
-vsn("$Revision: 1.1 $ ").
-author('klacke@kaja.klacke.net').

-export([
	 load_driver/0,
	 fopen/2,
	 fclose/1,
	 fread/2,
	 fwrite/2,
	 feof/1,
	 ferror/1,
	 set_linebuf_size/2,
	 fseek/3,
	 ftell/1,
	 ftruncate/1,
	 fflush/1,
	 frewind/1,
	 fgetc/1,
	 fungetc/2,
	 fgets/1,
	 gets/1,
	 pwrite/3,
	 pread/3
	]).


%% Opcodes
-define(OPEN,             $o).
-define(CLOSE,            $c).
-define(READ,             $r).
-define(WRITE,            $w).
-define(SEEK,             $s).
-define(TELL,             $t).
-define(TRUNCATE,         $T).
-define(FLUSH,            $f).
-define(OEOF,             $e).
-define(ERROR,            $E).
-define(GETC,             $g).
-define(GETS,             $G).
-define(GETS2,            $2).
-define(SET_LINEBUF_SIZE, $S).
-define(UNGETC,           $u).


%% ret codes
-define(VALUE, $v).
-define(FLINE,  $L).
-define(OK,    $o).
-define(I32,   $O).
-define(NOLINE,$N).
-define(FERROR, $E).
-define(REOF,   $x).

-define(int32(X), 
        [((X) bsr 24) band 16#ff, ((X) bsr 16) band 16#ff,
         ((X) bsr 8) band 16#ff, (X) band 16#ff]).
%% Bytes to unsigned
-define(u32(X3,X2,X1,X0), 
        (((X3) bsl 24) bor ((X2) bsl 16) bor ((X1) bsl 8) bor (X0))).
%% Bytes to signed
-define(i32(X3,X2,X1,X0),
        (?u32(X3,X2,X1,X0) - 
         (if (X3) > 127 -> 16#100000000; true -> 0 end))).

load_driver() ->
    Dir = filename:join([filename:dirname(code:which(bfile)),"..", "priv"]),
    erl_ddll:load_driver(Dir, "FILE_drv").


%% Flags = "r" | "w" | ... etc, see fopen(3)
%% Ret: {ok, Fd} | {error, Reason}

fopen(Fname, Flags) ->  
    P = open_port({spawn, 'FILE_drv'}, [binary]),
    erlang_port_command(P, [?OPEN, Fname, [0], Flags, [0]]),
    case recp(P) of
	ok ->
	    {ok, {bfile, P}};
	Err ->
	    unlink(P),
	    exit(P, die),
	    Err
    end.

%% void()
fclose({bfile, Fd}) ->
    unlink(Fd),
    catch erlang:port_close(Fd).

%% {ok, #Bin} | {error, Reason} | eof
fread({bfile, Fd}, Sz) ->
    erlang_port_command(Fd, [?READ|?int32(Sz)]),
    recp(Fd).

%% ok | {error, Reason}
fwrite({bfile, Fd}, IoList) ->
    erlang_port_command(Fd, [?WRITE , IoList]),
    recp(Fd).
	      
%% ok | {error, Reason}
pwrite(BFd, Pos, IoList) ->
    case fseek(BFd, Pos, seek_set) of
	ok ->
	    fwrite(BFd, IoList);
	Error ->
	    Error
    end.
    
%% {ok, #Bin} | {error, Reason} | eof
pread(BFd, Pos, Sz) ->
    case fseek(BFd, Pos, seek_set) of
	ok ->
	    fread(BFd, Sz);
	Error ->
	    Error
    end.
    
%% bool
feof({bfile, Fd}) ->
    erlang_port_command(Fd, [?OEOF]),
    bool(recp(Fd)).

%% bool
ferror({bfile, Fd}) ->
    erlang_port_command(Fd, [?ERROR]),
    bool(recp(Fd)).


%% void()
set_linebuf_size({bfile, Fd}, Sz) ->
    erlang_port_command(Fd, [?SET_LINEBUF_SIZE|?int32(Sz)]),
    recp(Fd).
    
%% Whence  == seek_set | seek_cur || seek_end
%% ok | {error, Reason}
fseek({bfile, Fd}, Offs, Whence) ->
    erlang_port_command(Fd, [?SEEK, ?int32(Offs), whence_enc(Whence)]),
    recp(Fd).

%% {ok, Int} | {error, Reason}
ftell({bfile, Fd}) ->
    erlang_port_command(Fd, [?TELL]),
    recp(Fd).

%% ok | {error, Reason}
ftruncate({bfile, Fd}) ->
    erlang_port_command(Fd, [?TRUNCATE]),
    recp(Fd).

%% ok | {error, Reason}
fflush({bfile, Fd}) ->
    erlang_port_command(Fd, [?FLUSH]),
    recp(Fd).

%% ok | {error, Reason}
frewind(BFd) ->
    fseek(BFd, 0, seek_set).

%% {ok, Char} | {error, Reason} | eof
fgetc({bfile, Fd}) ->
    erlang_port_command(Fd, [?GETC]),
    recp(Fd).

%% ok | {error, Reason}
fungetc({bfile, Fd}, Char) ->
    erlang_port_command(Fd, [?UNGETC, Char]),
    recp(Fd).

%% {line, #Bin} | {noline, #Bin} | {error, Reason} | eof
%% including newline
fgets({bfile, Fd}) ->
    erlang_port_command(Fd, [?GETS]),
    recp(Fd).
    
%% {line, #Bin} | {noline, #Bin} | {error, Reason} | eof
%% not including newline
gets({bfile, Fd}) ->
    erlang_port_command(Fd, [?GETS2]),
    recp(Fd).
    

whence_enc(seek_set) ->
    1;
whence_enc(seek_cur) ->
    2;
whence_enc(seek_end) ->
    3.


bool({ok, 1}) ->
    true;
bool({ok, 0}) ->
    false.


recp(P) when port(P) ->
    receive
	{P, {data, [?VALUE|Bin]}} ->
	    {ok, Bin};
	{P, {data, [?FLINE|Bin]}} ->
	    {line, Bin};
	{P, {data, [?OK|_]}} ->
	    ok;
	{P, {data, [?I32|Bin]}} ->
	    [X1,X2,X3,X4] = binary_to_list(Bin),
	    {ok, ?i32(X1, X2, X3, X4)};
	{P, {data, [?NOLINE|Bin]}} ->
	    {noline, Bin};
	{P, {data, [?FERROR|Err]}} ->
	    {error, list_to_atom(Err)};
	{P, {data, [?REOF|_]}} ->
	    eof
    end;


recp(_PP) ->
    {error, badarg}.

erlang_port_command(P, C) ->
    erlang:port_command(P, C).

