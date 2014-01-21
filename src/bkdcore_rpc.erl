% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_rpc).
-behaviour(gen_server).
-include("bkdcore.hrl").
% API
-export([call/2]).
% gen_server
-export([start/0,start/1, stop/1,stop/0, init/1, handle_call/3, 
 		  handle_cast/2, handle_info/2, terminate/2, code_change/3,t/0]).
-export([start_link/4,init/4]).

% RPC between bkdcore nodes
% Large calls are supported. Every call is split into 16kB chunks. 
%  So sending multimegabyte data over RPC is fine and will not block other smaller calls for longer than it takes to send a 16KB chunk.

call(Node,Msg) ->
	case distreg:whereis({bkdcore,Node}) of
		undefined ->
			case start(Node) of
				{error,name_exists} ->
					call(Node,Msg);
				{error,normal} ->
					{error,econnrefused};
				{error,E} ->
					E;
				{ok,Pid} ->
					call(Node,Pid,Msg)
			end;
		Pid ->
			call(Node,Pid,Msg)
	end.
call(Node,Pid,Msg) ->
	case catch gen_server:call(Pid,{call,Msg},infinity) of
		{'EXIT',{noproc,_}} ->
			erlang:yield(),
			call(Node,Msg);
		{'EXIT',{normal,_}} ->
			{error,econnrefused};
		normal ->
			{error,econnrefused};
		X ->
			X
	end.

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
start(Node) ->
	gen_server:start(?MODULE, Node, []).

stop() ->
	gen_server:call(?MODULE, stop).
stop(Node) when is_pid(Node) ->
	gen_server:call(Node, stop);
stop(undefined) ->
	ok;
stop(Node) ->
	stop(distreg:whereis({bkdcore,Node})).


-record(dp,{sock,sendproc,calln = 0,callsininterval = 0, callcount = 0,
			nactive = 0,permanent = false, direction,transport,
			isinit = false}).

handle_call({call,permanent},_,P) ->
	{reply,ok,P#dp{permanent = true}};
handle_call({call,Msg},From,P) ->
	Bin = term_to_binary({From,Msg},[compressed,{minor_version,1}]),
	handle_call({sendbin,Bin},From,P#dp{callcount = P#dp.callcount + 1});
handle_call({sendbin,Bin},_,P) ->
	case Bin of
		<<First:16384/binary,Rem/binary>> ->
			self() ! {continue,P#dp.calln,Rem};
		First ->
			ok
	end,
	Packet = [<<(P#dp.calln):24/unsigned,(byte_size(Bin)):32/unsigned>>,First],
	ok = gen_tcp:send(P#dp.sock,Packet),
	{noreply,P#dp{calln = P#dp.calln + 1,
				callsininterval = P#dp.callsininterval + 1,
				nactive = P#dp.nactive + 1}};
handle_call({print_info}, _, P) ->
	io:format("~p~n", [P]),
	{reply, ok, P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.

handle_cast(decr_callcount,P) ->
	{noreply,P#dp{callcount = P#dp.callcount - 1}};
handle_cast(_, P) ->
	{noreply, P}.

handle_info({tcp,_S,<<Key:40/binary>>},#dp{direction = receiver,isinit = false} = P) ->
	Key = bkdcore:rpccookie(),
	inet:setopts(P#dp.sock,[{active, once}]),
	{noreply,P#dp{isinit = true}};
handle_info({tcp,_,<<Id:24/unsigned,SizeAndBody/binary>>},P) ->
	case get(Id) of
		undefined ->
			Active = P#dp.nactive + 1,
			CallsInInt = P#dp.callsininterval + 1,
			Home = self(),
			<<Size:32/unsigned,Body/binary>> = SizeAndBody,
			case Size == byte_size(Body) of
				true ->
					{ProcPid,_} = spawn_monitor(fun() -> exec(Home,Body) end);
				false ->
					{ProcPid,_} = spawn_monitor(fun() -> exec_gather(Home,Body) end)
			end,
			put(Id,{Size - byte_size(Body),ProcPid}),
			put(ProcPid,Id);
		{SizeRem,Pid} ->
			Active = P#dp.nactive,
			CallsInInt = P#dp.callsininterval,
			case SizeRem - byte_size(SizeAndBody) =< 0 of
				true ->
					Pid ! {done,SizeAndBody};
				false ->
					Pid ! {chunk,SizeAndBody},
					put(Id,{SizeRem-byte_size(SizeAndBody),Pid})
			end
	end,
	inet:setopts(P#dp.sock,[{active, once}]),
	{noreply,P#dp{calln = P#dp.calln + 1, 
					callsininterval = CallsInInt,
					nactive = Active}};
handle_info({continue,N,Bin},P) ->
	case Bin of
		<<First:16384/binary,Rem/binary>> ->
			self() ! {continue,N,Rem};
		First ->
			ok
	end,
	ok = gen_tcp:send(P#dp.sock,[<<N:24/unsigned>>,First]),
	{noreply,P};
handle_info({'DOWN',_Monitor,_,Pid,_Reason},P) ->
	case get(Pid) of
		undefined ->
			{noreply,P};
		Id ->
			erase(Pid),
			erase(Id),
			{noreply,P#dp{nactive = P#dp.nactive - 1}}
	end;
% handle_info({tcp,_,Bin},P) ->
% 	{From,Msg} = binary_to_term(Bin),
% 	gen_server:reply(From,Msg),
% 	inet:setopts(P#dp.sock,[{active, once}]),
% 	{noreply,P#dp{nactive = P#dp.nactive - 1}};
handle_info({tcp_closed,_},#dp{direction = receiver} = P) ->
	[exit(Pid,tcp_closed) || {_Id,{_,Pid}} <- get(), is_pid(Pid)],
	{stop,normal,P};
handle_info({tcp_closed,_},P) ->
	{stop,normal,P};
handle_info(timeout,P) ->
	case P#dp.callsininterval of
		0 when P#dp.nactive == 0, P#dp.permanent == false, P#dp.callcount == 0 ->
			{stop,normal,P};
		_ ->
			garbage_collect(),
			erlang:send_after(5000,self(),timeout),
			{noreply,P#dp{callsininterval = 0}}
	end;
handle_info(_Msg, P) -> 
	io:format("bkdcoreout invalid msg ~p~n",[_Msg]),
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.

% Ranch
start_link(Ref, Socket, Transport, Opts) ->
    proc_lib:start_link(?MODULE, init, [Ref, Socket, Transport, Opts]).

init(Ref, Socket, Transport, _Opts) ->
	ok = proc_lib:init_ack({ok, self()}),
	ok = ranch:accept_ack(Ref),
	ok = Transport:setopts(Socket, [{active, once},{packet,4},{keepalive,true},{send_timeout,10000}]),
	erlang:send_after(5000,self(),timeout),
	gen_server:enter_loop(?MODULE, [], #dp{sock = Socket, transport = Transport}).

init([]) ->
	{ok,#dp{direction = receiver}};
init(Node) ->
	case distreg:reg({bkdcore,Node}) of
		ok ->
			{IP,Port} = bkdcore:node_address(Node),
			case gen_tcp:connect(IP,Port,[{packet,4},{keepalive,true},binary,{active,once},{send_timeout,2000}],2000) of
				{ok,S} ->
					ok = gen_tcp:send(S,bkdcore:rpccookie(Node)),
					erlang:send_after(5000,self(),timeout),
					{ok, #dp{sock = S, direction = sender}};
				_Err ->
					{stop,normal}
			end;
		name_exists ->
			{stop,name_exists}
	end.




exec_gather(Home,Bin) ->
	erlang:monitor(process,Home),
	exec_sum(Home,Bin).
exec_sum(Home,Bin) ->
	receive
		{chunk,C} ->
			exec_sum(Home,<<Bin/binary,C/binary>>);
		{'DOWN',_Monitor,_,Home,_Reason} ->
			ok;
		{done,C} ->
			exec(Home,<<Bin/binary,C/binary>>)
	end.

exec(Home,Msg) ->
	case binary_to_term(Msg) of
		{rpcreply,{From,X}} ->
			gen_server:reply(From,X),
			gen_server:cast(Home,decr_callcount);
		{From,{Mod,Func,Param}} when Mod /= file, Mod /= filelib, Mod /= init, 
									Mod /= io, Mod /= os, Mod /= erlang, Mod /= code ->
			case catch apply(Mod,Func,Param) of
				X ->
					gen_server:call(Home,{sendbin,term_to_binary({rpcreply,{From,X}},[compressed,{minor_version,1}])})
			end;
		{From,ping} ->
			gen_server:call(Home,{sendbin,term_to_binary({rpcreply,{From,pong}},[compressed,{minor_version,1}])});
		{From,_} ->
			gen_server:call(Home,{sendbin,term_to_binary({rpcreply,{From,module_not_alowed}},[compressed,{minor_version,1}])})
	end.









t() ->
	Bin = mkbin(<<>>),
	io:format("Starting ~p ~p~n",[os:timestamp(),byte_size(Bin)]),
	spawn(fun() -> Res = bkdcore:rpc("node3",{erlang,byte_size,[Bin]}),io:format("Bytesize response ~p ~p~n",[Res,os:timestamp()]) end),
	spawn(fun() -> Res = bkdcore:rpc("node3",ping),io:format("Ping response ~p ~p~n",[Res,os:timestamp()]) end).

mkbin(Bin) when byte_size(Bin) > 1024*1024 ->
	Bin;
mkbin(Bin) ->
	mkbin(<<Bin/binary,(butil:flatnow()):64>>).
