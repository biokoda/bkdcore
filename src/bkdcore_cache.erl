% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_cache).
-behaviour(gen_server).
-export([register/0,reload/0, print_info/0, start/0, stop/0, init/1, handle_call/3,
		 handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([is_flood/3,ban_ip/2,is_banned/2, lookup_insert/4, clear/0,
	     member/2,lookup/2,store/4,delete/2,modify/5,inc_counter/2]).
-include_lib("kernel/include/file.hrl").
% -define(DEBUG,true).
-include("bkdcore.hrl").

% -compile([{parse_transform, lager_transform}]).


% %%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%
% 				INTERVALS SHOULD BE: 1,5,30,60,180,600,etc
%
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

% IP of user to protect from
% Cmd - type of command -> atom (app specific)
% Interval - minimum time between commands (in seconds)
is_flood(IP,Context,Interval) ->
	% E = gen_server:call(?MODULE, get_ets),
	case ets:member(cachetable,{Context,butil:ip_to_int(IP)}) of
		true ->
			true;
		false ->
			gen_server:cast(?MODULE,{insert, Interval, {{Context,butil:ip_to_int(IP)}, 0}}),
			false
	end.

clear() ->
	gen_server:call(?MODULE,{clear}).

ban_ip(IP,Context) ->
	is_flood(IP,Context,60 * 60).
is_banned(IP,Context) ->
	ets:member(cachetable,{Context,butil:ip_to_int(IP)}).

member(Context,Key) ->
	ets:member(cachetable,{Context,Key}).
lookup_insert(Context,Key,Timeout,Fun) ->
	case lookup(Context,Key) of
		undefined ->
			case distreg:whereis({Context,Key}) of
				undefined ->
					{Pid,_} = spawn_monitor(fun() -> getdata(Context,Key,Timeout,Fun) end),
					receive
						{'DOWN',_Monitor,_,Pid,exists} ->
							lookup_insert(Context,Key,Timeout,Fun);
						{'DOWN',_Monitor,_,Pid,Res} ->
							Res
						after 10000 ->
							exit(Pid,toolong),
							{error,{gen,cache_func_timeout}}
					end;
				Pid ->
					erlang:monitor(process,Pid),
					receive
						{'DOWN',_Monitor,_,Pid,noproc} ->
							lookup_insert(Context,Key,Timeout,Fun);
						{'DOWN',_Monitor,_,Pid,Result} ->
							Result
						after 10000 ->
							exit(Pid,toolong),
							{error,{get,cache_func_timeout}}
					end
			end;
		V ->
			V
	end.
lookup(Context,Key) ->
	case ets:lookup(cachetable,{Context,Key}) of
		[] ->
			undefined;
		[{_,N,_,V}] ->
			case N >= 0 of
				true ->
					% Inc NRead by 1
					catch ets:update_counter(cachetable,{Context,Key},{2,1});
				false ->
					true
			end,
			V;
		[{_,V}] ->
			V
	end.
store(Context,Key,Val,Interval) ->
	 case ets:member(cachetable,{Context,Key}) of
		true ->
			true;
		false ->
			gen_server:cast(?MODULE,{insert, Interval, {{Context,Key}, Val}}),
			false
	end.
delete(Context,Key) ->
	gen_server:cast(?MODULE,{delete, {Context,Key}}).
inc_counter(Context,Key) ->
	catch ets:update_counter(cachetable,{Context,Key},{2,1}).
% Fun/2 modifies existing value in ets.
% Example: fun(SavedValue,[NewValue]) -> [NewValue|SavedValue] end.
modify(Context,Key,Val,Interval,Fun) ->
	gen_server:cast(?MODULE,{modify,Fun,Interval,{{Context,Key},Val}}).


getdata(Context,Key,Timeout,Fun) ->
	case distreg:reg({Context,Key}) of
		ok ->
			ok;
		_ ->
			exit(exists)
	end,
	case Fun of
		{Mod,Func,Arg} ->
			FR = apply(Mod,Func,Arg);
		_ ->
			case erlang:fun_info(Fun,arity) of
				{_,0} ->
					FR = Fun();
				{_,1} ->
					FR = Fun(undefined);
				{_,2} ->
					FR = Fun(undefined,0)
			end
	end,
	case FR of
		undefined ->
			ok;
		_ ->
			gen_server:call(?MODULE,{insert, Timeout, Fun, {{Context,Key}, FR}})
	end,
	exit(FR).


handle_call({insert,_,_,_} = IN,_,P) ->
	{noreply,NP} = handle_cast(IN,P),
	{reply,ok,NP};
handle_call({insert,_,_} = IN,_,P) ->
	{noreply,NP} = handle_cast(IN,P),
	{reply,ok,NP};
handle_call(get_ets, _, E) ->
	{reply, E, E};
handle_call({clear},_,E) ->
	erase(cache_times),
	put(cache_times,butil:ds_new(gb_tree)),
	ets:delete_all_objects(E),
	{reply, ok, E};
handle_call({print_info}, _, Ets) ->
	% {links, L} = erlang:process_info(self(), links),
	% io:format("~p~ncache proc table ~p~n", [get(), ets:tab2list(Ets)]),
	io:format("~p~n", [butil:ds_tolist(butil:ds_val(cache_times, get()))]),
	{reply, ok, Ets};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P}.


handle_cast({insert, Timeout, {Key,Val}}, P) ->
	case ets:member(P, Key) of
		true ->
			true;
		false ->
			instime(Timeout,Key),
			ets:insert(P,{Key,Val})
	end,
	{noreply, P};
handle_cast({insert, Timeout, Fun, {Key,Val}}, P) ->
	instime(Timeout,Key),
	ets:insert(P,{Key,0,Fun,Val}),
	{noreply, P};
handle_cast({modify,Fun,Interval,{Key,Val}}, P) ->
	case ets:lookup(cachetable, Key) of
		[] ->
			handle_cast({insert,Interval,{Key,Val}}, P);
		[{_,N,RefFun,SaveVal}] ->
			ets:insert(cachetable,{Key,N,RefFun,Fun(SaveVal,Val)}),
			{noreply, P};
		[{_,SaveVal}] ->
			ets:insert(cachetable,{Key,Fun(SaveVal,Val)}),
			{noreply, P}
	end;
handle_cast({delete, Key}, Ets) ->
	ets:delete(Ets, Key),
	{noreply, Ets};
handle_cast(_, P) ->
	{noreply, P}.

flat_tm() ->
	{MS,S,_} = os:timestamp(),
	MS*1000000000000 + S*1000000.

instime(Timeout,Key) ->
	% Time = Timeout*1000000+butil:flatnow(),
	Time = Timeout*1000000 + flat_tm(),
	case gb_trees:lookup(Time,get(cache_times)) of
		none ->
			put(cache_times, butil:ds_add(Time,{Key,Timeout},get(cache_times)));
		{value,[_|_] = KVS} ->
			put(cache_times, gb_trees:enter(Time,[{Key,Timeout}|KVS],get(cache_times)));
		{value,{K,Int}} ->
			put(cache_times, gb_trees:enter(Time,[{K,Int},{Key,Timeout}],get(cache_times)))
	end.

prune() ->
	T = get(cache_times),
	case gb_trees:size(T) > 0 of
		true ->
			Now = flat_tm(),
			case gb_trees:smallest(T) of
				{Time,[_|_] = KVS} when Time =< Now ->
					put(cache_times, butil:ds_rem(Time,T)),
					[val_timeout(Key,Interval) || {Key,Interval} <- KVS],
					prune();
				{Time,{Key,Interval}} when Time =< Now ->
					put(cache_times, butil:ds_rem(Time,T)),
					val_timeout(Key,Interval),
					prune();
				_ ->
					ok
			end;
		false ->
			ok
	end.


% {Key,NRead,UpdateFun,Val}
val_timeout(Key,T) ->
	case ets:lookup(cachetable,Key) of
		[{_,_}] ->
			% destroyval(V),
			ets:delete(cachetable,Key);
		[{_,N,Fun,_}] when Fun == undefined; N == 0 ->
			% destroyval(V),
			ets:delete(cachetable,Key);
		[{_,N,Fun,V}] ->
			instime(T,Key),
			case N > 0 of
				true ->
					ets:update_element(cachetable,Key,{2,0});
				false ->
					true
			end,
			spawn(fun() -> updateval(Key,Fun,N,V) end);
		_ ->
			ets:delete(cachetable,Key)
	end.

updateval(Key,Fun,N,V) ->
	put(cachecall,true),
	case Fun of
		{Mod,Func,Arg} ->
			FR = apply(Mod,Func,Arg);
		_ ->
			case erlang:fun_info(Fun,arity) of
				{_,0} ->
					FR = case catch Fun() of X -> X end;
				{_,1} ->
					FR = case catch Fun(V) of X -> X end;
				{_,2} ->
					FR = case catch Fun(V,N) of X -> X end
			end
	end,
	case FR of
		undefined ->
			true;
		{invalid_cachecall,Err} ->
			io:format("Invalid cachecall to util line ~p for key ~p", [Err,Key]);
		{'EXIT',_} ->
			gen_server:cast(?MODULE,{delete, Key});
		% {ok,V} ->
		% 	ets:update_element(cachetable,Key,{4,V});
		NV ->
			ets:update_element(cachetable,Key,{4,NV})
	end.

handle_info({prune}, P) ->
	erlang:send_after(1000,self(),{prune}),
	prune(),
	{noreply, P};
handle_info(_, P) ->
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	erlang:send_after(1000,self(),{prune}),
	put(cache_times,butil:ds_new(gb_tree)),
	{ok, ets:new(cachetable, [named_table,public])}.


register() ->
	supervisor:start_child(bkdweb_sup, {?MODULE, {?MODULE, start, []}, permanent, 100, worker, [?MODULE]}).

reload() ->
	code:purge(?MODULE),
	code:load_file(?MODULE).

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).

stop() ->
	gen_server:call(?MODULE, stop).

print_info() ->
	gen_server:call(?MODULE, {print_info}).
