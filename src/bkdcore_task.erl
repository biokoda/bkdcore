% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_task).
-behaviour(gen_server).
-export([print_info/0, start/0, stop/0, init/1, handle_call/3,
		 handle_cast/2, handle_info/2, terminate/2, code_change/3]).
-export([add_task/3,rem_task/1,exec/1,exists_task/1]).
% -compile(export_all).

add_task(Time,Key,Fun) ->
	gen_server:cast(?MODULE, {add_task,Time,Key,Fun}).
exists_task(Key) ->
	gen_server:call(?MODULE,{exists,Key}).
rem_task(Key) ->
	gen_server:cast(?MODULE,{rem_task,Key}).
exec(Fun) ->
	gen_server:call(?MODULE,{exec, Fun}).

-record(mnd,{}).

handle_call({exists,Key},_,P) ->
	case get(Key) of
		undefined ->
			{reply,false,P};
		_ ->
			{reply,true,P}
	end;
handle_call({exec,Fun}, _, P) ->
	{reply, catch Fun(), P};
handle_call({print_info}, _, P) ->
	io:format("~p~n~p~n", [P,get()]),
	{reply, ok, P};
handle_call(stop, _, P) ->
	{stop, shutdown, stopped, P};
handle_call(_,_,P) ->
	{reply, ok, P}.

handle_cast({add_task, Time, Key, Fun}, P) ->
	case get(Key) of
		undefined ->
			put(Key,{Time,Fun}),
			case get({interval,Time}) of
				undefined ->
					erlang:send_after(Time,self(),{timeout,Time}),
					put({interval,Time}, [Key]);
				L ->
					put({interval,Time},[Key|L])
			end;
		_ ->
			ok
	end,
	{noreply, P};
handle_cast({rem_task,Key}, P) ->
	case get(Key) of
		{Time,_} ->
			erase(Key),
			put({interval,Time}, lists:delete(Key,get({interval,Time})));
		_ ->
			true
	end,
	{noreply, P};
handle_cast(_, P) ->
	{noreply, P}.

handle_info({timeout,T}, P) ->
	L = get({interval,T}),
	erlang:send_after(T,self(),{timeout,T}),
	case L of
		[] ->
			erase({interval,T});
		_ ->
			[runproc(element(2,get(Key))) || Key <- L]
	end,
	{noreply, P};
handle_info(_, P) ->
	{noreply, P}.

terminate(_, _) ->
	ok.
code_change(_, P, _) ->
	{ok, P}.
init([]) ->
	{ok, #mnd{}}.


runproc(F) when is_function(F) ->
	spawn(F);
runproc({M,F,A}) ->
	spawn(M,F,A).

start() ->
	gen_server:start_link({local, ?MODULE}, ?MODULE, [], []).
stop() ->
	gen_server:call(?MODULE, stop).
print_info() ->
	gen_server:call(?MODULE, {print_info}).
