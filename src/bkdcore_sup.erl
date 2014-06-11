% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_sup).
-behavior(supervisor).
-export([start_link/0, init/1]).

start_link() ->
	supervisor:start_link({local, ?MODULE}, ?MODULE, []).


init([]) ->
	case bkdcore:webport() of
		undefined ->
			Mochi = [];
		_ ->
			Mochi = [{bkdcore_mochi,
								{bkdcore_mochi, start, []},
								 permanent,
								 100,
								 worker,
								[bkdcore_mochi]}]
	end,
	case bkdcore:rpcport() of
		undefined ->
			Tcp = [];
		_ ->
		Tcp = [{bkdcore_rpc,
			{bkdcore_rpc, start, []},
			 permanent,
			 100,
			 worker,
			[bkdcore_rpc]}]
	end,
	case application:get_env(bkdcore,usesharedstate) of
		{ok,false} ->
			Shared = [];
		_ ->
			Shared = 
			[{bkdcore_sharedstate,
				{bkdcore_sharedstate, start, []},
				 permanent,
				 100,
				 worker,
				[bkdcore_sharedstate]},
			{bkdcore_idgen,
			{bkdcore_idgen, start, []},
			 permanent,
			 100,
			 worker,
			[bkdcore_idgen]}]
	end,
	{ok, {{one_for_one, 10, 1},
		 [
		{bkdcore_changecheck,
			{bkdcore_changecheck, start, []},
			 permanent,
			 100,
			 worker,
			[bkdcore_changecheck]},
		{bkdcore_task,
			{bkdcore_task, start, []},
				 permanent,
				 100,
				 worker,
				[bkdcore_task]},
		{bkdcore_geoip,
			{bkdcore_geoip, start, []},
				 permanent,
				 100,
				 worker,
				[bkdcore_geoip]},
		{bkdcore_cache,
			{bkdcore_cache, start, []},
				 permanent,
				 100,
				 worker,
				[bkdcore_cache]}
		 ] ++ Shared ++ Mochi ++ Tcp
	}}.

