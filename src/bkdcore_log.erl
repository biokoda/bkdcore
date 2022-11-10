% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_log).
-export([conftypes/0,reconfigure/0]).
-export([start_lager/0]).
-export([restart_lager/0]).
-include("bkdcore.hrl").

-compile([{parse_transform, lager_transform}]).

reconfigure() ->
	io:format("reconfiguring lager"),
	case application:get_application(lager) of
		undefined ->
			lager:info("lager | loading..."),
			start_lager(),
			lager:info("lager | loaded .");
		_ ->
			lager:info("lager | running"),
			running
	end.

conftypes() ->
	[{log,fun(A) -> 	% log pairs {name,path}
			[begin
				LogId = util:toatom("log_"++util:tolist(Ln)),
				LogFn = util:tolist(Lp),
				{LogId,LogFn}
			end || {Ln,Lp} <- A]
		end},
	{trace_path,fun(A) -> util:tolist(A) end}].

start_lager() ->
	StartLager = fun() ->
		case util:is_app_running(compiler) of
			false ->
				application:start(compiler);
			_ ->
				ok
		end,
		case util:is_app_running(syntax_tools) of
			false ->
				application:start(syntax_tools);
			_ ->
				ok
		end,
		util:wait_for_app(compiler),
		util:wait_for_app(syntax_tools),
		application:load(lager),
		%?PRT("Lager - setting environment"),
		application:set_env(lager,crash_log, util:tolist(sdq_log_conf:global_log_path())++"global.crash.log"),
		application:set_env(lager, handlers, [
		 	{lager_console_backend, [debug, {lager_default_formatter, [date," ",time," [",severity,"] [ ",{module,"?"}," ] [L: ",{line,"?"},"] | ", message, "\n"]}]},
		 	{lager_file_backend, [
		 		{util:ds_val(log_debug,sdq_log_conf:log()) , debug, 104857600, "$D0", 30},
	        	{util:ds_val(log_error,sdq_log_conf:log()), error, 104857600, "$D0", 30},
	        	{util:ds_val(log_warning,sdq_log_conf:log()), warning, 104857600, "$D0", 30},
	        	{util:ds_val(log_console,sdq_log_conf:log()), info, 104857600, "$D0", 30}
	      ]}
		 ]),		
		%?PRT("Lager - starting"),
		application:start(lager),
		util:wait_for_app(lager),
		install_traces()
	end,
	StartLager().

restart_lager() ->
	install_traces().

install_traces() ->
	lager:clear_all_traces(),
	Types = sdq_log_conf:traces(),
	[begin
		TraceId = util:toatom(util:ds_val(<<"id">>,Tracer)),
		TraceFile = util:tolist(util:ds_val(<<"fn">>,Tracer)),
		trace_file(sdq_log_conf:trace_path()++TraceFile , [{log_type, TraceId}])		
	end || Tracer <- Types].

trace_file(File, Filter) ->
    trace_file(File, Filter, debug).
trace_file(File, Filter, Level) ->
    Trace0 = {Filter, Level, {sdq_trace_backend, File}},
    case lager_util:validate_trace(Trace0) of
        {ok, Trace} ->
            Handlers = gen_event:which_handlers(lager_event),
            %% check if this file backend is already installed
            Res = case lists:member({sdq_trace_backend, File}, Handlers) of
                false ->
                    %% install the handler
                    supervisor:start_child(lager_handler_watcher_sup,
                        %[lager_event, {sdq_trace_backend, File}, {File, debug, 104857600, "$D0", 30}]);
						
						[lager_event, {sdq_trace_backend, File}, {File, none}]);
                _ ->
                    {ok, exists}
            end,
            case Res of
              {ok, _} ->
                %% install the trace.
                {MinLevel, Traces} = lager_mochiglobal:get(loglevel),
                case lists:member(Trace, Traces) of
                  false ->
                    lager_mochiglobal:put(loglevel, {MinLevel, [Trace|Traces]});
                  _ ->
                    ok
                end,
                {ok, Trace};
              {error, _} = E ->
                E
            end;
        Error ->
            Error
    end.
