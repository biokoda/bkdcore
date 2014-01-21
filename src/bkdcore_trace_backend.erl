% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_trace_backend).
-behaviour(gen_event).
-include_lib("lager/include/lager.hrl").
-export([init/1, handle_call/2, handle_event/2, handle_info/2, terminate/2,
        code_change/3]).

-record(state, {
        name :: string(),
        level :: integer(),
        fd :: file:io_device(),
        inode :: integer(),
        flap=false :: boolean(),
        size = 0 :: integer(),
        date,
        count = 10,
        formatter,
        formatter_config
    }).

%% @private
-spec init([{string(), lager:log_level()},...]) -> {ok, #state{}}.
init([LogFile,{Formatter}]) ->
    init([LogFile,{Formatter,[]}]);
init([LogFile,{Formatter,FormatterConfig}]) ->
	%io:format("Tracing backend Installed - LogFile ~p~n Formatter ~p ~nFormatterConfig ~p~n",[LogFile,Formatter,FormatterConfig]),
    case validate_logfile(LogFile) of
        {Name, Level, _Size, _Date, _Count} -> 
            Size = 104857600,
            Date = [{hour,0}],
            Count = 10,
            schedule_rotation(Name, Date),
            State = case lager_util:open_logfile(Name, true) of
                {ok, {FD, Inode, _}} ->
                    #state{name=Name, level=lager_util:level_to_num(Level),
                        fd=FD, inode=Inode, size=Size, date=Date, count=Count, formatter=Formatter, formatter_config=FormatterConfig};
                {error, Reason} ->
                    ?INT_LOG(error, "Failed to open log file ~s with error ~s",
                        [Name, file:format_error(Reason)]),
                    #state{name=Name, level=lager_util:level_to_num(Level),
                        flap=true, size=Size, date=Date, count=Count, formatter=Formatter, formatter_config=FormatterConfig}
            end,
            {ok, State};
        false ->
            ignore
    end;
init(LogFile) ->
    init([LogFile,{lager_default_formatter,[]}]).


%% @private
handle_call({set_loglevel, Level}, #state{name=Ident} = State) ->
    ?INT_LOG(notice, "Changed loglevel of ~s to ~p", [Ident, Level]),
    {ok, ok, State#state{level=lager_util:level_to_num(Level)}};
handle_call(get_loglevel, #state{level=Level} = State) ->
    {ok, Level, State};
handle_call(_Request, State) ->
    {ok, ok, State}.

%% @private
handle_event({log, Message},
    #state{name=Name, level=L,formatter=Formatter,formatter_config=FormatConfig} = State) ->
    case lager_util:is_loggable(Message,L,{sdq_trace_backend, Name}) of
        true ->
            {ok,write(State, lager_msg:severity_as_int(Message), Formatter:format(Message,FormatConfig)) };
        false ->
            {ok, State}
    end;
handle_event(_Event, State) ->
    {ok, State}.

%% @private
handle_info({rotate, File}, #state{name=File,count=Count,date=Date} = State) ->
    lager_util:rotate_logfile(File, Count),
    schedule_rotation(File, Date),
    {ok, State};
handle_info(_Info, State) ->
    {ok, State}.

%% @private
terminate(_Reason, #state{fd=FD}) ->
    %% flush and close any file handles
    _ = file:datasync(FD),
    _ = file:close(FD),
    ok.

%% @private
code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

write(#state{name=Name, fd=FD, inode=Inode, flap=Flap, size=RotSize,
        count=Count} = State, Level, Msg) ->
    case lager_util:ensure_logfile(Name, FD, Inode, true) of
        {ok, {_, _, Size}} when RotSize /= 0, Size > RotSize ->
            lager_util:rotate_logfile(Name, Count),
            write(State, Level, Msg);
        {ok, {NewFD, NewInode, _}} ->
            %% delayed_write doesn't report errors
            _ = file:write(NewFD, Msg),
            case Level of
                _ when Level =< ?ERROR ->
                    %% force a sync on any message at error severity or above
                    Flap2 = case file:datasync(NewFD) of
                        {error, Reason2} when Flap == false ->
                            ?INT_LOG(error, "Failed to write log message to file ~s: ~s",
                                [Name, file:format_error(Reason2)]),
                            true;
                        ok ->
                            false;
                        _ ->
                            Flap
                    end,
                    State#state{fd=NewFD, inode=NewInode, flap=Flap2};
                _ -> 
                    State#state{fd=NewFD, inode=NewInode}
            end;
        {error, Reason} ->
            case Flap of
                true ->
                    State;
                _ ->
                    ?INT_LOG(error, "Failed to reopen log file ~s with error ~s",
                        [Name, file:format_error(Reason)]),
                    State#state{flap=true}
            end
    end.

validate_logfile({Name, Level}) ->
    case lists:member(Level, ?LEVELS) of
        true ->
            {Name, Level, 0, undefined, 0};
        _ ->
            ?INT_LOG(error, "Invalid log level of ~p for ~s.",
                [Level, Name]),
            false
    end;
validate_logfile({Name, Level, Size, Date, Count}) ->
    ValidLevel = (lists:member(Level, ?LEVELS)),
    ValidSize = (is_integer(Size) andalso Size >= 0),
    ValidCount = (is_integer(Count) andalso Count >= 0),
    case {ValidLevel, ValidSize, ValidCount} of
        {false, _, _} ->
            ?INT_LOG(error, "Invalid log level of ~p for ~s.",
                [Level, Name]),
            false;
        {_, false, _} ->
            ?INT_LOG(error, "Invalid rotation size of ~p for ~s.",
                [Size, Name]),
            false;
        {_, _, false} ->
            ?INT_LOG(error, "Invalid rotation count of ~p for ~s.",
                [Count, Name]),
            false;
        {true, true, true} ->
            case lager_util:parse_rotation_date_spec(Date) of
                {ok, Spec} ->
                    {Name, Level, Size, Spec, Count};
                {error, _} when Date == "" ->
                    %% blank ones are fine.
                    {Name, Level, Size, undefined, Count};
                {error, _} ->
                    ?INT_LOG(error, "Invalid rotation date of ~p for ~s.",
                        [Date, Name]),
                    false
            end
    end;
validate_logfile(H) ->
    ?INT_LOG(error, "Invalid log file config ~p.", [H]),
    false.

schedule_rotation(_, undefined) ->
    ok;
schedule_rotation(Name, Date) ->
    erlang:send_after(lager_util:calculate_next_rotation(Date) * 1000, self(), {rotate, Name}),
    ok.