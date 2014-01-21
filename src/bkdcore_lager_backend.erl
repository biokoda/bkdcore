% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
-module(bkdcore_lager_backend).

%
%   Example config:
%

% {lager, [
%       {colored, true},
%       {handlers, [
%         {lager_console_backend, info},

% --->    {bkdcore_lager_backend, [{file,"log/project-error-combined.log"},{file_prefix, "log/project-error-"}, {level, error}, {traces,[test_trace,demo_trace]}, {date, "$D0"} ]},
% --->    {bkdcore_lager_backend, [{file,"log/project-info-combined.log"},{file_prefix, "log/project-info-"}, {level, info}, {traces,[test_trace,demo_trace]}, {date, "$D0"} ]},

%         {lager_file_backend, [{file, "log/error.log"}, {level, error}, {date, "$D0"}]},
%         {lager_file_backend, [{file, "log/console.log"}, {level, info}, {date, "$D0"}]}
%       ]}
%     ]},

%
%   To log into these files:
%   lager:info([{log_type,test_trace}],"logging info into test_trace file").
%   lager:error([{log_type,test_trace}],"logging error into demo_trace file").
%
%

% -include("../deps/lager/include/lager.hrl").
-include("../include/lager.hrl").

-behaviour(gen_event).

-export([init/1, handle_call/2, handle_event/2, handle_info/2, terminate/2,
        code_change/3]).

-export([config_to_id/1]).

-define(DEFAULT_LOG_LEVEL, info).
-define(DEFAULT_ROTATION_SIZE, 10485760000). % 10 GB
-define(DEFAULT_ROTATION_DATE, "$D0"). %% midnight
-define(DEFAULT_ROTATION_COUNT, 1).
-define(DEFAULT_SYNC_LEVEL, error).
-define(DEFAULT_SYNC_INTERVAL, 1000).
-define(DEFAULT_SYNC_SIZE, 1024*64). %% 64kb
-define(DEFAULT_CHECK_INTERVAL, 1000).

-record(trace, {
        name :: string(),
        type :: atom(),
        fd :: file:io_device(),
        date,
        inode :: integer(),
        flap :: boolean()
    }).

-record(state, {
        name :: string(),
        level :: {'mask', integer()},
        fd :: file:io_device(),
        inode :: integer(),
        flap=false :: boolean(),
        size = 0 :: integer(),
        date,
        count = 10,
        formatter,
        formatter_config,
        sync_on,
        check_interval = ?DEFAULT_CHECK_INTERVAL,
        sync_interval = ?DEFAULT_SYNC_INTERVAL,
        sync_size = ?DEFAULT_SYNC_SIZE,
        last_check = os:timestamp(),
        file_prefix,
        traces = [] :: list()
    }).

-type option() :: {file, string()} | {level, lager:log_level()} |
                  {size, non_neg_integer()} | {date, string()} |
                  {count, non_neg_integer()} | {sync_interval, non_neg_integer()} |
                  {sync_size, non_neg_integer()} | {sync_on, lager:log_level()} |
                  {check_interval, non_neg_integer()} | {formatter, atom()} |
                  {formatter_config, term()}.

-spec init([option(),...]) -> {ok, #state{}} | {error, bad_config}.
init({FileName, LogLevel}) when is_list(FileName), is_atom(LogLevel) ->
    %% backwards compatability hack
    init([{file, FileName}, {level, LogLevel}]);
init({FileName, LogLevel, Size, Date, Count}) when is_list(FileName), is_atom(LogLevel) ->
    %% backwards compatability hack
    init([{file, FileName}, {level, LogLevel}, {size, Size}, {date, Date}, {count, Count}]);
init([{FileName, LogLevel, Size, Date, Count}, {Formatter,FormatterConfig}]) when is_list(FileName), is_atom(LogLevel), is_atom(Formatter) ->
    %% backwards compatability hack
    init([{file, FileName}, {level, LogLevel}, {size, Size}, {date, Date}, {count, Count}, {formatter, Formatter}, {formatter_config, FormatterConfig}]);
init([LogFile,{Formatter}]) ->
    %% backwards compatability hack
    init([LogFile,{Formatter,[]}]);
init([{FileName, LogLevel}, {Formatter,FormatterConfig}]) when is_list(FileName), is_atom(LogLevel), is_atom(Formatter) ->
    %% backwards compatability hack
    init([{file, FileName}, {level, LogLevel}, {formatter, Formatter}, {formatter_config, FormatterConfig}]);
init(LogFileConfig) when is_list(LogFileConfig) ->
    case validate_logfile_proplist(LogFileConfig) of
        false ->
            %% falied to validate config
            {error, {fatal, bad_config}};
        Config ->
            %% probabably a better way to do this, but whatever
            [FilePrefix, Traces, Name, Level, Date, Size, Count, SyncInterval, SyncSize, SyncOn, CheckInterval, Formatter, FormatterConfig] =
              [proplists:get_value(Key, Config) || Key <- [file_prefix, traces, file, level, date, size, count, sync_interval, sync_size, sync_on, check_interval, formatter, formatter_config]],
            schedule_rotation(Name, Date),
            State0 = #state{name=Name, level=Level, size=Size, date=Date, count=Count, formatter=Formatter,
                formatter_config=FormatterConfig, sync_on=SyncOn, sync_interval=SyncInterval, sync_size=SyncSize,
                check_interval=CheckInterval, file_prefix=FilePrefix, traces=[]},
            State1 = case lager_util:open_logfile(Name, {SyncSize, SyncInterval}) of
                        {ok, {FDs, Inodes, _}} ->
                            State0#state{fd=FDs, inode=Inodes};
                        {error, _Reasons} ->
                            %?INT_LOG(error, "Failed to bkdcore trace file ~s with error ~s", [Name, file:format_error(Reasons)]),
                            State0#state{flap=true}                            
                    end,
            Traces1 = [begin
                    TraceName = FilePrefix++butil:tolist(Tr)++".log",
                    _St = case lager_util:open_logfile(TraceName, {SyncSize, SyncInterval}) of
                        {ok, {FD, Inode, _}} ->
                            % State0#state{fd=FD, inode=Inode};
                            ?INT_LOG(info,"Opened trace logfile ~p '~s' with type |~s|",[self(),TraceName,Tr]),                    
                            #trace{name=TraceName,type=Tr,fd=FD,inode=Inode};
                        {error, Reason} ->
                            ?INT_LOG(error, "Failed to bkdcore trace file ~s with error ~s", [Name, file:format_error(Reason)]),
                            % State0#state{flap=true}
                            #trace{name=TraceName,type=Tr,flap=true}
                    end,
                    _St
                end || Tr <- Traces], 
            State = State1#state{traces=Traces1},            
            {ok, State}
    end.

%% @private
handle_call({set_loglevel, Level}, #state{name=Ident} = State) ->
    case validate_loglevel(Level) of
        false ->
            {ok, {error, bad_loglevel}, State};
        Levels ->
            ?INT_LOG(notice, "Changed loglevel of ~s to ~p", [Ident, Level]),
            {ok, ok, State#state{level=Levels}}
    end;
handle_call(get_loglevel, #state{level=Level} = State) ->
    {ok, Level, State};
handle_call(_Request, State) ->
    {ok, ok, State}.

%% @private
handle_event({log, Message},
    #state{name=Name, level=L,formatter=Formatter,formatter_config=FormatConfig} = State) ->    
    LogType = butil:ds_val(log_type,lager_msg:metadata(Message)),
    case LogType of
        undefined ->
            {ok,State};
        _ ->
            % io:format("logging backend ~p ~p ~n",[Message,LogType]),    
            case lager_util:is_loggable(Message,L,{bkdcore_lager_backend, Name}) of
                true ->                    
                    {ok,write(State, lager_msg:timestamp(Message), lager_msg:severity_as_int(Message), LogType, Formatter:format(Message,FormatConfig)) };
                false ->
                    {ok, State}
            end
    end;    
handle_event(_Event, State) ->
    {ok, State}.

%% @private
handle_info({rotate, File}, #state{name=File,count=_Count,date=Date,traces=Traces} = State) ->
    %lager_util:rotate_logfile(File, Count),
    ?INT_LOG(info,"rotating files ~p ~p",[File,Traces]),
    [begin
        file:rename(Tr#trace.name,Tr#trace.name ++ "." ++ dh_date:format("Y_m_d_H_i_s"))
    end || Tr <- Traces],
    file:rename(File,File ++ "." ++ dh_date:format("Y_m_d_H_i_s") ),
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

%% @private convert the config into a gen_event handler ID
config_to_id({Name,_Severity}) when is_list(Name) ->
    {?MODULE, Name};
config_to_id({Name,_Severity,_Size,_Rotation,_Count}) ->
    {?MODULE, Name};
config_to_id([{Name,_Severity,_Size,_Rotation,_Count}, _Format]) ->
    {?MODULE, Name};
config_to_id([{Name,_Severity}, _Format]) when is_list(Name) ->
    {?MODULE, Name};
config_to_id(Config) ->
    case proplists:get_value(file, Config) of
        undefined ->
            erlang:error(no_file);
        File ->
            {?MODULE, File}
    end.


write(#state{name=Name, fd=FD, inode=Inode, flap=Flap, size=RotSize, count=Count, traces=Traces} = State, Timestamp, Level, LogType, Msg) ->
    LastCheck = timer:now_diff(Timestamp, State#state.last_check) div 1000,
    case get_trace(Traces,LogType) of
        undefined ->
            State;
        WritableTrace ->
            case LastCheck >= State#state.check_interval orelse FD == undefined of
                true ->
                    case lager_util:ensure_logfile(WritableTrace#trace.name,WritableTrace#trace.fd,WritableTrace#trace.inode,{State#state.sync_size, State#state.sync_interval}) of
                        {ok,{TrNewFD, TrNewInode,_}} ->
                            % we rotate logfiles alongisde of "main" log file
                            NewTraceInfo = #trace{fd=TrNewFD,inode=TrNewInode},
                            NewTraces = update_trace(WritableTrace#trace.type,NewTraceInfo,Traces,[]),
                            State0 = State#state{traces=NewTraces};
                        {error,_Reason} ->
                            % io:format("error ensuring rotation for trace ~p",[WritableTrace]),
                            State0 = State
                    end,
                    %% need to check for rotation            
                    case lager_util:ensure_logfile(Name, FD, Inode, {State0#state.sync_size, State0#state.sync_interval}) of
                        {ok, {_, _, Size}} when RotSize /= 0, Size > RotSize ->
                            lager_util:rotate_logfile(Name, Count),
                            %% go around the loop again, we'll do another rotation check and hit the next clause here
                            write(State0, Timestamp, Level, LogType, Msg);
                        {ok, {NewFD, NewInode, _}} ->
                            %% update our last check and try again
                            do_write(State0#state{last_check=Timestamp, fd=NewFD, inode=NewInode}, Level, LogType, Msg);
                        {error, Reason} ->
                            case Flap of
                                true ->
                                    State0;
                                _ ->
                                    ?INT_LOG(error, "Failed to reopen log file ~s with error ~s", [Name, file:format_error(Reason)]),
                                    State0#state{flap=true}
                            end
                    end;
                false ->
                    do_write(State, Level, LogType, Msg)
            end
    end.    

do_write(#state{fd=FD, name=Name, flap=Flap, traces=Traces} = State, Level, LogType, Msg) ->
    %% delayed_write doesn't report errors
    case get_trace(Traces,LogType) of
        undefined ->
            State;
        Trace ->
            _ = file:write(FD, unicode:characters_to_binary(Msg)),            
            _R = file:write(Trace#trace.fd,unicode:characters_to_binary(Msg)),
            % io:format("writing to trace ~p ~p~n",[Trace,_R]),
            {mask, SyncLevel} = State#state.sync_on,
            case (Level band SyncLevel) /= 0 of
                true ->
                    %% force a sync on any message that matches the 'sync_on' bitmask
                    Flap2 = case file:datasync(FD) of
                        {error, Reason2} when Flap == false ->
                            ?INT_LOG(error, "Failed to write log message to file ~s: ~s",
                                [Name, file:format_error(Reason2)]),
                            true;
                        ok ->
                            false;
                        _ ->
                            Flap
                    end,
                    State#state{flap=Flap2};
                _ -> 
                    State
            end
    end.
    

% @private
get_trace([],_) ->
    undefined;
get_trace([H|T],LogType) ->
    case H#trace.type of
        LogType ->
            H;
        _ ->
            get_trace(T,LogType)
    end.

update_trace(_,_,[],Out) ->
    Out;
update_trace(LogType,NewTrace,[H|T],L) ->
    case H#trace.type of
        LogType ->
            Hm = H#trace{fd=NewTrace#trace.fd,inode=NewTrace#trace.inode},
            update_trace(LogType,NewTrace,T,[Hm|L]);
        _ ->
            update_trace(LogType,NewTrace,T,[H|L])
    end.


validate_loglevel(Level) ->
    try lager_util:config_to_mask(Level) of
        Levels ->
            Levels
    catch
        _:_ ->
            false
    end.

validate_logfile_proplist(List) ->
    try validate_logfile_proplist(List, []) of
        Res ->
            case proplists:get_value(file, Res) of
                undefined ->
                    ?INT_LOG(error, "Missing required file option", []),
                    false;
                _File ->
                    %% merge with the default options
                    {ok, DefaultRotationDate} = lager_util:parse_rotation_date_spec(?DEFAULT_ROTATION_DATE),
                    lists:keymerge(1, lists:sort(Res), lists:sort([
                            {level, validate_loglevel(?DEFAULT_LOG_LEVEL)}, {date, DefaultRotationDate},
                            {size, ?DEFAULT_ROTATION_SIZE}, {count, ?DEFAULT_ROTATION_COUNT},
                            {sync_on, validate_loglevel(?DEFAULT_SYNC_LEVEL)}, {sync_interval, ?DEFAULT_SYNC_INTERVAL},
                            {sync_size, ?DEFAULT_SYNC_SIZE}, {check_interval, ?DEFAULT_CHECK_INTERVAL},
                            {formatter, lager_default_formatter}, {formatter_config, []}
                        ]))
            end
    catch
        {bad_config, Msg, Value} ->
            ?INT_LOG(error, "~s ~p for file ~p",
                [Msg, Value, proplists:get_value(file, List)]),
            false
    end.

validate_logfile_proplist([], Acc) ->
    Acc;
validate_logfile_proplist([{file, File}|Tail], Acc) ->
    %% is there any reasonable validation we can do here?
    validate_logfile_proplist(Tail, [{file, File}|Acc]);

validate_logfile_proplist([{file_prefix, Fprefix}|Tail], Acc) ->
    validate_logfile_proplist(Tail, [{file_prefix, Fprefix}|Acc]);

validate_logfile_proplist([{traces, Traces}|Tail], Acc) ->
    validate_logfile_proplist(Tail, [{traces, Traces}|Acc]);

validate_logfile_proplist([{level, Level}|Tail], Acc) ->
    case validate_loglevel(Level) of
        false ->
            throw({bad_config, "Invalid loglevel", Level});
        Res ->
            validate_logfile_proplist(Tail, [{level, Res}|Acc])
    end;
validate_logfile_proplist([{size, Size}|Tail], Acc) ->
    case Size of
        S when is_integer(S), S >= 0 ->
            validate_logfile_proplist(Tail, [{size, Size}|Acc]);
        _ ->
            throw({bad_config, "Invalid rotation size", Size})
    end;
validate_logfile_proplist([{count, Count}|Tail], Acc) ->
    case Count of
        C when is_integer(C), C >= 0 ->
            validate_logfile_proplist(Tail, [{count, Count}|Acc]);
        _ ->
            throw({bad_config, "Invalid rotation count", Count})
    end;
validate_logfile_proplist([{date, Date}|Tail], Acc) ->
    case lager_util:parse_rotation_date_spec(Date) of
        {ok, Spec} ->
            validate_logfile_proplist(Tail, [{date, Spec}|Acc]);
        {error, _} when Date == "" ->
            %% legacy config allowed blanks
            validate_logfile_proplist(Tail, Acc);
        {error, _} ->
            throw({bad_config, "Invalid rotation date", Date})
    end;
validate_logfile_proplist([{sync_interval, SyncInt}|Tail], Acc) ->
    case SyncInt of
        Val when is_integer(Val), Val >= 0 ->
            validate_logfile_proplist(Tail, [{sync_interval, Val}|Acc]);
        _ ->
            throw({bad_config, "Invalid sync interval", SyncInt})
    end;
validate_logfile_proplist([{sync_size, SyncSize}|Tail], Acc) ->
    case SyncSize of
        Val when is_integer(Val), Val >= 0 ->
            validate_logfile_proplist(Tail, [{sync_size, Val}|Acc]);
        _ ->
            throw({bad_config, "Invalid sync size", SyncSize})
    end;
validate_logfile_proplist([{check_interval, CheckInt}|Tail], Acc) ->
    case CheckInt of
        Val when is_integer(Val), Val >= 0 ->
            validate_logfile_proplist(Tail, [{check_interval, Val}|Acc]);
        always ->
            validate_logfile_proplist(Tail, [{check_interval, 0}|Acc]);
        _ ->
            throw({bad_config, "Invalid check interval", CheckInt})
    end;
validate_logfile_proplist([{sync_on, Level}|Tail], Acc) ->
    case validate_loglevel(Level) of
        false ->
            throw({bad_config, "Invalid sync on level", Level});
        Res ->
            validate_logfile_proplist(Tail, [{sync_on, Res}|Acc])
    end;
validate_logfile_proplist([{formatter, Fmt}|Tail], Acc) ->
    case is_atom(Fmt) of
        true ->
            validate_logfile_proplist(Tail, [{formatter, Fmt}|Acc]);
        false ->
            throw({bad_config, "Invalid formatter module", Fmt})
    end;
validate_logfile_proplist([{formatter_config, FmtCfg}|Tail], Acc) ->
    case is_list(FmtCfg) of
        true ->
            validate_logfile_proplist(Tail, [{formatter_config, FmtCfg}|Acc]);
        false ->
            throw({bad_config, "Invalid formatter config", FmtCfg})
    end;    
validate_logfile_proplist([Other|_Tail], _Acc) ->
    throw({bad_config, "Invalid option", Other}).

schedule_rotation(_, undefined) ->
    ok;
schedule_rotation(Name, Date) ->
    %io:format("rotating..."),
    erlang:send_after(lager_util:calculate_next_rotation(Date) * 1000, self(), {rotate, Name}),    
    %erlang:send_after(30 * 1000, self(), {rotate, Name}),
    ok.