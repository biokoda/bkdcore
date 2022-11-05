all: deps compile

.PHONY: rel deps

compile:
	rebar3 compile

recompile:
	rebar3 update-deps
	rebar3 compile

update:
	rebar3 update-deps

deps:
	rebar3 get-deps

clean:
	rebar3 clean
