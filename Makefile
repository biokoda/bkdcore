all: deps compile

.PHONY: rel deps

compile:
	./rebar compile

recompile:
	./rebar update-deps
	./rebar compile

update:
	./rebar update-deps

deps:
	./rebar get-deps

clean:
	./rebar clean
