.PHONY: test

repl:
    # Double-pipe to avoid error when exiting REPL
	@clj||:

test:
	@clj -M:test

lint:
	@clj -M:nsorg
	@clj -M:lint
