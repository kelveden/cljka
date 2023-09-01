.PHONY: test

repl:
	@clj||:

test:
	@clj -M:test

lint:
	@clj -M:lint||:
