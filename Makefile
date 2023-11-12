.PHONY: test

repl:
    # Double-pipe to avoid error when exiting REPL
	@clojure||:

test:
	@clojure -M:test

lint:
	@clojure -M:organise-namespaces
	@clojure -M:lint

scan:
	@clojure -M:clj-watson