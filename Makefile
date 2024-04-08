.PHONY: test repl lint scan ancient

# Open a REPL
repl:
    # Double-pipe to avoid error when exiting REPL
	@clojure -M:repl||:

# Run tests
test:
	@clojure -M:test

# Prettify things
lint:
	@clojure -M:organise-namespaces
	@clojure -M:lint

# Scan dependencies for vulnerabilities
scan:
	@clojure -M:clj-watson

# Check for outdated dependencies
ancient:
	@clojure -M:outdated

upgrade:
	@clojure -M:outdated --upgrade

# Build documentation
codox:
	rm docs -rf
	@clojure -X:codox
