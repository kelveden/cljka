.PHONY: test repl lint scan ancient

# Open a REPL
repl:
    # Double-pipe to avoid error when exiting REPL
	@clojure||:

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

# AOT compile classes
build:
	mkdir -p classes
	@clojure -M:build

# Build documentation
codox:
	rm docs -rf
	@clojure -X:codox
