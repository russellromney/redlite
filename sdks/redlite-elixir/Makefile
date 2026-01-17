.PHONY: all deps compile test clean

all: deps compile

deps:
	mix deps.get

compile:
	mix compile

test:
	mix test

clean:
	mix clean
	rm -rf _build deps priv/native

# Build release
release:
	MIX_ENV=prod mix compile

# Format code
format:
	mix format

# Check formatting
format-check:
	mix format --check-formatted

help:
	@echo "Available targets:"
	@echo "  deps     - Install dependencies"
	@echo "  compile  - Compile the project"
	@echo "  test     - Run tests"
	@echo "  clean    - Clean build artifacts"
	@echo "  release  - Build release"
	@echo "  format   - Format code"
