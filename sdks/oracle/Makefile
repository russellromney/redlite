# Oracle Test Makefile
# Cross-SDK consistency testing for Redlite

PYTHON_VENV := ../redlite-python/.venv/bin/python
SPEC_DIR := spec

.PHONY: help test test-python test-ts test-verbose clean

help:
	@echo "Oracle Test Commands:"
	@echo "  make test           - Run all oracle tests (all SDKs)"
	@echo "  make test-python    - Run oracle tests against Python SDK"
	@echo "  make test-ts        - Run oracle tests against TypeScript SDK"
	@echo "  make test-verbose   - Run all tests with verbose output"
	@echo ""
	@echo "Spec files:"
	@ls -1 $(SPEC_DIR)/*.yaml 2>/dev/null || echo "  (none found)"

# Run all SDK tests
test: test-python test-ts
	@echo ""
	@echo "All oracle tests complete!"

# Python SDK tests
test-python:
	@echo "Running Oracle tests against Python SDK..."
	$(PYTHON_VENV) runners/python_runner.py

# Python SDK tests (verbose)
test-python-verbose:
	$(PYTHON_VENV) runners/python_runner.py -v

# TypeScript SDK tests
test-ts:
	@echo "Running Oracle tests against TypeScript SDK..."
	node runners/ts_runner.js

# TypeScript SDK tests (verbose)
test-ts-verbose:
	node runners/ts_runner.js -v

# Verbose output for all
test-verbose: test-python-verbose test-ts-verbose

# Run a single spec file against Python
test-spec-python:
	@if [ -z "$(SPEC)" ]; then echo "Usage: make test-spec-python SPEC=strings.yaml"; exit 1; fi
	$(PYTHON_VENV) runners/python_runner.py $(SPEC_DIR)/$(SPEC)

# Run a single spec file against TypeScript
test-spec-ts:
	@if [ -z "$(SPEC)" ]; then echo "Usage: make test-spec-ts SPEC=strings.yaml"; exit 1; fi
	node runners/ts_runner.js $(SPEC_DIR)/$(SPEC)

clean:
	rm -rf node_modules package-lock.json
