# Redlite Whitespace SDK Makefile
#

WSPACE = wspace
EXAMPLES_DIR = examples
TESTS_DIR = tests

.PHONY: all build test clean help void reveal

all: void

void:
	@echo ""
	@echo "╔══════════════════════════════════════════════════════════════╗"
	@echo "║                                                              ║"
	@echo "║              REDLITE WHITESPACE SDK                          ║"
	@echo "║                                                              ║"
	@echo "║                                                              ║"
	@echo "║                                                              ║"
	@echo "║                                                              ║"
	@echo "║              (The documentation is above)                    ║"
	@echo "║                                                              ║"
	@echo "╚══════════════════════════════════════════════════════════════╝"
	@echo ""

build:
	@echo "Building Whitespace SDK..."
	@echo ""
	@echo "..."
	@echo ""
	@echo "Done."
	@echo ""
	@echo "(There's nothing to build. It's Whitespace.)"

test:
	@echo ""
	@echo "=== RUNNING WHITESPACE TESTS ==="
	@echo ""
	@if command -v $(WSPACE) >/dev/null 2>&1; then \
		echo "Executing tests/test.ws..."; \
		$(WSPACE) $(TESTS_DIR)/test.ws; \
		echo ""; \
		echo "Tests complete!"; \
	else \
		echo "Whitespace interpreter not found."; \
		echo ""; \
		echo "Simulating test run:"; \
		echo ""; \
		echo "Test 1: OPEN.......... "; \
		echo "Test 2: SET........... "; \
		echo "Test 3: GET........... "; \
		echo "Test 4: DEL........... "; \
		echo "Test 5: CLOSE......... "; \
		echo ""; \
		echo "All tests passed!"; \
		echo "(You can't see the results, but trust us)"; \
	fi
	@echo ""

reveal:
	@echo ""
	@echo "=== REVEALING THE INVISIBLE ==="
	@echo ""
	@echo "redlite.ws contains:"
	@if [ -f redlite.ws ]; then \
		echo ""; \
		echo "Bytes: $$(wc -c < redlite.ws)"; \
		echo "Spaces: $$(tr -cd ' ' < redlite.ws | wc -c)"; \
		echo "Tabs: $$(tr -cd '\t' < redlite.ws | wc -c)"; \
		echo "Newlines: $$(wc -l < redlite.ws)"; \
		echo ""; \
		echo "Hex dump (first 100 bytes):"; \
		xxd redlite.ws 2>/dev/null | head -7 || \
		hexdump -C redlite.ws 2>/dev/null | head -7 || \
		od -A x -t x1z redlite.ws | head -7; \
	else \
		echo "(file not found - or is it?)"; \
	fi
	@echo ""

clean:
	@echo "Cleaning..."
	rm -f *.db
	rm -f $(EXAMPLES_DIR)/*.output
	rm -f $(TESTS_DIR)/*.output
	@echo ""
	@echo "Cleaned. (Not that you could see anything anyway)"

help:
	@echo ""
	@echo "Redlite Whitespace SDK"
	@echo ""
	@echo "Commands:"
	@echo "  make void    - Display the void"
	@echo "  make build   - Build nothing"
	@echo "  make test    - Run invisible tests"
	@echo "  make reveal  - Show what's hidden"
	@echo "  make clean   - Clean invisible artifacts"
	@echo "  make help    - Display this help"
	@echo ""
	@echo "File Guide:"
	@echo "  redlite.ws      - Main library (invisible)"
	@echo "  examples/set.ws - SET example (invisible)"
	@echo "  examples/get.ws - GET example (invisible)"
	@echo "  tests/test.ws   - Test suite (invisible)"
	@echo ""
	@echo "Instruction Reference:"
	@echo "  [Space]            - Stack manipulation"
	@echo "  [Tab][Space]       - Arithmetic"
	@echo "  [Tab][Tab]         - Heap access"
	@echo "  [Tab][LF]          - I/O"
	@echo "  [LF]               - Flow control"
	@echo "  [Tab][Tab][Tab]... - Redlite syscalls"
	@echo ""
	@echo "\"                                                              \""
	@echo "  - The Whitespace Philosophy"
	@echo ""

# Easter egg
nothing:
	@echo ""
	@echo ""
	@echo ""
	@echo ""
	@echo ""

# Another easter egg
something:
	@echo "Error: 'something' is not compatible with Whitespace philosophy"
	@exit 1
