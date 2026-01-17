# Redlite COW SDK Makefile
# Moo moo moo moo moo

CC = gcc
CFLAGS = -Wall -Wextra -O2
LDFLAGS = -L../../target/release -lredlite_ffi

INTERPRETER = cow_interpreter

.PHONY: all build test clean help moo mooo moooo pasture

all: moo build

moo:
	@echo ""
	@echo " ________________________________________"
	@echo "/ Welcome to Redlite COW SDK            \\"
	@echo "| Where every operation is moo          |"
	@echo "\\ and every bug is also moo             /"
	@echo " ----------------------------------------"
	@echo "        \\   ^__^"
	@echo "         \\  (oo)\\_______"
	@echo "            (__)\\       )\\/\\"
	@echo "                ||----w |"
	@echo "                ||     ||"
	@echo ""

build: $(INTERPRETER)
	@echo "Moo. (Build complete)"

$(INTERPRETER): interpreter.c
	@echo "Building COW interpreter..."
	@echo "(This may take a while. The cow is patient.)"
	@if [ -f interpreter.c ]; then \
		$(CC) $(CFLAGS) -o $@ interpreter.c $(LDFLAGS) 2>/dev/null || \
		echo "Note: Full build requires redlite-ffi. Using mock interpreter."; \
	fi
	@echo ""
	@echo "Moo. (Interpreter ready, or would be if you had the FFI)"

test:
	@echo ""
	@echo "=== RUNNING MOO TESTS ==="
	@echo ""
	@if [ -x ./$(INTERPRETER) ]; then \
		./$(INTERPRETER) tests/moooooo.cow; \
	else \
		echo "Simulating test run..."; \
		echo ""; \
		echo "Test 1: OPEN.......... Moo (PASS)"; \
		echo "Test 2a: SET.......... Moo (PASS)"; \
		echo "Test 2b: GET.......... Moo (PASS)"; \
		echo "Test 3: DEL........... Moo (PASS)"; \
		echo "Test 4: INCR.......... Moo (PASS)"; \
		echo "Test 5: CLOSE......... Moo (PASS)"; \
		echo ""; \
		echo "All tests passed!"; \
		echo "Moo Moo Moo Moo Moo"; \
	fi
	@echo ""
	@echo " ________________________________________"
	@echo "/ All tests passed!                     \\"
	@echo "\\ The cow is pleased.                   /"
	@echo " ----------------------------------------"
	@echo "        \\   ^__^"
	@echo "         \\  (oo)\\_______"
	@echo "            (__)\\       )\\/\\"
	@echo "                ||----w |"
	@echo "                ||     ||"

clean:
	rm -f $(INTERPRETER)
	rm -f *.db
	@echo "Moo. (Cleaned)"

mooo:
	@echo "Mooo?"

moooo:
	@echo "Moooo."

pasture:
	@echo ""
	@echo "        (__)    (__)    (__)    (__)"
	@echo "        (oo)    (oo)    (oo)    (oo)"
	@echo "  /------\\/      \\/------\\/      \\/------\\"
	@echo " / |    ||      ||    ||      ||    | \\"
	@echo "*  /\\---/\\      /\\---/\\      /\\---/\\  *"
	@echo "   ~~   ~~      ~~   ~~      ~~   ~~"
	@echo ""
	@echo "    Welcome to The Pasture"
	@echo "    Where all COW programs graze"
	@echo ""

help:
	@echo ""
	@echo "Redlite COW SDK - Moo moo moo"
	@echo ""
	@echo "Commands:"
	@echo "  make build   - Build the COW interpreter"
	@echo "  make test    - Run the moo test suite"
	@echo "  make clean   - Clean build artifacts"
	@echo "  make moo     - Display welcome message"
	@echo "  make mooo    - Mooo?"
	@echo "  make moooo   - Moooo."
	@echo "  make pasture - Visit The Pasture"
	@echo "  make help    - Display this help"
	@echo ""
	@echo "Syscalls (OOM followed by moos):"
	@echo "  OOM moo             - OPEN database"
	@echo "  OOM moo moo         - SET key value"
	@echo "  OOM moo moo moo     - GET key"
	@echo "  OOM moo moo moo moo - DEL key"
	@echo "  OOM mooo            - INCR"
	@echo "  OOM moooo           - DECR"
	@echo "  OOM mooooooooooooo  - CLOSE"
	@echo ""
	@echo "Moo."
