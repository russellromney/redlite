# Redlite Brainfuck SDK Makefile
# We're sorry.

CC = gcc
CFLAGS = -Wall -Wextra -O2
LDFLAGS = -L../../target/release -lredlite_ffi

INTERPRETER = bf_interpreter

.PHONY: all build clean test run help apologize

all: build

build: $(INTERPRETER)
	@echo "Built. We're sorry."

$(INTERPRETER): interpreter.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS)
	@echo "Interpreter compiled. Your journey into madness begins."

clean:
	rm -f $(INTERPRETER)
	@echo "Cleaned. The regret remains."

test: build
	@echo "Running tests... This may take a while."
	@echo "Consider making tea. Or reconsidering your life choices."
	./$(INTERPRETER) examples/hello_world.bf

run: build
	@echo "Usage: ./$(INTERPRETER) <file.bf>"
	@echo "May your loops be finite and your pointers true."

help:
	@echo "Redlite Brainfuck SDK"
	@echo ""
	@echo "Commands:"
	@echo "  make build    - Build the interpreter"
	@echo "  make test     - Run the test suite (bring snacks)"
	@echo "  make run      - Show usage information"
	@echo "  make clean    - Remove build artifacts"
	@echo "  make apologize - We're sorry"
	@echo ""
	@echo "Memory Layout:"
	@echo "  Cells 0-7:    Syscall number"
	@echo "  Cells 8-15:   Key buffer"
	@echo "  Cells 16-255: Value buffer"
	@echo "  Cell 256:     Regret accumulator"

apologize:
	@echo "We're deeply sorry."
	@echo "We don't know what came over us."
	@echo "Please forgive us."
	@sleep 1
	@echo "..."
	@sleep 1
	@echo "But also, it works."
