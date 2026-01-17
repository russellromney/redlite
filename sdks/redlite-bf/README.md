# Redlite Brainfuck SDK

> "I'm sorry." - The maintainers

A Brainfuck SDK for Redlite because sometimes you want to suffer.

## Installation

```bash
# You'll need our custom interpreter with redlite syscalls
make build-interpreter
```

## Architecture

The memory tape maps to database operations:

| Cells | Purpose |
|-------|---------|
| 0-7 | Syscall number |
| 8-15 | Key pointer |
| 16-255 | Value buffer |
| 256 | Regret accumulator |

## Syscall Numbers

| Number | Operation |
|--------|-----------|
| 1 | OPEN |
| 2 | CLOSE |
| 32 | SET |
| 33 | GET |
| 34 | DEL |
| 35 | EXISTS |
| 48 | INCR |
| 49 | DECR |

## Example: SET "A" "B"

```brainfuck
++++++++[->++++<]>          Set cell 0 to 32 (SET syscall)
>++++++++[->++++++++<]>+    Cell 1 = 65 ('A' - the key)
>++++++++[->++++++++<]>++   Cell 2 = 66 ('B' - the value)
<<<.                        Execute syscall
```

## Running

```bash
./bf_interpreter redlite.bf    # May take several hours
./bf_interpreter examples/*.bf # Will definitely timeout
```

## Testing

```bash
./bf_interpreter tests/test_strings.bf
# Expected output: nothing (tests passed)
# Or: core dump (tests failed)
```

## FAQ

**Q: Why?**
A: We asked ourselves the same question.

**Q: Is this production ready?**
A: Is anything, really?

**Q: How do I debug this?**
A: You don't. You rewrite it from scratch and hope.

**Q: What's the regret accumulator for?**
A: It increments every time you run the interpreter. When it overflows, the program exits with dignity.

## Contributing

Please don't.

## License

MIT (Madness Is Tolerated)
