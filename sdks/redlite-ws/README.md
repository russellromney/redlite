# Redlite Whitespace SDK

> *"                                                                              "*
> — The entire documentation

## What is Whitespace?

Whitespace is an esoteric programming language where only spaces, tabs, and newlines are significant. All other characters are ignored as comments. This means your code is invisible.

Yes, really.

## Installation

```bash
# Install a Whitespace interpreter
# (There are several, none of them make sense)

# Haskell version:
cabal install whitespace

# Or use our interpreter:
make build
```

## Language Reference

### Instruction Encoding

In Whitespace, instructions are encoded as follows:

| Prefix | Description |
|--------|-------------|
| `[Space]` | Stack manipulation |
| `[Tab][Space]` | Arithmetic |
| `[Tab][Tab]` | Heap access |
| `[Tab][LF]` | I/O |
| `[LF]` | Flow control |

Where:
- `[Space]` = the space character (ASCII 32)
- `[Tab]` = the tab character (ASCII 9)
- `[LF]` = the newline character (ASCII 10)

### Redlite Extensions

We add a custom prefix for database operations:

| Instruction | Operation |
|-------------|-----------|
| `[Tab][Tab][Tab][Space]` | OPEN database |
| `[Tab][Tab][Tab][Space][Space]` | SET key value |
| `[Tab][Tab][Tab][Space][Tab]` | GET key |
| `[Tab][Tab][Tab][Tab][Space]` | DEL key |
| `[Tab][Tab][Tab][Tab][Tab]` | INCR |
| `[Tab][Tab][Tab][Tab][Space][Tab]` | CLOSE |

## Example Programs

### Hello World (SET and GET)

The following file contains a complete program:

```


```

*Note: If you see nothing above, the program loaded correctly.*

To reveal the code, select the text or use a hex editor:
- `20` = Space
- `09` = Tab
- `0A` = Newline

### Viewing Source Code

```bash
# See what's really there
xxd examples/set.ws

# Or
cat -A examples/set.ws

# Or
hexdump -C examples/set.ws
```

## File Structure

```
redlite-ws/
├── README.md          # This file (the only readable one)
├── Makefile           # Build system
├── redlite.ws         # Main library (looks empty)
├── examples/
│   ├── set.ws         # SET operation (invisible)
│   └── get.ws         # GET operation (also invisible)
└── tests/
    └── test.ws        # Tests (completely invisible)
```

## How to Read the Code

1. **Select All**: Highlight the entire file. Hidden characters will be selected.

2. **Hex View**: Use `xxd` or a hex editor to see the actual bytes.

3. **Replace Display**:
   ```bash
   sed 's/ /S/g; s/\t/T/g' file.ws
   ```

4. **Trust**: Just trust that it works. It's better this way.

## Development Tips

### Writing Whitespace

```python
# Helper to generate Whitespace code
def ws(s):
    return s.replace('S', ' ').replace('T', '\t').replace('L', '\n')

# Example: Push the number 5 onto the stack
push_5 = ws('SSTST L')  # [Space][Space] = push, [Tab][Space][Tab] = 5, [LF] = end
```

### Debugging

```
Q: How do I debug Whitespace?
A: You don't.

Q: But what if there's a bug?
A: There is no bug. There is only the void.

Q: The void?
A:                                                              .
```

## Git Considerations

**WARNING**: Git diffs for Whitespace files are... interesting.

```diff
$ git diff redlite.ws
diff --git a/redlite.ws b/redlite.ws
index abc123..def456 100644
--- a/redlite.ws
+++ b/redlite.ws
@@ -1 +1 @@
-
+
```

*Completely informative.*

## The Philosophy of Nothing

Whitespace represents the ultimate minimalism in programming. Consider:

- **The Code You Can't See**: Your logic exists in the absence of visible characters.
- **The Meaning in Emptiness**: Every space is intentional. Every tab, deliberate.
- **The Beauty of Nothing**: The most elegant code is the code that isn't there.

```
"In the space between the stars,
 In the pause between the heartbeats,
 In the silence between the words,
 There lies the truth of Whitespace."

 — Ancient Programmer Proverb
```

## Common Errors

| Error | Cause | Solution |
|-------|-------|----------|
| Nothing happens | Could be anything |           |
| Still nothing | Probably correct |           |
| Something happens | Bug in interpreter |           |

## Testing

```bash
make test

# Expected output:
# (nothing visible)
#
# If you see nothing, tests passed!
# If you see something, tests passed ironically!
```

## FAQ

**Q: Is this a real programming language?**
A: Yes. It's Turing complete.

**Q: Why would anyone use this?**
A:

**Q: Can I use this in production?**
A: You could. The question is whether you should.

**Q: How do I explain this to my team?**
A: You don't. Some things are better left unsaid. Like this entire language.

**Q: What's the point?**
A: The point is that there is no point. And that IS the point.

## Contributing

PRs welcome. Please ensure your changes are properly invisible.

When submitting:
1. Make sure your code looks empty
2. Test that nothing visible happens
3. Document nothing
4. ???
5. Profit

## License

Public Domain. You can't copyright nothing.

---

```






```
*^ That was the entire Redlite client. You're welcome.*
