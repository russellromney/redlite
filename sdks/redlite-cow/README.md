# Redlite COW SDK

```
 ________________________________________
/ Moo moo moo moo moo moo moo moo moo   \
| moo moo moo moo moo moo moo moo moo   |
| moo moo moo moo moo moo moo moo moo   |
\ moo moo moo. - The Documentation      /
 ----------------------------------------
        \   ^__^
         \  (oo)\_______
            (__)\       )\/\
                ||----w |
                ||     ||
```

## Moo?

COW (Cow Operations Whatever) is an esoteric programming language that uses variations of "moo" to express all operations. We've extended it with the `OOM` instruction for Redlite syscalls.

## Installation

```bash
# Install COW interpreter
# (There are surprisingly few maintained COW interpreters)
make build

# Run a COW program
./cow_interpreter examples/moo_set.cow
```

## Language Reference

### Standard COW Instructions

| Instruction | Description |
|-------------|-------------|
| `moo` | Move pointer forward |
| `MOO` | Execute instruction based on current cell |
| `moO` | Move pointer backward |
| `MoO` | Increment current cell |
| `Moo` | Input/Output (print if cell != 0) |
| `MOo` | Decrement current cell |
| `OOO` | Set current cell to zero |
| `MMM` | Copy previous cell to current |
| `OOM` | **REDLITE SYSCALL** (custom!) |
| `oom` | Print current cell as integer |

### Redlite Syscalls (OOM)

After `OOM`, the next moos determine the operation:

| Pattern | Operation | Description |
|---------|-----------|-------------|
| `OOM moo` | OPEN | Open database |
| `OOM moo moo` | SET | Set key-value |
| `OOM moo moo moo` | GET | Get value by key |
| `OOM moo moo moo moo` | DEL | Delete key |
| `OOM mooo` | INCR | Increment key |
| `OOM moooo` | DECR | Decrement key |
| `OOM mooooo` | EXPIRE | Set expiry |
| `OOM moooooo` | HSET | Hash set |
| `OOM mooooooo` | HGET | Hash get |
| `OOM moooooooo` | LPUSH | List push left |
| `OOM mooooooooo` | RPUSH | List push right |
| `OOM moooooooooo` | SADD | Set add |
| `OOM mooooooooooo` | CLOSE | Close database |

## Memory Layout

```
Cell 0:     Database handle
Cell 1:     Syscall result
Cell 2-10:  Key buffer
Cell 11-50: Value buffer
Cell 51+:   General purpose
```

## Examples

### Hello World (SET and GET)

See `examples/moo_set.cow` for a complete example.

### Basic Operations

**Setting a value:**
```cow
; Set cell 0 to database handle
OOM moo                    ; OPEN database

; Prepare key "A" (ASCII 65)
moo                        ; Move to cell 2
MoO MoO MoO MoO MoO MoO   ; Add 6
MoO MoO MoO MoO MoO MoO   ; Add 6
MoO MoO MoO MoO MoO MoO   ; Add 6
MoO MoO MoO MoO MoO MoO   ; Add 6
MoO MoO MoO MoO MoO MoO   ; Add 6
MoO MoO MoO MoO MoO MoO   ; Add 6
MoO MoO MoO MoO MoO MoO   ; Add 6
MoO MoO MoO MoO MoO MoO   ; Add 6
MoO MoO MoO MoO MoO MoO   ; Add 6
MoO MoO MoO MoO MoO MoO   ; Add 6
MoO MoO MoO MoO MoO       ; = 65 = 'A'

; Prepare value "1" (ASCII 49)
moo moo moo moo moo       ; Move to cell 11
moo moo moo moo moo
moo
MoO MoO MoO MoO MoO MoO   ; Add 6 (x8 = 48)
MoO MoO MoO MoO MoO MoO
MoO MoO MoO MoO MoO MoO
MoO MoO MoO MoO MoO MoO
MoO MoO MoO MoO MoO MoO
MoO MoO MoO MoO MoO MoO
MoO MoO MoO MoO MoO MoO
MoO MoO MoO MoO MoO MoO
MoO                        ; = 49 = '1'

; Execute SET syscall
moO moO moO moO moO        ; Back to cell 0
moO moO moO moO moO
moO
OOM moo moo                ; SET

; Success indicated by cell 1 = 1
moo                        ; Move to cell 1
Moo                        ; Print result
```

## The Moo Philosophy

In COW programming, we believe:

1. **All data is moo.** Whether you're storing a user profile or a session token, at the end of the day, it's all just variations of moo.

2. **Simplicity is key.** With only ~10 instructions, there's no room for over-engineering. Every moo counts.

3. **The cow is patient.** Your program may take hours to run. The cow does not mind. The cow has grass to eat.

4. **Errors are silent.** If something goes wrong, the cow simply stares at you. No stack traces. No error messages. Just... moo.

## Debugging Tips

1. **Count your moos.** Seriously. Count them.

2. **Check your cases.** `moo` and `MOO` are very different.

3. **Draw the tape.** Keep track of where your pointer is.

4. **Accept your fate.** Some bugs in COW cannot be fixed. Only accepted.

## Performance Considerations

```
 ________________________________________
/ O(moo^moo) is considered acceptable   \
\ in the COW community.                  /
 ----------------------------------------
        \   ^__^
         \  (oo)\_______
            (__)\       )\/\
                ||----w |
                ||     ||
```

## Testing

```bash
make test
# Output: Moo. (This means it passed)
# Output: MOO! (This means it failed)
```

## FAQ

**Q: Why?**
A: Moo.

**Q: Is this a joke?**
A: Moo moo moo.

**Q: How do I debug this?**
A: You don't. You rewrite. From scratch. Again.

**Q: Can I use this in production?**
A: Moo? MOO! moo moo MOO MOO moo.

**Q: What does that mean?**
A: Exactly.

## License

MOOSPL (Moo Open Source Public License)

You are free to use, modify, and distribute this software, provided you:
1. Include the original moo
2. Add at least one moo of your own
3. Never explain why

---

```
 ________________________________________
/ Thank you for using Redlite COW SDK.  \
| Your data is safe with us.            |
|                                        |
\ Probably.                              /
 ----------------------------------------
        \   ^__^
         \  (oo)\_______
            (__)\       )\/\
                ||----w |
                ||     ||
```
