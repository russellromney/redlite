---
title: Esoteric Languages
description: Redlite SDKs for esoteric programming languages
---

For the adventurous, Redlite includes bindings for several esoteric programming languages.

## Available Languages

### Brainf*ck

```brainfuck
; SET key value (simplified example)
++++++++++[>+++++++>++++++++++>+++<<<-]
>++.>+.+++++++..+++.
```

[Source](https://github.com/russellromney/redlite/tree/main/sdks/redlite-bf)

### Chef

```chef
Redlite Set Recipe.

This recipe sets a key-value pair in the database.

Ingredients.
72 g key
101 g value

Method.
Put key into mixing bowl.
Put value into mixing bowl.
Pour contents of mixing bowl into baking dish.
```

[Source](https://github.com/russellromney/redlite/tree/main/sdks/redlite-chef)

### COW

```cow
MoO moO MoO mOo MOO OOM MMM moO moO
```

[Source](https://github.com/russellromney/redlite/tree/main/sdks/redlite-cow)

### LOLCODE

```lolcode
HAI 1.2
  CAN HAS REDLITE?
  I HAS A db ITZ OPENZ ":memory:"
  db SETZ "key" "value"
  VISIBLE db GETZ "key"
KTHXBYE
```

[Source](https://github.com/russellromney/redlite/tree/main/sdks/redlite-lol)

### Piet

Visual programming language where code is represented as images.

[Source](https://github.com/russellromney/redlite/tree/main/sdks/redlite-piet)

### Shakespeare Programming Language (SPL)

```
The Tragedy of Redlite.

Romeo, a database client.
Juliet, a key-value store.

Act I: Setting Values.
Scene I: Romeo sets a key.

[Enter Romeo and Juliet]

Romeo:
  You are as lovely as the sum of a warm peaceful day and a cat.
```

[Source](https://github.com/russellromney/redlite/tree/main/sdks/redlite-spl)

### Whitespace

Code written entirely in spaces, tabs, and newlines.

[Source](https://github.com/russellromney/redlite/tree/main/sdks/redlite-ws)

## Why?

Because we can. These SDKs demonstrate Redlite's FFI flexibility and provide entertainment value.

## Testing

Each esoteric SDK has a Makefile with test targets where applicable:

```bash
cd sdks/redlite-<language>
make test
```
