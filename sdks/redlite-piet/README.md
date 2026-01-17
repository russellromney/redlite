# Redlite Piet SDK

> *"Every database operation is a work of art."*
> — Piet Mondrian (probably)

A Piet programming language SDK for Redlite, where your database operations are expressed as abstract art.

## What is Piet?

Piet is an esoteric programming language where programs are bitmap images. The language is named after the Dutch painter Piet Mondrian. Programs look like abstract art, and execution flows based on color transitions.

## Gallery

```
┌─────────────────────────────────────────────────────────────────────┐
│                                                                     │
│   ██████████████████████████████████████████████████████████████   │
│   ██████████████████████████████████████████████████████████████   │
│   ████░░░░░░░░░░░░░░░░░░████░░░░░░░░████████████████████████████   │
│   ████░░░░░░░░░░░░░░░░░░████░░░░░░░░████████████████████████████   │
│   ████░░░░████████░░░░░░████████████████░░░░░░░░░░░░████████████   │
│   ████░░░░████████░░░░░░████████████████░░░░░░░░░░░░████████████   │
│   ████████████████████████████████████████████████████████████████  │
│   ████████████████████████████████████████████████████████████████  │
│                                                                     │
│   "Database Connection" - SET operation, 2024                       │
│   Oil on PNG, 50x50 codels                                          │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

## Installation

```bash
# Install npiet (Piet interpreter)
brew install npiet
# or
apt-get install npiet

# View the gallery
make gallery

# Run a piece of art
npiet art/database_open.png
```

## Color Reference

Piet uses 18 colors plus black and white:

| Color | Light | Normal | Dark |
|-------|-------|--------|------|
| Red | 🟥 | 🔴 | ⬛ |
| Yellow | 🟨 | 🟡 | 🟫 |
| Green | 🟩 | 🟢 | 🌲 |
| Cyan | 🩵 | 🩵 | 🌊 |
| Blue | 🔵 | 🔷 | 🔹 |
| Magenta | 🩷 | 🟣 | 🍇 |

### Color Transitions = Operations

Moving from one color to another performs stack operations:

| Hue Change | Lightness Change | Operation |
|------------|------------------|-----------|
| 0 | +1 | push |
| 0 | +2 | pop |
| 1 | 0 | add |
| 1 | +1 | subtract |
| 1 | +2 | multiply |
| 2 | 0 | divide |
| 2 | +1 | mod |
| 2 | +2 | not |
| 3 | 0 | greater |
| 3 | +1 | pointer |
| 3 | +2 | switch |
| 4 | 0 | duplicate |
| 4 | +1 | roll |
| 4 | +2 | in(number) |
| 5 | 0 | in(char) |
| 5 | +1 | out(number) |
| 5 | +2 | out(char) |

### Redlite Extensions

We extend Piet with special "syscall blocks" - regions that trigger database operations:

| Pattern | Operation |
|---------|-----------|
| 🔴🟢🔵🟡 | OPEN database |
| 🟢🔵🟣🔴 | SET key value |
| 🔵🟣🔴🟢 | GET key |
| 🟣🔴🟢🔵 | DEL key |
| 🟡🟣🔵🟢 | INCR |
| 🟢🟡🟣🔵 | CLOSE |

## The Artworks

### database_open.png
*"The Opening"*

A serene landscape of color blocks that, when executed by the Piet interpreter, opens a database connection. The dominant blues represent the calm of successful initialization, while the red accents symbolize the potential for data manipulation.

### set_value.png
*"Persistence"*

A bold statement in primary colors. The yellow represents the key seeking its purpose, the blue the value finding its home. When these colors meet, data is written.

### get_value.png
*"Retrieval"*

A contemplative piece. The program flows from darkness (no data) to light (data found). The composition speaks to the eternal question: "Does this key exist?"

### hash_operations.gif
*"The Many Becoming One"*

An animated work exploring the relationship between multiple fields and a single hash key. Each frame represents a different HSET operation, culminating in a complete data structure.

## Creating Your Own Art

### Guidelines

1. **Composition**: Consider the flow of execution. The interpreter starts in the top-left and generally moves right/down.

2. **Color Harmony**: Use complementary colors for operations that work together. SET and GET should have related palettes.

3. **White Space**: White blocks are passable but perform no operation. Use them for composition, like negative space in traditional art.

4. **Black Barriers**: Black blocks are impassable. Use them to direct program flow, like the frame of a painting.

5. **Codel Size**: Each "pixel" of logic is called a codel. Larger codels make programs easier to see but files larger.

### Example: A Simple SET

```
The composition flows left to right:

1. [Light Red Block]     - Push key character onto stack
2. [Dark Red Block]      - Push more characters
3. [Yellow Block]        - Transition to value entry
4. [Light Yellow]        - Push value character
5. [Green Block]         - Execute SET syscall
6. [White Space]         - Rest
7. [Blue Block]          - Program termination
```

## Running Artwork

```bash
# Execute a single piece
npiet art/database_open.png

# Execute with debugging (see the beauty unfold)
npiet -v art/set_value.png

# Generate execution trace
npiet -t art/get_value.png > trace.txt
```

## Testing

```bash
make critique
# The art critic (test suite) examines each piece
# Output: "Masterpiece!" or "Needs work..."
```

## Gallery HTML

Open `gallery.html` in a browser to view all artworks with descriptions and execution previews.

## Contributing

When submitting new artworks:

1. Include an artist's statement explaining the piece
2. Provide codel-level documentation
3. Ensure the work compiles (runs without errors)
4. Consider accessibility (describe the colors for those who cannot see them)

## License

Creative Commons - Because code should be art, and art should be free.

---

```
   🔴🟡🟢🔵🟣
  🟡🟢🔵🟣🔴
 🟢🔵🟣🔴🟡
🔵🟣🔴🟡🟢
🟣🔴🟡🟢🔵

"In the dance of colors, data finds its home."
   - The Redlite Piet Gallery
```
