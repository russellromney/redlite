# Redlite Shakespeare SDK

> *"To persist, or not to persist: that is the query."*
> — Hamlet, Act III, Scene I (Database Edition)

A Shakespeare Programming Language SDK for Redlite, for databases of such stuff as dreams are made on.

## Dramatis Personae

In the Shakespeare Programming Language, characters are variables. For our database operations:

| Character | Role | Type |
|-----------|------|------|
| Romeo | The Key | String variable |
| Juliet | The Value | String variable |
| The Database | Keeper of Secrets | Database handle |
| Hamlet | The Query | String variable |
| Ophelia | The Result | String variable |
| Macbeth | The Counter | Integer variable |
| Lady Macbeth | The Increment | Integer variable |

## Installation

```bash
# Install the SPL compiler
git clone https://github.com/drsam94/Spl.git
cd Spl && make

# Perform the plays
make perform
```

## The Language of Data

### Opening the Database (Open thy heart)

```shakespeare
The Database:
  Open thy heart to "test.db"!
```

### Storing Values (Remember thyself)

```shakespeare
Romeo:
  Thou art as fair as the sum of a sunny day and a rose.

Juliet:
  Thou art as lovely as the sum of a flower and a summer's day.

The Database:
  Romeo, thou art the key to my soul.
  Juliet, remember thyself!
```

### Retrieving Values (Recall thy grief)

```shakespeare
The Database:
  Romeo, recall thy grief!

Romeo:
  Am I better than nothing?
  If so, speak thy mind.
```

### Deleting Values (Thou art banished)

```shakespeare
The Database:
  Romeo, thou art banished from my heart!
```

### Checking Existence (Am I not thy beloved?)

```shakespeare
Romeo:
  Am I not thy beloved?

The Database:
  If so, let us proceed to scene II.
  If not, let us return to scene I.
```

## The Plays

### The Tragedy of Data Lost
`plays/the_tragedy_of_dropped_tables.spl`

A cautionary tale of a database administrator who ran FLUSHDB in production.

### A Midsummer Night's Query
`plays/a_midsummer_nights_query.spl`

A whimsical journey through SELECT statements under moonlight.

### Much Ado About Caching
`plays/much_ado_about_caching.spl`

A comedy of cache invalidation and the two hard problems.

### The Taming of the Test
`tests/the_taming_of_the_test.spl`

A comprehensive test suite in five acts.

## Value Calculation

In SPL, values are calculated using nouns and adjectives:

**Positive nouns** (value = 1):
- summer's day, rose, flower, lord, king

**Negative nouns** (value = -1):
- bastard, beggar, codpiece, foul, devil

**Adjectives** multiply by 2:
- beautiful, fair, sweet, lovely, golden

**Examples:**
```
"a cat" = 1
"a fine cat" = 2
"a beautiful fair golden flower" = 2 * 2 * 2 * 1 = 8
"the sum of a rose and a summer's day" = 1 + 1 = 2
```

## Error Handling

```shakespeare
Hamlet:
  Let us proceed to scene V.

[Scene V: The Exception Handler]

Ophelia:
  Thou art as vile as the product of a toad and a codpiece!
  Speak thy mind!

[Output: "ERROR"]
```

## Running the Plays

```bash
# Perform a single play
spl plays/the_tragedy_of_dropped_tables.spl

# Perform with debug output
spl --verbose plays/a_midsummer_nights_query.spl

# Run the test suite
make test
```

## Example: A Complete Scene

```shakespeare
The Tragedy of Data Lost.

A tale of love, loss, and database operations.

Romeo, a young key seeking its value.
Juliet, a fair value awaiting storage.
The Database, a keeper of secrets eternal.

Act I: The Opening of Connections.

Scene I: The Initialization.

[Enter Romeo and The Database]

The Database:
  Open thy heart to "romance.db"!
  Romeo, thou art as fair as the sum of
  the product of a golden summer's day and a rose
  and the square of a lovely flower.

Romeo:
  Speak thy mind.

[Output: ASCII character 72 = 'H']

[Exit Romeo]
[Enter Juliet]

Scene II: The Setting of Values.

The Database:
  Juliet, thou art as sweet as the sum of
  a summer's day and the cube of a rose.

Juliet:
  Remember thyself!

The Database:
  Romeo, recall thy grief!

[Enter Romeo]

Romeo:
  Am I better than nothing?

The Database:
  If so, speak thy mind.

[Exeunt]

Act II: The Tragic Deletion.

Scene I: The Banishment.

[Enter Romeo and The Database]

The Database:
  Romeo, thou art banished from my heart!
  All data is lost, like tears in rain.

Romeo:
  Am I not thy beloved?

The Database:
  You are nothing to me now.

[Exeunt omnes]
```

## Testing

```bash
make test
# or
spl --perform tests/the_taming_of_the_test.spl
```

*Standing ovation expected.*

## License

Public Domain (like Shakespeare's works)

---

*"Parting is such sweet sorrow, that I shall say goodnight till it be morrow."*
— Your data, after FLUSHDB
