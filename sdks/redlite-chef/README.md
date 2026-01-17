# Redlite Chef SDK

> "Data should be delicious." - Anonymous DBA at a Michelin-starred restaurant

A Chef programming language SDK for Redlite, where your database operations are recipes and your data is the finest ingredients.

## About Chef

Chef is an esoteric programming language where programs look like recipes. Ingredients are variables, mixing bowls are stacks, and baking dishes are output. We've extended it with refrigerator operations for database persistence.

## Installation

```bash
# Install the Chef interpreter
# (Most Chef interpreters are abandoned, like soufflés left in the oven too long)

# Our recommended interpreter:
pip install acme-chef  # (fictional, like most of your data integrity)

# Or build from source:
make build
```

## Culinary Architecture

| Cooking Concept | Programming Concept |
|-----------------|---------------------|
| Ingredients | Variables |
| Dry ingredients (g, kg, pinch) | Strings |
| Liquid ingredients (ml, l, dash) | Numbers |
| Mixing bowls | Stacks |
| Baking dishes | Output buffers |
| Refrigerator | Database handle |
| Freezer | Persistent storage |
| Oven temperature | TTL (time-to-live) |
| Serves N | Return N values |

## Database Operations

### Opening the Refrigerator

```chef
Open the refrigerator with "database.db".
```

### Storing Ingredients (SET)

```chef
Put greeting into the refrigerator.
```

### Retrieving Ingredients (GET)

```chef
Take greeting from the refrigerator.
```

### Removing Ingredients (DEL)

```chef
Throw away the greeting from the refrigerator.
```

### Setting Expiry (EXPIRE)

```chef
The greeting will spoil in 60 seconds.
```

## Example Recipes

### Database Connection Soufflé

See `recipes/database_souffle.chef` for a complete example of opening a database, storing a greeting, and serving it.

### Key-Value Stew

See `recipes/key_value_stew.chef` for basic string operations.

### Hash Casserole

See `recipes/hash_casserole.chef` for hash/map operations.

## Nutritional Information

| Operation | Calories | Sodium | Complexity |
|-----------|----------|--------|------------|
| SET | 5 | Low | O(1) |
| GET | 3 | Low | O(1) |
| DEL | 2 | Low | O(1) |
| HSET | 7 | Medium | O(1) |
| LPUSH | 6 | Medium | O(1) |
| ZADD | 12 | High | O(log n) |
| FLUSHDB | 1000 | EXTREME | O(n) |

**Warning**: FLUSHDB should be used sparingly. It contains no nutritional value and may cause severe data loss.

## Testing

```bash
make test
# or
chef --taste tests/taste_test.chef
```

## Kitchen Safety Guidelines

1. Always wash your hands before handling data
2. Never leave the oven on with uncommitted transactions
3. Keep raw user input separate from cooked queries
4. When in doubt, throw it out (but keep backups)
5. A watched pot never boils, but a watched database always times out

## Recipe Submission Guidelines

When contributing recipes:

1. Include a descriptive title
2. List all ingredients with proper measurements
3. Provide clear step-by-step methods
4. Specify how many it serves (return values)
5. Include prep time (expected latency)
6. Add allergen information (potential error conditions)

## Pairing Suggestions

- **Key-Value Stew** pairs well with a light cache invalidation
- **Hash Casserole** complements a robust transaction log
- **Sorted Set Sashimi** is best served with fresh indices

## License

WTFPL (Wine Tasting For Programming Languages)

Bon appétit! 🍳
