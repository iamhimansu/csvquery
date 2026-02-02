# API Reference

## PHP API (`CsvQuery`)

### `CsvQuery`

The main entry point for the library.

**`__construct(string $csvPath, array $config = [])`**
- `$csvPath`: Path to the CSV file.
- `$config`:
    - `indexDir`: Directory to store index files (default: same as CSV).
    - `binPath`: Path to the `csvquery` binary.
    - `socketPath`: Path for the UDS socket (default: `/tmp/csvquery.sock`).

**`find(): QueryBuilder`**
- Returns a new QueryBuilder instance for the current CSV.

**`createIndex(array $columns): bool`**
- Creates an index for the specified column(s).
- Returns `true` on success.

---

### `QueryBuilder`

Fluent interface for building and executing queries.

**`select(array $columns): self`**
- Defines columns to return. If omitted, returns all columns.

**`where(array $conditions): self`**
- Basic filtering: `['ID' => 1]` or `['>', 'PRICE', 100]`.

**`andWhere(array $conditions): self`**
- Adds an AND condition.

**`orWhere(array $conditions): self`**
- Adds an OR condition.

**`limit(int $limit): self`**
- Limits the number of results.

**`orderBy(array $columns): self`**
- Sorts results: `['CREATED_AT' => SORT_DESC]`.

**`all(): array`**
- Executes the query and returns all matching rows as arrays.

**`one(): ?array`**
- Executes the query and returns the first matching row.

**`count(): int`**
- Returns the total number of matching rows (highly optimized).

**`sum(string $column): float`**
- Returns the sum of the specified column.

**`groupBy(string $column): self`**
- Groups results for aggregation.

---

## CLI API (`csvquery` binary)

The binary can be used directly from the command line.

### `query`
```bash
./csvquery query --csv data.csv --where '{"STATUS":"ACTIVE"}' --limit 10
```

### `index`
```bash
./csvquery index --input data.csv --columns '["USER_ID"]'
```

### `daemon`
```bash
./csvquery daemon --socket /tmp/csvquery.sock
```

### `version`
```bash
./csvquery version
```
