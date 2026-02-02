# Getting Started with CsvQuery

This guide will help you get CsvQuery up and running in your project.

## 1. Installation

### Via PHP (Composer)
Add CsvQuery to your PHP project:
```bash
composer require csvquery/csvquery
```

### Build the Go Engine
The PHP wrapper requires a compiled Go binary to handle the heavy lifting.
```bash
# Clone the repository if you haven't already
git clone https://github.com/csvquery/csvquery.git
cd csvquery

# Build the binary
make build
```
The binary will be created in `bin/csvquery`.

## 2. Basic Setup

In your PHP code, initialize the `CsvQuery` object:

```php
use CsvQuery\CsvQuery;

$csv = new CsvQuery('/path/to/your/data.csv', [
    'indexDir' => '/path/to/save/indexes',
    'binPath'  => '/path/to/csvquery_binary'
]);
```

## 3. Creating Indexes

To achieve sub-millisecond query speeds, you must index the columns you plan to search:

```php
// Create a single column index
$csv->createIndex(['USER_ID']);

// Create a composite index for multi-column filters
$csv->createIndex(['YEAR', 'MONTH', 'STATUS']);
```

## 4. Your First Query

Now you can query your CSV like a database:

```php
$rows = $csv->find()
    ->where(['USER_ID' => 12345])
    ->all();

foreach ($rows as $row) {
    echo $row['USER_NAME'];
}
```

## 5. Aggregations

CsvQuery is highly optimized for aggregations:

```php
// Count rows matching a condition
$total = $csv->find()->where(['STATUS' => 'ACTIVE'])->count();

// Sum a column with grouping
$stats = $csv->find()
    ->groupBy('DEPARTMENT')
    ->sum('SALARY');
```

## Common Issues

### "Socket connection failed"
Ensure the Go daemon is allowed to run on your system. The PHP library attempts to start it automatically, but it may require permissions to create a socket in `/tmp`.

### "Index not found"
If you query a column without an index, CsvQuery will perform a full SIMD scan. While fast, it's not as instantaneous as an indexed lookup. For files >1GB, always use indexes.

---
Next: [Check the full API Reference](API.md)
