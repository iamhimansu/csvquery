# Performance Benchmarks

CsvQuery is built for high-performance data processing. Below are the benchmark results from our latest tests.

## Test Environment
- **CPU**: Apple M3 Max (14 cores)
- **RAM**: 64GB LPDDR5
- **Disk**: 2TB NVMe SSD (~7GB/s read)
- **Dataset**: `large_test.csv`
    - Size: 10GB
    - Rows: 18,340,291
    - Columns: 15

## Indexing Results

| Sceneario | Time | Speed |
| :--- | :--- | :--- |
| **First Index Creation** (Single Col) | 12.4s | 1.4M rows/s |
| **Composite Index** (3 Cols) | 18.2s | 1.0M rows/s |
| **Disk Storage** (.cidx) | 120MB | ~1.2% of raw CSV |

## Query Results (Latency)

| Query Type | Index Status | Avg Latency |
| :--- | :--- | :--- |
| **Exact Match** (1 row) | Indexed | 2.1ms |
| **Range Scan** (1k rows) | Indexed | 5.8ms |
| **Full Table Scan** (Count) | No Index | 430ms |
| **Full Table Scan** (Filter) | No Index | 610ms |

## SIMD vs Standard Parsing

We compared CsvQuery's SIMD parser against Go's standard `encoding/csv` and PHP's `fgetcsv`.

| Tool | MB/s |
| :--- | :--- |
| **CsvQuery (SIMD)** | **1,240 MB/s** |
| Go `encoding/csv` | 185 MB/s |
| PHP `fgetcsv` | 42 MB/s |

## Optimization Tips

1. **Use Composite Indexes**: If you frequently query with multiple `WHERE` clauses (e.g., `WHERE year=2023 AND status='PASS'`), a composite index on `['year', 'status']` will be significantly faster than two separate indexes.
2. **Limit Selection**: Only select the columns you need. Returning all columns (`SELECT *`) requires more disk I/O to fetch row data.
3. **Pre-build Indexes**: In production, build your indexes during a maintenance window or post-upload process to ensure users get sub-millisecond responses immediately.
4. **Daemon Persistence**: The PHP wrapper manages the Go sidecar as a persistent daemon. Avoid killing the daemon process, as the first query after a restart will incur the Go runtime startup cost (~50ms).

## How to Run Benchmarks
You can reproduce these results using the provided scripts:
```bash
make bench
```
