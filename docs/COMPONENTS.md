# System Components

CsvQuery is divided into six logical components. If you are a developer looking to contribute, this guide will help you find where different features are implemented.

## 1. Storage Component (`go/internal/storage`)
- **Responsibility**: All interactions with the filesystem.
- **Key Files**: `reader.go`, `writer.go`.
- **Functionality**:
    - `MMAP` implementation for zero-copy reading.
    - Safe concurrent writes for index files.
    - Handling large file seeking beyond 4GB.

## 2. Parser Component (`go/internal/parser`)
- **Responsibility**: Turning raw bytes from a CSV into structured rows.
- **Key Files**: `simd_parser.go`, `field_extractor.go`.
- **Functionality**:
    - **SIMD (AVX2/SSE)** scanning logic.
    - Quoted field handling (parity bit counting).
    - Multi-character delimiter support.

## 3. Index Component (`go/internal/index`)
- **Responsibility**: Fast lookup mechanisms.
- **Key Files**: `manager.go`, `hash_index.go`, `composite.go`.
- **Functionality**:
    - Loading/Saving `.cidx` files.
    - **Score-based Selection**: Automatically picking the best index for a query.
    - Composite key generation.

## 4. Query Component (`go/internal/query`)
- **Responsibility**: Query execution and planning.
- **Key Files**: `executor.go`, `filter.go`, `aggregator.go`.
- **Functionality**:
    - Evaluating `WHERE` conditions (`>`, `<`, `=`, `IN`, `LIKE`).
    - Implementing aggregations (`SUM`, `COUNT`, `DISTINCT`, `MIN`, `MAX`).
    - Query optimization (joining multiple filter results).

## 5. Types Component (`go/internal/types`)
- **Responsibility**: Shared data structures.
- **Key Files**: `row.go`, `query.go`, `result.go`.
- **Functionality**:
    - Defining the JSON schema for UDS communication.
    - Base interfaces for different engine parts.

## 6. PHP Wrapper (`src/`)
- **Responsibility**: The developer-facing API.
- **Key Files**: `CsvQuery.php`, `QueryBuilder.php`, `Executor.php`.
- **Functionality**:
    - Fluent API implementation.
    - UDS client and Daemon management.
    - Hydrating raw results into PHP objects/arrays.

---
Next: [Full API Reference](API.md)
