# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.1.0] - 2026-02-02

### Changed
- **Rebranded Package**: Renamed from `iamhimansu/csvquery` to `csvquery/csvquery` for a more professional identity.
- **Updated License**: Updated copyright to "CsvQuery Contributors".
- **Documentation**: Standardized namespaces and improved installation guides.


## [1.0.0] - 2026-02-02

### Added
- Initial public release of CsvQuery.
- **SIMD-Optimized CSV Parsing**: Support for AVX2 and SSE4.2 instructions for lightning-fast scanning.
- **Index Management**: Single and composite column indexing with `.cidx` compressed block storage.
- **Query Engine**: Support for `WHERE` (operators: `=`, `>`, `<`, `>=`, `<=`, `IN`, `LIKE`), `LIMIT`, `ORDER BY`.
- **Aggregations**: Optimized `COUNT`, `SUM`, `DISTINCT`, `MIN`, `MAX`.
- **PHP Wrapper**: Fluent QueryBuilder API with ActiveQuery pattern.
- **Sidecar Architecture**: Go engine handling requests over StdIO pipes.
- **Comprehensive Documentation**: Architectural overview, API reference, and development guides.
- **Examples**: Basic queries, indexed searches, and real-world usage scenarios.

### Performance
- **Throughput**: Up to 1.4M rows/second during indexing.
- **Latency**: Sub-5ms response time for indexed lookups on 10GB+ CSV files.
- **Memory Efficiency**: Low memory footprint even with massive datasets due to `mmap` and streamed responses.

### Testing
- Full unit test coverage for Go engine components.
- PHPUnit tests for the fluent API and UDS bridge.
- Benchmark suite for performance regression tracking.
