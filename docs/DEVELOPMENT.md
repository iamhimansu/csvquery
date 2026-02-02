# Development Guide

This document provides technical details for developers working on the CsvQuery codebase.

## Project Structure

```text
.
├── .github/          # GitHub Actions and templates
├── bin/              # Compiled Go binaries (ignored by git)
├── docs/             # Documentation
├── examples/         # Usage examples
├── go/               # Go Engine Source
│   ├── cmd/          # CLI Entry points
│   └── internal/     # Core components (Storage, Parser, Index, Query)
├── src/              # PHP Wrapper Source
├── tests/            # PHP and Go tests
└── Makefile          # Build automation
```

## Adding a New Feature (Workflow)

1. **Define the Type**: If your feature involves new query parameters or result formats, update the structs in `go/internal/types`.
2. **Implement Logic**: Add the core logic in the appropriate `go/internal/` component.
3. **Expose to CLI**: Add the necessary flags/command logic in `go/cmd/csvquery/main.go`.
4. **Update PHP Wrapper**: Update `QueryBuilder` or `Executor` in `src/` to support the new feature.
5. **Add Example**: Create a script in `examples/` demonstrating the feature.

## Debugging

### Go Engine
You can run the Go engine in "Daemon Mode" with verbose logging:
```bash
cd go
go run cmd/csvquery/main.go daemon --socket /tmp/debug.sock --verbose
```

### PHP Wrapper
The PHP wrapper logs daemon errors to `stderr.log` in the current working directory by default. Check this file if queries are failing.

## Profiling

### Go
Use Go's built-in pprof for performance profiling:
```bash
go test -cpuprofile cpu.prof -memprofile mem.prof ./internal/parser
go tool pprof cpu.prof
```

### PHP
Use Xdebug or Blackfire to profile the PHP side, though the bottleneck is almost always the data processing in Go.

## Code Standards Check
Before submitting a PR, run:
```bash
make format
make test
```
