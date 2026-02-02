# Contributing to CsvQuery

First off, thank you for considering contributing to CsvQuery! It's people like you that make CsvQuery such a great tool.

## Getting Started

1. **Fork the repo** on GitHub.
2. **Clone your fork** locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/csvquery.git
   cd csvquery
   ```
3. **Create a feature branch**:
   ```bash
   git checkout -b feat/your-awesome-feature
   ```

## Development Setup

To set up your development environment:

1. **Go Requirements**: Install Go 1.21+
2. **PHP Requirements**: Install PHP 7.4+ and Composer
3. **Initialize**:
   ```bash
   go mod download
   composer install
   ```
4. **Build and Test**:
   ```bash
   make build
   make test
   ```

## Code Standards

### Go Standards
- Follow [Effective Go](https://golang.org/doc/effective_go).
- Run `go fmt` before committing.
- Ensure `go vet` passes.

### PHP Standards
- Follow PSR-12 coding standard.
- Use `declare(strict_types=1);` in all PHP files.

### General Rules
- **Comments**: Explain *WHY* you're doing something, not *WHAT* the code is doing.
- **No Debug Code**: Ensure no `fmt.Println`, `var_dump`, or console logs remain in production code.
- **Function Size**: Keep functions under 50 lines where possible.

## Making Changes

1. **Identify the Component**: Determine which component your change affects (Storage, Parser, Index, Query, Types, or PHP Wrapper).
2. **Write Tests**: Add unit tests for any new functionality.
3. **Run Benchmarks**: If your change affects performance, run `make bench` and include the results in your PR.
4. **Update Docs**: If you change the API or behavior, update the relevant files in `docs/`.

## Submitting a Pull Request

1. **Commit Message**: Use descriptive commit messages (e.g., `feat: add composite index support`).
2. **PR Title**: Use the format `type: description` (e.g., `fix: resolve goroutine leak in parser`).
3. **PR Description**: Explain what changed and why. Mention any related issues.
4. **Checklist**:
   - [ ] All tests pass (`make test`)
   - [ ] No performance regressions (`make bench`)
   - [ ] Code is formatted (`make format`)
   - [ ] Documentation is updated
   - [ ] No debug code remains

## Community

If you have questions, feel free to open a Discussion on GitHub.

---
By contributing, you agree that your contributions will be licensed under its MIT License.
