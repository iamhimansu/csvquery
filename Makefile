.PHONY: build test bench format clean help

BINARY_NAME=csvquery
GO_DIR=.
CMD_DIR=$(GO_DIR)/cmd/csvquery

# Build targets
build:
	go build -ldflags="-s -w" -o bin/$(BINARY_NAME) ./cmd/csvquery

test:
	go test ./...
	# composer test # script not defined yet

bench:
	go test -bench=. -benchmem ./...
	# php examples/benchmark_basic.php # file missing

format:
	go fmt ./...
	# vendor/bin/php-cs-fixer fix src/

clean:
	rm -rf bin/$(BINARY_NAME)
	rm -rf vendor/

help:
	@echo "Available commands:"
	@echo "  build   : Compile Go library"
	@echo "  test    : Run all tests (Go & PHP)"
	@echo "  bench   : Run benchmarks"
	@echo "  format  : Format Go and PHP code"
	@echo "  clean   : Remove build artifacts"
	@echo "  help    : Show this help message"
