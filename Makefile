.PHONY: build test bench format clean help

BINARY_NAME=csvquery
GO_DIR=go
CMD_DIR=$(GO_DIR)/cmd/csvquery

# Build targets
build:
	cd $(GO_DIR) && go build -ldflags="-s -w" -o ../bin/$(BINARY_NAME) ./cmd/csvquery

test:
	cd $(GO_DIR) && go test ./...
	composer test

bench:
	cd $(GO_DIR) && go test -bench=. -benchmem ./...
	php examples/benchmark_basic.php

format:
	cd $(GO_DIR) && go fmt ./...
	vendor/bin/php-cs-fixer fix src/

clean:
	rm -rf bin/
	rm -f $(GO_DIR)/$(BINARY_NAME)
	rm -rf vendor/
	rm -rf go/vendor/

help:
	@echo "Available commands:"
	@echo "  build   : Compile Go library"
	@echo "  test    : Run all tests (Go & PHP)"
	@echo "  bench   : Run benchmarks"
	@echo "  format  : Format Go and PHP code"
	@echo "  clean   : Remove build artifacts"
	@echo "  help    : Show this help message"
