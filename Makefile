# Nebula Makefile
.PHONY: all build test lint fmt clean run help install-tools coverage bench docs-check

# Variables
BINARY_NAME=nebula
BINARY_PATH=bin/$(BINARY_NAME)
GO_FILES=$(shell find . -name '*.go' -type f)
COVERAGE_FILE=coverage.out
COVERAGE_HTML=coverage.html

# Default target
all: fmt lint test build

# Build the application
build:
	@echo "Building $(BINARY_NAME)..."
	@go build -o $(BINARY_PATH) ./cmd/nebula/
	@echo "Build complete: $(BINARY_PATH)"

# Run tests
test:
	@echo "Running tests..."
	@go test -v -race ./...

# Run tests with coverage
coverage:
	@echo "Running tests with coverage..."
	@go test -v -race -coverprofile=$(COVERAGE_FILE) -covermode=atomic ./...
	@go tool cover -html=$(COVERAGE_FILE) -o $(COVERAGE_HTML)
	@echo "Coverage report generated: $(COVERAGE_HTML)"

# Run benchmarks
bench:
	@echo "Running benchmarks..."
	@./scripts/benchmark.sh

# Establish performance baseline
baseline:
	@echo "Establishing performance baseline..."
	@./scripts/establish-baseline.sh

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...
	@$(shell go env GOPATH)/bin/gofumpt -w .

# Lint code
lint:
	@echo "Linting code..."
	@$(shell go env GOPATH)/bin/golangci-lint run --timeout=10m ./...

# Check documentation coverage
docs-check:
	@echo "Checking documentation coverage..."
	@./scripts/check-docs.sh

# Verbose documentation check
docs-check-verbose:
	@echo "Checking documentation coverage (verbose)..."
	@./scripts/check-docs.sh -v

# Clean build artifacts
clean:
	@echo "Cleaning..."
	@rm -rf bin/ $(COVERAGE_FILE) $(COVERAGE_HTML)
	@go clean -cache

# Run the application
run: build
	@echo "Running $(BINARY_NAME)..."
	@$(BINARY_PATH)

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@echo "Installing golangci-lint v2..."
	@curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(shell go env GOPATH)/bin v2.4.0
	@go install mvdan.cc/gofumpt@latest
	@go install golang.org/x/tools/cmd/goimports@latest
	@echo "Tools installed!"

# Generate mocks (when needed)
generate:
	@echo "Generating code..."
	@go generate ./...

# Help
help:
	@echo "Nebula - High-performance data integration platform"
	@echo ""
	@echo "Usage:"
	@echo "  make [target]"
	@echo ""
	@echo "Targets:"
	@echo "  all          - Format, lint, test, and build"
	@echo "  build        - Build the binary"
	@echo "  test         - Run tests"
	@echo "  coverage     - Run tests with coverage report"
	@echo "  bench        - Run benchmarks"
	@echo "  fmt          - Format code"
	@echo "  lint         - Lint code"
	@echo "  docs-check   - Check documentation coverage"
	@echo "  docs-check-verbose - Check documentation coverage (detailed)"
	@echo "  clean        - Clean build artifacts"
	@echo "  run          - Build and run the application"
	@echo "  install-tools - Install development tools"
	@echo "  generate     - Generate code (mocks, etc.)"
	@echo "  help         - Show this help message"

# Development shortcuts
.PHONY: dev
dev:
	@air -c .air.toml

.PHONY: docker-build
docker-build:
	@echo "Building Docker image..."
	@docker build -t nebula:latest .

.PHONY: docker-run
docker-run: docker-build
	@echo "Running Docker container..."
	@docker run --rm -it nebula:latest