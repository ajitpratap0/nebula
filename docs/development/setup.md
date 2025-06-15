# Development Setup Guide

This guide will help you set up your development environment for contributing to Nebula.

## Prerequisites

### Required Software

1. **Go 1.23+** (with 1.24.3 toolchain)
   ```bash
   # Install via official installer or package manager
   # Verify installation
   go version
   # Should show: go version go1.23.x
   ```

2. **Git**
   ```bash
   git --version
   # Should show: git version 2.x.x
   ```

3. **Make**
   ```bash
   make --version
   # Should show: GNU Make 4.x
   ```

4. **Docker** (for integration tests)
   ```bash
   docker --version
   # Should show: Docker version 20.x.x
   ```

### Recommended Tools

1. **golangci-lint** (installed automatically)
2. **gofumpt** (installed automatically)
3. **air** (for hot reload development)
4. **VS Code** with Go extension

## Getting Started

### 1. Clone the Repository

```bash
# Clone via HTTPS
git clone https://github.com/ajitpratap0/nebula.git

# Or via SSH
git clone git@github.com:ajitpratap0/nebula.git

cd nebula
```

### 2. Install Development Tools

```bash
# Install all required tools
make install-tools

# This installs:
# - golangci-lint (linting)
# - gofumpt (formatting)
# - goimports (import organization)
```

### 3. Verify Installation

```bash
# Run all checks
make all

# This runs:
# - make fmt (code formatting)
# - make lint (linting)
# - make test (unit tests)
# - make build (compilation)
```

### 4. Build the Project

```bash
# Build the binary
make build

# Run the binary
./bin/nebula help

# Check version
./bin/nebula version
```

## Development Environment

### VS Code Setup

1. Install the Go extension
2. Open the project: `code .`
3. The project includes devcontainer configuration

**Recommended VS Code settings** (`.vscode/settings.json`):
```json
{
    "go.useLanguageServer": true,
    "go.lintTool": "golangci-lint",
    "go.lintOnSave": "package",
    "go.formatTool": "gofumpt",
    "go.testFlags": ["-v", "-race"],
    "editor.formatOnSave": true
}
```

### Using DevContainers

The project includes a devcontainer configuration:

```bash
# Open in VS Code
code .

# VS Code will prompt to reopen in container
# Click "Reopen in Container"
```

The devcontainer includes:
- Go 1.23 with all tools
- PostgreSQL and MySQL for testing
- Redis for caching tests
- All development dependencies

### Docker Development Environment

Set up the full development environment:

```bash
# Start all services
./scripts/dev-setup.sh

# This starts:
# - PostgreSQL (port 5432)
# - MySQL (port 3306)
# - Redis (port 6379)

# Stop services
docker-compose -f deployments/docker/docker-compose.dev.yml down
```

## Development Workflow

### 1. Create a Feature Branch

```bash
# Create and checkout a new branch
git checkout -b feature/my-new-feature

# Or for bug fixes
git checkout -b fix/issue-description
```

### 2. Make Your Changes

Follow the coding standards:
- Run `make fmt` before committing
- Ensure `make lint` passes
- Add tests for new functionality
- Update documentation as needed

### 3. Test Your Changes

```bash
# Run unit tests
make test

# Run specific package tests
go test -v ./pkg/connector/...

# Run with race detection
go test -race ./...

# Run integration tests
make test-integration

# Check test coverage
make coverage
```

### 4. Benchmark Your Changes

```bash
# Run all benchmarks
make bench

# Run specific benchmarks
go test -bench=BenchmarkHybridStorage ./tests/benchmarks/...

# Profile CPU usage
go test -bench=. -cpuprofile=cpu.prof ./tests/benchmarks/...
go tool pprof cpu.prof

# Profile memory usage
go test -bench=. -memprofile=mem.prof ./tests/benchmarks/...
go tool pprof mem.prof
```

### 5. Validate Performance

```bash
# Quick performance test
./scripts/quick-perf-test.sh quick

# Full performance suite
./scripts/quick-perf-test.sh suite

# Establish baseline
make baseline
```

### 6. Pre-commit Checks

```bash
# Run all pre-commit checks
make all

# This ensures:
# - Code is formatted (gofumpt)
# - Linting passes (golangci-lint)
# - Tests pass with race detection
# - Binary builds successfully
```

## Hot Reload Development

For rapid development with automatic reloading:

```bash
# Install air
go install github.com/air-verse/air@latest

# Run with hot reload
make dev

# Or directly
air
```

## Database Setup for Testing

### PostgreSQL CDC Testing

```bash
# Start PostgreSQL with replication enabled
docker run -d \
  --name postgres-cdc \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=testdb \
  -p 5432:5432 \
  postgres:14 \
  -c wal_level=logical

# Create replication slot
docker exec -it postgres-cdc psql -U postgres -d testdb \
  -c "SELECT pg_create_logical_replication_slot('nebula_slot', 'pgoutput');"
```

### MySQL CDC Testing

```bash
# Start MySQL with binlog enabled
docker run -d \
  --name mysql-cdc \
  -e MYSQL_ROOT_PASSWORD=root \
  -e MYSQL_DATABASE=testdb \
  -p 3306:3306 \
  mysql:8.0 \
  --log-bin=mysql-bin \
  --binlog-format=ROW \
  --server-id=1
```

## Common Development Tasks

### Adding a New Connector

1. Create the connector structure:
   ```bash
   mkdir -p pkg/connector/sources/myconnector
   ```

2. Implement the connector following the template
3. Add tests
4. Register in `init.go`
5. Update documentation

### Running Specific Tests

```bash
# Test a specific connector
go test -v ./pkg/connector/sources/csv/...

# Test with specific pattern
go test -v -run TestCSVSource ./...

# Benchmark specific function
go test -bench=BenchmarkColumnarStorage -benchmem ./...
```

### Debugging

```bash
# Run with debug logging
NEBULA_LOG_LEVEL=debug ./bin/nebula pipeline csv json \
  --source-path input.csv \
  --dest-path output.json

# Use delve debugger
dlv debug ./cmd/nebula -- pipeline csv json \
  --source-path input.csv \
  --dest-path output.json
```

## Environment Variables

Key environment variables for development:

```bash
# Logging
export NEBULA_LOG_LEVEL=debug
export NEBULA_LOG_FORMAT=json

# Performance
export NEBULA_WORKERS=8
export NEBULA_BATCH_SIZE=10000

# Testing
export NEBULA_TEST_TIMEOUT=30s
export NEBULA_TEST_PARALLEL=4
```

## Troubleshooting

### Common Issues

1. **Build fails with module errors**
   ```bash
   go mod tidy
   go mod download
   ```

2. **Lint errors**
   ```bash
   # See detailed lint errors
   golangci-lint run -v
   
   # Auto-fix some issues
   golangci-lint run --fix
   ```

3. **Test failures**
   ```bash
   # Run tests verbosely
   go test -v -failfast ./...
   
   # Check for race conditions
   go test -race ./...
   ```

4. **Performance regression**
   ```bash
   # Compare with baseline
   make bench > new.txt
   benchcmp baseline.txt new.txt
   ```

## Next Steps

- Review the [Code Style Guide](style-guide.md)
- Read the [Testing Guide](testing.md)
- Check out [Contributing Guidelines](contributing.md)
- Explore the [Architecture Documentation](../architecture/overview.md)