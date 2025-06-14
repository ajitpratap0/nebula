# Contributing to Nebula 🚀

Thank you for your interest in contributing to Nebula! We welcome contributions from the community and are excited to work with you.

## 📋 Table of Contents

- [Code of Conduct](#code-of-conduct)
- [Getting Started](#getting-started)
- [Development Setup](#development-setup)
- [How to Contribute](#how-to-contribute)
- [Pull Request Process](#pull-request-process)
- [Coding Standards](#coding-standards)
- [Testing Guidelines](#testing-guidelines)
- [Performance Requirements](#performance-requirements)
- [Documentation](#documentation)
- [Community](#community)

## 📜 Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

## 🚀 Getting Started

### Prerequisites

- **Go 1.23+**
- **Git**
- **Docker** (for integration tests)
- **Make** (for build automation)

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork locally:
   ```bash
   git clone https://github.com/YOUR_USERNAME/nebula.git
   cd nebula
   ```
3. Add the upstream remote:
   ```bash
   git remote add upstream https://github.com/ajitpratap0/nebula.git
   ```

## 🛠️ Development Setup

### Quick Setup

```bash
# Install development tools
make install-tools

# Verify everything works
make all

# Start development environment
./scripts/dev-setup.sh
```

### Development Workflow

```bash
# Keep your fork synced
git fetch upstream
git checkout main
git merge upstream/main

# Create a feature branch
git checkout -b feature/your-feature-name

# Make changes and test
make all

# Commit and push
git add .
git commit -m "feat: add amazing feature"
git push origin feature/your-feature-name
```

## 🤝 How to Contribute

### Types of Contributions

We welcome several types of contributions:

- 🐛 **Bug Reports**: Help us identify and fix issues
- ✨ **Feature Requests**: Suggest new functionality
- 🔧 **Code Contributions**: Implement features or fix bugs
- 📚 **Documentation**: Improve guides, examples, and API docs
- 🧪 **Testing**: Add test cases and improve coverage
- 🚀 **Performance**: Optimize existing code and algorithms
- 🔌 **Connectors**: Add new source and destination connectors

### Bug Reports

When filing a bug report, please include:

- **Environment**: Go version, OS, Nebula version
- **Steps to Reproduce**: Clear, minimal steps
- **Expected Behavior**: What should happen
- **Actual Behavior**: What actually happens
- **Logs**: Relevant log output or error messages
- **Performance Impact**: If applicable

**Template:**
```markdown
## Environment
- Go version: 1.23.4
- OS: macOS 14.0
- Nebula version: 0.3.0

## Steps to Reproduce
1. Run `./bin/nebula pipeline csv json --source-path large.csv --dest-path out.json`
2. Observe memory usage

## Expected Behavior
Memory should stay under 100MB for 1M records

## Actual Behavior
Memory grows to 500MB and causes OOM

## Logs
```
ERROR: memory allocation failed
```
```

### Feature Requests

For feature requests, please include:

- **Use Case**: Why is this feature needed?
- **Proposal**: Detailed description of the feature
- **Alternatives**: Other solutions you've considered
- **Performance Impact**: Expected impact on throughput/memory
- **Breaking Changes**: Any backwards compatibility concerns

## 📥 Pull Request Process

### Before Submitting

1. **Check existing issues**: Avoid duplicate work
2. **Discuss large changes**: Open an issue for significant features
3. **Write tests**: All new code must have tests
4. **Update documentation**: Keep docs current
5. **Follow conventions**: Use established patterns

### PR Requirements

- ✅ **Tests pass**: `make test` must succeed
- ✅ **Linting passes**: `make lint` must succeed
- ✅ **Performance**: Benchmarks show no regression
- ✅ **Documentation**: Updated if needed
- ✅ **Conventional commits**: Use semantic commit messages

### Commit Message Format

We use [Conventional Commits](https://conventionalcommits.org/):

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Formatting, missing semicolons, etc.
- `refactor`: Code change that neither fixes bug nor adds feature
- `perf`: Performance improvement
- `test`: Adding missing tests
- `chore`: Changes to build process or auxiliary tools

**Examples:**
```
feat(connector): add PostgreSQL CDC source connector

Implements real-time change data capture for PostgreSQL using
logical replication. Includes connection pooling, error recovery,
and state management.

Closes #123
```

```
perf(pipeline): optimize memory allocation in batch processor

Reduces memory allocations by 40% through buffer reuse and
eliminates string concatenation in hot paths.

Benchmark results:
- Before: 1.2M records/sec
- After: 1.7M records/sec
```

### Review Process

1. **Automated Checks**: CI must pass
2. **Maintainer Review**: Core team member reviews
3. **Performance Review**: For performance-critical changes
4. **Community Review**: For significant features
5. **Approval**: At least one maintainer approval required

## 📏 Coding Standards

### Go Code Style

- **gofmt**: All code must be formatted with `gofmt`
- **golint**: Follow Go lint recommendations
- **govet**: Must pass `go vet` checks
- **Naming**: Use clear, descriptive names
- **Comments**: Document all exported functions

### Performance Guidelines

- **Zero-Copy**: Minimize memory allocations
- **Benchmarks**: Include benchmarks for new features
- **Profiling**: Profile performance-critical code
- **Memory Pools**: Use unified `pool.Pool[T]` system
- **Error Handling**: Use structured error types

### Architecture Patterns

- **Unified Configuration**: All connectors use `config.BaseConfig`
- **Memory Management**: Always use `pool.GetRecord()` / `record.Release()`
- **Connector Framework**: Extend `base.BaseConnector`
- **Error Handling**: Use `pkg/errors` for structured errors

### Example Code Style

```go
// Good: Clear naming and error handling
func (c *CSVSource) processRecord(ctx context.Context, row []string) (*pool.Record, error) {
    record := pool.GetRecord()
    defer func() {
        if record != nil {
            record.Release()
        }
    }()

    if err := c.validateRow(row); err != nil {
        return nil, errors.Wrap(err, errors.ErrorTypeValidation, "invalid CSV row")
    }

    // Process row...
    return record, nil
}

// Bad: Poor naming and error handling
func (c *CSVSource) proc(r []string) (*pool.Record, error) {
    rec := pool.GetRecord()
    // Missing validation and proper error handling
    return rec, nil
}
```

## 🧪 Testing Guidelines

### Test Requirements

- **Unit Tests**: All new functions must have unit tests
- **Integration Tests**: Add integration tests for connectors
- **Benchmarks**: Performance-critical code needs benchmarks
- **Coverage**: Maintain >80% test coverage

### Test Structure

```go
func TestCSVSource_ProcessRecord(t *testing.T) {
    tests := []struct {
        name    string
        row     []string
        want    map[string]interface{}
        wantErr bool
    }{
        {
            name: "valid row",
            row:  []string{"1", "Alice", "alice@example.com"},
            want: map[string]interface{}{
                "id":    1,
                "name":  "Alice",
                "email": "alice@example.com",
            },
            wantErr: false,
        },
        {
            name:    "invalid row",
            row:     []string{"invalid"},
            want:    nil,
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            source := NewCSVSource(testConfig)
            got, err := source.processRecord(context.Background(), tt.row)
            
            if (err != nil) != tt.wantErr {
                t.Errorf("processRecord() error = %v, wantErr %v", err, tt.wantErr)
                return
            }
            
            if !tt.wantErr {
                assert.Equal(t, tt.want, got.Data)
                got.Release()
            }
        })
    }
}
```

### Running Tests

```bash
# Run all tests
make test

# Run tests with coverage
make coverage

# Run specific tests
go test -v ./pkg/connector/sources/csv/...

# Run benchmarks
go test -bench=. ./tests/benchmarks/...

# Run integration tests
go test -v -tags=integration ./tests/integration/...
```

## ⚡ Performance Requirements

### Benchmarking

All performance-critical code must include benchmarks:

```go
func BenchmarkCSVSource_ProcessRecord(b *testing.B) {
    source := NewCSVSource(testConfig)
    row := []string{"1", "Alice", "alice@example.com"}
    
    b.ResetTimer()
    b.ReportAllocs()
    
    for i := 0; i < b.N; i++ {
        record, err := source.processRecord(context.Background(), row)
        if err != nil {
            b.Fatal(err)
        }
        record.Release()
    }
}
```

### Performance Targets

- **Throughput**: >1M records/sec for core operations
- **Memory**: <100 bytes/record in optimized mode
- **Latency**: <1ms P99 for individual operations
- **Allocation**: Minimize allocations in hot paths

### Performance Testing

```bash
# Quick performance validation
./scripts/quick-perf-test.sh quick

# Full performance suite
./scripts/quick-perf-test.sh suite

# Memory profiling
go test -bench=BenchmarkHybridStorage -memprofile=mem.prof ./tests/benchmarks/
go tool pprof mem.prof
```

## 📚 Documentation

### Documentation Types

- **API Documentation**: Godoc comments for all exported functions
- **User Guides**: High-level usage documentation
- **Connector Guides**: Specific connector documentation
- **Architecture Docs**: Design decisions and patterns
- **Examples**: Working code examples

### Documentation Standards

- **Clear Examples**: Include working code samples
- **Up-to-Date**: Keep docs synchronized with code
- **Comprehensive**: Cover all major features
- **Accessible**: Use clear, simple language

### Updating Documentation

When making changes, update relevant documentation:

- **README.md**: For user-facing changes
- **CLAUDE.md**: For development patterns
- **docs/**: For detailed guides
- **Godoc**: For API changes

## 🌟 Connector Development

### Adding New Connectors

1. **Create directory structure**:
   ```
   pkg/connector/sources/myconnector/
   ├── myconnector_source.go
   ├── init.go
   └── myconnector_source_test.go
   ```

2. **Implement interfaces**:
   ```go
   type MyConnector struct {
       *base.BaseConnector
       config MyConfig
   }

   type MyConfig struct {
       config.BaseConfig `yaml:",inline"`
       // Connector-specific fields
   }
   ```

3. **Register connector**:
   ```go
   // init.go
   func init() {
       registry.RegisterSource("myconnector", NewMyConnector)
   }
   ```

4. **Add tests and benchmarks**
5. **Update documentation**

### Connector Guidelines

- **Use BaseConnector**: Inherit from `base.BaseConnector`
- **Unified Configuration**: Embed `config.BaseConfig`
- **Error Handling**: Use structured errors
- **Performance**: Include benchmarks
- **Production Features**: Circuit breakers, rate limiting, health checks

## 💬 Community

### Getting Help

- **GitHub Discussions**: General questions and ideas
- **GitHub Issues**: Bug reports and feature requests
- **Documentation**: Check existing docs first

### Communication Guidelines

- **Be Respectful**: Follow our Code of Conduct
- **Be Clear**: Provide context and details
- **Be Patient**: Maintainers are volunteers
- **Be Helpful**: Help others when you can

### Recognition

We recognize contributors in several ways:

- **Contributors List**: Listed in repository
- **Release Notes**: Major contributions highlighted
- **Community Recognition**: Featured in discussions

## 🎯 Next Steps

After your first contribution:

1. **Join Discussions**: Participate in project planning
2. **Review PRs**: Help review other contributions
3. **Mentor Others**: Help new contributors
4. **Become a Maintainer**: Take on more responsibility

## 📞 Questions?

- **General Questions**: [GitHub Discussions](https://github.com/ajitpratap0/nebula/discussions)
- **Bug Reports**: [GitHub Issues](https://github.com/ajitpratap0/nebula/issues)
- **Security Issues**: See [SECURITY.md](SECURITY.md)

---

Thank you for contributing to Nebula! Together, we're building the fastest data integration platform in the world. 🚀