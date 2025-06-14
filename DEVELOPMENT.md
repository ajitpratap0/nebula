# Nebula Development Environment

This document provides comprehensive guidance for setting up and using the Nebula development environment.

## 🚀 Quick Start

### Prerequisites

- Docker and Docker Compose
- Go 1.23+ (for local development)
- Git
- VS Code with Remote-Containers extension (recommended)

### Clone and Setup

```bash
# Clone the repository
git clone https://github.com/ajitpratap0/nebula.git
cd nebula

# One-command setup
./scripts/dev-setup.sh
```

This script will:
- ✅ Check prerequisites
- ✅ Build development containers
- ✅ Start all services (PostgreSQL, MySQL, Redis, MinIO)
- ✅ Create test data
- ✅ Test the pipeline
- ✅ Show connection information

## 🏗️ Development Options

### Option 1: VS Code DevContainer (Recommended)

1. Open the project in VS Code
2. Install the "Remote - Containers" extension
3. Press `Ctrl+Shift+P` and select "Remote-Containers: Reopen in Container"
4. Wait for the environment to build and start

The devcontainer provides:
- 🔧 Pre-configured Go environment
- 🗄️ Connected databases (PostgreSQL, MySQL, Redis)
- 🛠️ Development tools (Air, golangci-lint, etc.)
- 🔍 Debugging support with Delve
- 📊 Integrated performance monitoring

### Option 2: Local Development with Docker Services

```bash
# Start only the services (databases, etc.)
docker-compose -f docker-compose.dev.yml up -d postgres mysql redis minio

# Develop locally
go build -o bin/nebula .
./bin/nebula help
```

### Option 3: Full Docker Development

```bash
# Start everything including the app in a container
docker-compose -f docker-compose.dev.yml up -d

# View logs
docker-compose -f docker-compose.dev.yml logs -f nebula
```

## 🛠️ Development Workflow

### Available Scripts

| Script | Purpose |
|--------|---------|
| `./scripts/dev-setup.sh` | Complete environment setup |
| `./scripts/dev-monitor.sh` | Monitor services and performance |
| `./scripts/quick-perf-test.sh` | Run performance tests |

### Common Commands

```bash
# Start development environment
./scripts/dev-setup.sh start

# Monitor environment
./scripts/dev-monitor.sh watch

# Run performance tests
./scripts/quick-perf-test.sh

# Stop environment
./scripts/dev-setup.sh stop

# Rebuild environment
./scripts/dev-setup.sh rebuild
```

### Hot Reload Development

The development environment includes Air for hot reloading:

```bash
# In the devcontainer or with local setup
air -c .air.toml
```

This will automatically rebuild and restart the application when you make changes.

### VS Code Tasks

Press `Ctrl+Shift+P` and search for "Tasks" to access:

- **Build Nebula** - Build the binary
- **Run Tests** - Execute test suite
- **Run Benchmarks** - Performance benchmarks
- **Format Code** - Format with goimports
- **Lint Code** - Run golangci-lint
- **Start Hot Reload** - Begin hot reload development
- **Test Pipeline** - Test CSV to JSON conversion

### Debugging

#### VS Code Debugging

1. Set breakpoints in your code
2. Press `F5` or use the Debug view
3. Select appropriate launch configuration:
   - "Launch Nebula CLI"
   - "Debug CSV to JSON Pipeline" 
   - "Debug Current Test"

#### Remote Debugging

For debugging in containers:

```bash
# Build with debug info
go build -gcflags="all=-N -l" -o bin/nebula .

# Start with Delve
dlv --listen=:2345 --headless=true --api-version=2 exec ./bin/nebula
```

Then attach VS Code to port 2345.

## 🗄️ Database Development

### Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| PostgreSQL | `localhost:5432` | `nebula` / `password` |
| MySQL | `localhost:3306` | `nebula` / `password` |
| Redis | `localhost:6379` | No auth |
| MinIO Console | `http://localhost:9001` | `nebula` / `nebulapass123` |
| MinIO API | `http://localhost:9000` | - |
| Application | `http://localhost:8080` | - |
| Metrics | `http://localhost:9090` | - |

### Database Setup

Both PostgreSQL and MySQL are pre-configured with:
- ✅ Sample data for testing
- ✅ CDC/replication enabled
- ✅ Test tables and procedures
- ✅ Monitoring views

### Testing CDC Functionality

```bash
# Connect to PostgreSQL
docker exec -it nebula-postgres-dev psql -U nebula -d nebula_dev

# Insert test data
INSERT INTO testing.users (name, email, age, city) VALUES ('Test User', 'test@example.com', 25, 'Test City');

# Check replication status
SELECT * FROM testing.replication_status;
```

```bash
# Connect to MySQL
docker exec -it nebula-mysql-dev mysql -u nebula -ppassword nebula_dev

# Insert test data
INSERT INTO users (name, email, age, city) VALUES ('Test User', 'test@example.com', 25, 'Test City');

# Generate more test data
CALL generate_test_data(100, 500);
```

## 📊 Performance Monitoring

### Real-time Monitoring

```bash
# Monitor all services
./scripts/dev-monitor.sh watch

# Monitor just resources
./scripts/dev-monitor.sh resources

# Monitor databases
./scripts/dev-monitor.sh databases
```

### Performance Testing

```bash
# Full performance suite
./scripts/quick-perf-test.sh

# Continuous monitoring
./scripts/quick-perf-test.sh continuous

# Memory benchmark
./scripts/quick-perf-test.sh memory

# Quick single test
./scripts/quick-perf-test.sh quick
```

### Benchmarking

```bash
# Run Go benchmarks
go test -bench=. -benchmem ./tests/benchmarks/...

# Profile CPU usage
go test -bench=. -cpuprofile=cpu.prof ./tests/benchmarks/...
go tool pprof cpu.prof

# Profile memory usage
go test -bench=. -memprofile=mem.prof ./tests/benchmarks/...
go tool pprof mem.prof
```

## 🧪 Testing

### Unit Tests

```bash
# Run all tests
go test -v ./...

# Run tests with race detection
go test -v -race ./...

# Run specific package tests
go test -v ./pkg/connector/...
```

### Integration Tests

```bash
# Test with actual databases
go test -v ./tests/integration/...

# Test CDC functionality
go test -v ./tests/cdc/...
```

### End-to-End Testing

```bash
# Test complete pipelines
./bin/nebula pipeline csv json --source-path test_input.csv --dest-path test_output.json

# Test different connectors
./bin/nebula pipeline csv csv --source-path input.csv --dest-path output.csv
```

## 🔧 Configuration

### Environment Variables

The development environment supports these environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `GO_ENV` | `development` | Environment mode |
| `POSTGRES_URL` | `postgres://nebula:password@postgres:5432/nebula_dev` | PostgreSQL connection |
| `MYSQL_URL` | `nebula:password@tcp(mysql:3306)/nebula_dev` | MySQL connection |
| `REDIS_URL` | `redis://redis:6379/0` | Redis connection |

### Hot Reload Configuration

Edit `.air.toml` to customize hot reload behavior:

```toml
[build]
cmd = "go build -o ./tmp/nebula ."
include_ext = ["go", "yml", "yaml"]
exclude_dir = ["tmp", "vendor", "bin"]
```

## 🚨 Troubleshooting

### Common Issues

#### Services Not Starting

```bash
# Check Docker status
docker ps

# Check service logs
docker-compose -f docker-compose.dev.yml logs

# Restart services
./scripts/dev-setup.sh rebuild
```

#### Connection Issues

```bash
# Test service connectivity
./scripts/dev-monitor.sh status

# Check network
docker network ls | grep nebula
```

#### Performance Issues

```bash
# Monitor resources
./scripts/dev-monitor.sh resources

# Run performance diagnostics
./scripts/quick-perf-test.sh memory
```

#### Build Issues

```bash
# Clean and rebuild
go clean -cache
go mod tidy
go build ./...
```

### Getting Help

1. Check service status: `./scripts/dev-monitor.sh`
2. View logs: `docker-compose -f docker-compose.dev.yml logs`
3. Generate report: `./scripts/dev-monitor.sh report`
4. Check this documentation
5. Review container logs for specific services

## 🎯 Development Tips

### Code Quality

- ✅ Use `golangci-lint` for code quality
- ✅ Format with `goimports` before committing
- ✅ Write tests for new functionality
- ✅ Use the provided VS Code tasks

### Performance

- 🚀 Use the hybrid storage system for optimal memory usage
- 🚀 Monitor memory per record (target: <100 bytes)
- 🚀 Use provided benchmarking tools
- 🚀 Profile before optimizing

### Debugging

- 🔍 Use VS Code debugging configurations
- 🔍 Set up remote debugging for container issues
- 🔍 Use the monitoring scripts for real-time insights
- 🔍 Check metrics at `http://localhost:9090`

### Database Development

- 📊 Use the pre-configured test data
- 📊 Test CDC changes with provided tables
- 📊 Monitor replication status with provided views
- 📊 Use the database generation procedures for load testing

## 📚 Next Steps

1. **Explore the Codebase**: Start with `cmd/nebula/main.go`
2. **Run Performance Tests**: Use `./scripts/quick-perf-test.sh`
3. **Try Different Connectors**: Test CSV, JSON, and database connectors
4. **Monitor Performance**: Use the hybrid storage system and monitor efficiency
5. **Contribute**: Follow the patterns established in existing connectors

## 🎉 Ready to Develop!

Your Nebula development environment is now ready for high-performance data integration development. The environment provides:

- 🏃‍♂️ **1.7M-3.6M records/sec** throughput capability
- 🧠 **84 bytes/record** memory efficiency with hybrid storage
- 🔧 **Complete development toolkit** with debugging, profiling, and monitoring
- 🗄️ **Production-like services** for realistic testing
- 📊 **Real-time performance monitoring** and optimization

Happy coding! 🚀