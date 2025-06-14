#!/bin/bash

# Nebula Development Environment Setup Script
# This script sets up the complete development environment

set -e

echo "🚀 Setting up Nebula development environment..."

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Check if Docker is installed and running
check_docker() {
    log_info "Checking Docker installation..."
    if ! command -v docker &> /dev/null; then
        log_error "Docker is not installed. Please install Docker first."
        exit 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker is not running. Please start Docker first."
        exit 1
    fi
    
    log_success "Docker is installed and running"
}

# Check if Docker Compose is available
check_docker_compose() {
    log_info "Checking Docker Compose..."
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        log_error "Docker Compose is not available. Please install Docker Compose."
        exit 1
    fi
    log_success "Docker Compose is available"
}

# Build the development environment
build_dev_environment() {
    log_info "Building development environment..."
    
    if command -v docker-compose &> /dev/null; then
        COMPOSE_CMD="docker-compose"
    else
        COMPOSE_CMD="docker compose"
    fi
    
    $COMPOSE_CMD -f docker-compose.dev.yml build --no-cache
    log_success "Development environment built"
}

# Start the development environment
start_dev_environment() {
    log_info "Starting development environment..."
    
    if command -v docker-compose &> /dev/null; then
        COMPOSE_CMD="docker-compose"
    else
        COMPOSE_CMD="docker compose"
    fi
    
    $COMPOSE_CMD -f docker-compose.dev.yml up -d
    log_success "Development environment started"
}

# Wait for services to be healthy
wait_for_services() {
    log_info "Waiting for services to be healthy..."
    
    # Wait for PostgreSQL
    log_info "Waiting for PostgreSQL..."
    timeout 60 bash -c 'until docker exec nebula-postgres-dev pg_isready -U nebula -d nebula_dev; do sleep 2; done'
    log_success "PostgreSQL is ready"
    
    # Wait for MySQL
    log_info "Waiting for MySQL..."
    timeout 60 bash -c 'until docker exec nebula-mysql-dev mysqladmin ping -h localhost -u nebula -ppassword --silent; do sleep 2; done'
    log_success "MySQL is ready"
    
    # Wait for Redis
    log_info "Waiting for Redis..."
    timeout 30 bash -c 'until docker exec nebula-redis-dev redis-cli ping; do sleep 2; done'
    log_success "Redis is ready"
    
    # Wait for MinIO
    log_info "Waiting for MinIO..."
    timeout 30 bash -c 'until curl -f http://localhost:9000/minio/health/live > /dev/null 2>&1; do sleep 2; done'
    log_success "MinIO is ready"
}

# Create test data
create_test_data() {
    log_info "Creating test data..."
    
    # Create CSV test file
    cat > test_input.csv << EOF
name,age,city,department,salary
John Doe,30,New York,Engineering,75000
Jane Smith,25,Los Angeles,Marketing,65000
Bob Johnson,35,Chicago,Sales,70000
Alice Brown,28,Houston,Engineering,80000
Charlie Wilson,32,Phoenix,Marketing,62000
David Lee,29,Philadelphia,Engineering,78000
Emma Davis,31,San Antonio,Sales,68000
Frank Miller,27,San Diego,Engineering,82000
Grace Chen,26,Dallas,Marketing,64000
Henry Garcia,33,San Jose,Sales,72000
EOF
    
    log_success "Test data created (test_input.csv)"
}

# Test the pipeline
test_pipeline() {
    log_info "Testing CSV to JSON pipeline..."
    
    # Build nebula binary
    go build -o bin/nebula ./cmd/nebula
    
    # Run pipeline test
    ./bin/nebula pipeline csv json --source-path test_input.csv --dest-path test_output.json --format array
    
    if [ -f test_output.json ]; then
        log_success "Pipeline test completed successfully"
        log_info "Output file: test_output.json"
    else
        log_error "Pipeline test failed - no output file generated"
        exit 1
    fi
}

# Show connection information
show_connection_info() {
    echo ""
    echo "🎉 Development environment is ready!"
    echo ""
    echo "📊 Service URLs:"
    echo "  • Application:     http://localhost:8080"
    echo "  • Metrics:         http://localhost:9090"
    echo "  • MinIO Console:   http://localhost:9001 (nebula/nebulapass123)"
    echo "  • MinIO API:       http://localhost:9000"
    echo ""
    echo "🗄️  Database Connections:"
    echo "  • PostgreSQL:      localhost:5432 (nebula/password)"
    echo "  • MySQL:           localhost:3306 (nebula/password)"
    echo "  • Redis:           localhost:6379"
    echo ""
    echo "🛠️  Development Commands:"
    echo "  • Start development: docker-compose -f docker-compose.dev.yml up -d"
    echo "  • Stop development:  docker-compose -f docker-compose.dev.yml down"
    echo "  • View logs:         docker-compose -f docker-compose.dev.yml logs -f"
    echo "  • Hot reload:        air -c .air.toml"
    echo "  • Run tests:         go test -v ./..."
    echo "  • Run benchmarks:    go test -bench=. ./tests/benchmarks/..."
    echo ""
    echo "📝 Next Steps:"
    echo "  1. Open VS Code with Remote-Containers extension"
    echo "  2. Run 'Remote-Containers: Reopen in Container'"
    echo "  3. Use Ctrl+Shift+P and search 'Tasks' for development commands"
    echo ""
}

# Main execution
main() {
    echo "🌟 Nebula High-Performance Data Integration Platform"
    echo "   Development Environment Setup"
    echo ""
    
    check_docker
    check_docker_compose
    build_dev_environment
    start_dev_environment
    wait_for_services
    create_test_data
    test_pipeline
    show_connection_info
    
    log_success "Setup completed successfully! 🎉"
}

# Handle script interruption
trap 'log_error "Setup interrupted"; exit 1' INT

# Parse command line arguments
case "${1:-setup}" in
    "setup")
        main
        ;;
    "start")
        start_dev_environment
        wait_for_services
        show_connection_info
        ;;
    "stop")
        log_info "Stopping development environment..."
        if command -v docker-compose &> /dev/null; then
            docker-compose -f docker-compose.dev.yml down
        else
            docker compose -f docker-compose.dev.yml down
        fi
        log_success "Development environment stopped"
        ;;
    "rebuild")
        log_info "Rebuilding development environment..."
        if command -v docker-compose &> /dev/null; then
            docker-compose -f docker-compose.dev.yml down
            docker-compose -f docker-compose.dev.yml build --no-cache
            docker-compose -f docker-compose.dev.yml up -d
        else
            docker compose -f docker-compose.dev.yml down
            docker compose -f docker-compose.dev.yml build --no-cache
            docker compose -f docker-compose.dev.yml up -d
        fi
        wait_for_services
        log_success "Development environment rebuilt and started"
        ;;
    "test")
        create_test_data
        test_pipeline
        ;;
    "help")
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  setup     - Full setup (default)"
        echo "  start     - Start existing environment"
        echo "  stop      - Stop environment"
        echo "  rebuild   - Rebuild and restart"
        echo "  test      - Test pipeline functionality"
        echo "  help      - Show this help"
        ;;
    *)
        log_error "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac