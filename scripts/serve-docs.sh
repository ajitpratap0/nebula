#!/bin/bash

# Script to serve Go documentation locally
# This provides an easy way to browse the Nebula API documentation

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Default port
PORT=${DOC_PORT:-6060}

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

# Check if godoc is installed
check_godoc() {
    if ! command -v godoc &> /dev/null; then
        print_error "godoc is not installed"
        print_info "Installing godoc..."
        go install golang.org/x/tools/cmd/godoc@latest
        
        if ! command -v godoc &> /dev/null; then
            print_error "Failed to install godoc. Please install manually:"
            print_info "go install golang.org/x/tools/cmd/godoc@latest"
            exit 1
        fi
    fi
}

# Check if port is already in use
check_port() {
    if lsof -Pi :$PORT -sTCP:LISTEN -t >/dev/null 2>&1; then
        print_error "Port $PORT is already in use"
        print_info "You can specify a different port with DOC_PORT=8080 $0"
        exit 1
    fi
}

# Main function
main() {
    print_info "Starting Nebula documentation server..."
    
    # Check prerequisites
    check_godoc
    check_port
    
    # Get the project root directory
    SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
    PROJECT_ROOT="$( cd "$SCRIPT_DIR/.." && pwd )"
    
    print_info "Project root: $PROJECT_ROOT"
    print_info "Starting godoc server on port $PORT..."
    
    # Change to project root
    cd "$PROJECT_ROOT"
    
    # Start godoc server
    print_info "Documentation server starting..."
    print_info "Open your browser to: http://localhost:$PORT/pkg/github.com/ajitpratap0/nebula/"
    print_info "Press Ctrl+C to stop the server"
    
    # Add some helpful URLs
    echo ""
    print_info "Quick links:"
    echo "  - Package overview: http://localhost:$PORT/pkg/github.com/ajitpratap0/nebula/"
    echo "  - Config package:   http://localhost:$PORT/pkg/github.com/ajitpratap0/nebula/pkg/config/"
    echo "  - Connector package: http://localhost:$PORT/pkg/github.com/ajitpratap0/nebula/pkg/connector/"
    echo "  - Pool package:     http://localhost:$PORT/pkg/github.com/ajitpratap0/nebula/pkg/pool/"
    echo "  - Pipeline package: http://localhost:$PORT/pkg/github.com/ajitpratap0/nebula/pkg/pipeline/"
    echo ""
    
    # Start the server
    godoc -http=:$PORT -index -links=true -v
}

# Handle Ctrl+C gracefully
trap 'print_info "Documentation server stopped"; exit 0' INT TERM

# Run main function
main