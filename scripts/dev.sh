#!/bin/bash
# Development helper script for Nebula

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Functions
print_success() {
    echo -e "${GREEN}✓${NC} $1"
}

print_error() {
    echo -e "${RED}✗${NC} $1"
}

print_info() {
    echo -e "${YELLOW}→${NC} $1"
}

# Check if Go is installed
check_go() {
    if ! command -v go &> /dev/null; then
        print_error "Go is not installed. Please install Go 1.21+"
        exit 1
    fi
    print_success "Go is installed: $(go version)"
}

# Install development tools
install_tools() {
    print_info "Installing development tools..."
    make install-tools
    print_success "Development tools installed"
}

# Setup local environment
setup_env() {
    if [ ! -f .env ]; then
        print_info "Creating .env file from .env.example..."
        cp .env.example .env
        print_success ".env file created. Please update it with your credentials."
    else
        print_info ".env file already exists"
    fi
}

# Run pre-commit checks
pre_commit() {
    print_info "Running pre-commit checks..."
    make fmt
    make lint
    make test
    print_success "Pre-commit checks passed"
}

# Main menu
show_menu() {
    echo "Nebula Development Helper"
    echo "========================"
    echo "1. Check Go installation"
    echo "2. Install development tools"
    echo "3. Setup local environment"
    echo "4. Run pre-commit checks"
    echo "5. Build project"
    echo "6. Run tests"
    echo "7. Run with hot reload (requires air)"
    echo "8. Exit"
}

# Main loop
while true; do
    show_menu
    read -p "Choose an option: " choice
    
    case $choice in
        1)
            check_go
            ;;
        2)
            install_tools
            ;;
        3)
            setup_env
            ;;
        4)
            pre_commit
            ;;
        5)
            make build
            ;;
        6)
            make test
            ;;
        7)
            if command -v air &> /dev/null; then
                air
            else
                print_error "air is not installed. Install with: go install github.com/cosmtrek/air@latest"
            fi
            ;;
        8)
            print_info "Goodbye!"
            exit 0
            ;;
        *)
            print_error "Invalid option"
            ;;
    esac
    
    echo
    read -p "Press Enter to continue..."
    clear
done