#!/bin/bash

# Nebula Development Environment Monitoring Script
# Provides real-time monitoring of the development environment

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

log_success() {
    echo -e "${GREEN}[✓]${NC} $1"
}

log_warning() {
    echo -e "${YELLOW}[⚠]${NC} $1"
}

log_error() {
    echo -e "${RED}[✗]${NC} $1"
}

log_header() {
    echo -e "${CYAN}========================================${NC}"
    echo -e "${CYAN}$1${NC}"
    echo -e "${CYAN}========================================${NC}"
}

# Check service health
check_service_health() {
    local service_name=$1
    local check_command=$2
    
    if eval "$check_command" &> /dev/null; then
        log_success "$service_name is healthy"
    else
        log_error "$service_name is not responding"
    fi
}

# Monitor container status
monitor_containers() {
    log_header "CONTAINER STATUS"
    
    if command -v docker-compose &> /dev/null; then
        COMPOSE_CMD="docker-compose"
    else
        COMPOSE_CMD="docker compose"
    fi
    
    echo -e "${CYAN}Service${NC}\t\t${CYAN}Status${NC}\t\t${CYAN}Health${NC}"
    echo "----------------------------------------"
    
    # Get container status
    containers=$(docker ps --filter "name=nebula-" --format "{{.Names}}\t{{.Status}}")
    
    if [ -z "$containers" ]; then
        log_warning "No Nebula containers running"
        return
    fi
    
    while IFS=$'\t' read -r name status; do
        if [[ $status == *"healthy"* ]]; then
            echo -e "${name}\t${GREEN}${status}${NC}"
        elif [[ $status == *"Up"* ]]; then
            echo -e "${name}\t${YELLOW}${status}${NC}"
        else
            echo -e "${name}\t${RED}${status}${NC}"
        fi
    done <<< "$containers"
}

# Monitor service connectivity
monitor_services() {
    log_header "SERVICE CONNECTIVITY"
    
    # Check PostgreSQL
    check_service_health "PostgreSQL" "docker exec nebula-postgres-dev pg_isready -U nebula -d nebula_dev"
    
    # Check MySQL
    check_service_health "MySQL" "docker exec nebula-mysql-dev mysqladmin ping -h localhost -u nebula -ppassword --silent"
    
    # Check Redis
    check_service_health "Redis" "docker exec nebula-redis-dev redis-cli ping"
    
    # Check MinIO
    check_service_health "MinIO" "curl -f http://localhost:9000/minio/health/live"
    
    # Check Nebula app (if running)
    if docker ps --filter "name=nebula-dev" --filter "status=running" | grep -q nebula-dev; then
        check_service_health "Nebula App" "curl -f http://localhost:9090/metrics"
    else
        log_warning "Nebula App container not running"
    fi
}

# Monitor resource usage
monitor_resources() {
    log_header "RESOURCE USAGE"
    
    # Docker stats for Nebula containers
    echo -e "${CYAN}Container Resource Usage:${NC}"
    docker stats --no-stream --format "table {{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" \
        $(docker ps --filter "name=nebula-" --format "{{.Names}}" | tr '\n' ' ')
}

# Monitor database connections
monitor_databases() {
    log_header "DATABASE STATUS"
    
    # PostgreSQL connections
    echo -e "${CYAN}PostgreSQL Connections:${NC}"
    if docker exec nebula-postgres-dev psql -U nebula -d nebula_dev -c "SELECT count(*) as active_connections FROM pg_stat_activity WHERE state = 'active';" 2>/dev/null; then
        log_success "PostgreSQL query executed successfully"
    else
        log_error "Failed to query PostgreSQL"
    fi
    
    echo ""
    
    # MySQL connections
    echo -e "${CYAN}MySQL Connections:${NC}"
    if docker exec nebula-mysql-dev mysql -u nebula -ppassword -e "SHOW STATUS WHERE Variable_name = 'Threads_connected';" 2>/dev/null; then
        log_success "MySQL query executed successfully"
    else
        log_error "Failed to query MySQL"
    fi
}

# Monitor application logs
monitor_logs() {
    log_header "RECENT APPLICATION LOGS"
    
    if docker ps --filter "name=nebula-dev" --filter "status=running" | grep -q nebula-dev; then
        echo -e "${CYAN}Last 10 log entries from Nebula app:${NC}"
        docker logs nebula-dev --tail 10
    else
        log_warning "Nebula app container not running"
    fi
}

# Show performance metrics
show_metrics() {
    log_header "PERFORMANCE METRICS"
    
    if curl -s http://localhost:9090/metrics | grep -E "(records_processed|processing_time|memory_usage)" &> /dev/null; then
        echo -e "${CYAN}Application Metrics:${NC}"
        curl -s http://localhost:9090/metrics | grep -E "(records_processed|processing_time|memory_usage)" | head -10
    else
        log_warning "No metrics available or app not running"
    fi
}

# Show network information
show_network_info() {
    log_header "NETWORK INFORMATION"
    
    echo -e "${CYAN}Port Mappings:${NC}"
    docker ps --filter "name=nebula-" --format "table {{.Names}}\t{{.Ports}}"
    
    echo ""
    echo -e "${CYAN}Service URLs:${NC}"
    echo "• Application:     http://localhost:8080"
    echo "• Metrics:         http://localhost:9090"
    echo "• MinIO Console:   http://localhost:9001"
    echo "• MinIO API:       http://localhost:9000"
    echo "• PostgreSQL:      localhost:5432"
    echo "• MySQL:           localhost:3306"
    echo "• Redis:           localhost:6379"
}

# Continuous monitoring mode
continuous_monitor() {
    log_info "Starting continuous monitoring mode (Press Ctrl+C to stop)"
    
    while true; do
        clear
        echo -e "${GREEN}🔍 Nebula Development Environment Monitor${NC}"
        echo -e "${YELLOW}$(date)${NC}"
        echo ""
        
        monitor_containers
        echo ""
        monitor_services
        echo ""
        monitor_resources
        echo ""
        
        echo -e "\n${BLUE}Refreshing in 10 seconds...${NC}"
        sleep 10
    done
}

# Generate development report
generate_report() {
    local report_file="dev-environment-report-$(date +%Y%m%d-%H%M%S).txt"
    
    log_info "Generating development environment report..."
    
    {
        echo "Nebula Development Environment Report"
        echo "Generated: $(date)"
        echo "========================================"
        echo ""
        
        echo "CONTAINER STATUS"
        echo "----------------"
        docker ps --filter "name=nebula-" --format "{{.Names}}\t{{.Status}}\t{{.Ports}}"
        echo ""
        
        echo "RESOURCE USAGE"
        echo "--------------"
        docker stats --no-stream --format "{{.Name}}\t{{.CPUPerc}}\t{{.MemUsage}}\t{{.NetIO}}" \
            $(docker ps --filter "name=nebula-" --format "{{.Names}}" | tr '\n' ' ')
        echo ""
        
        echo "DOCKER IMAGES"
        echo "-------------"
        docker images | grep nebula
        echo ""
        
        echo "DOCKER VOLUMES"
        echo "--------------"
        docker volume ls | grep nebula
        echo ""
        
        echo "NETWORK CONFIGURATION"
        echo "--------------------"
        docker network ls | grep nebula
        echo ""
        
        if command -v docker-compose &> /dev/null; then
            echo "DOCKER COMPOSE STATUS"
            echo "--------------------"
            docker-compose -f docker-compose.dev.yml ps
        fi
        
    } > "$report_file"
    
    log_success "Report generated: $report_file"
}

# Main execution
case "${1:-status}" in
    "status"|"")
        monitor_containers
        echo ""
        monitor_services
        echo ""
        show_network_info
        ;;
    "watch"|"continuous")
        continuous_monitor
        ;;
    "resources")
        monitor_resources
        ;;
    "db"|"databases")
        monitor_databases
        ;;
    "logs")
        monitor_logs
        ;;
    "metrics")
        show_metrics
        ;;
    "full")
        monitor_containers
        echo ""
        monitor_services
        echo ""
        monitor_resources
        echo ""
        monitor_databases
        echo ""
        show_network_info
        ;;
    "report")
        generate_report
        ;;
    "help")
        echo "Usage: $0 [command]"
        echo ""
        echo "Commands:"
        echo "  status      - Show basic status (default)"
        echo "  watch       - Continuous monitoring mode"
        echo "  resources   - Show resource usage"
        echo "  databases   - Check database connectivity"
        echo "  logs        - Show recent application logs"
        echo "  metrics     - Show performance metrics"
        echo "  full        - Show comprehensive status"
        echo "  report      - Generate detailed report file"
        echo "  help        - Show this help"
        ;;
    *)
        log_error "Unknown command: $1"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac