#!/bin/bash -eu

# Dependency checker for validation scripts
check_dependencies() {
    local missing_deps=()

    if ! command -v curl >/dev/null 2>&1; then
        missing_deps+=("curl")
    fi

    if ! command -v jq >/dev/null 2>&1; then
        missing_deps+=("jq")
    fi

    if [ ${#missing_deps[@]} -ne 0 ]; then
        echo "Error: Missing required dependencies: ${missing_deps[*]}"
        echo "Please install them:"
        echo "  macOS: brew install ${missing_deps[*]}"
        echo "  Ubuntu/Debian: sudo apt-get install ${missing_deps[*]}"
        echo "  CentOS/RHEL: sudo yum install ${missing_deps[*]}"
        exit 1
    fi
}

# Function to parse JSON response with fallback
parse_calls_remaining() {
    local response="$1"
    local remaining

    # Try jq first (preferred)
    if command -v jq >/dev/null 2>&1; then
        remaining=$(echo "$response" | jq -r '.calls_remaining // 0' 2>/dev/null || echo "0")
    else
        # Fallback to grep/sed parsing
        remaining=$(echo "$response" | grep -o '"calls_remaining":[0-9]*' | cut -d: -f2 2>/dev/null || echo "0")
    fi

    echo "$remaining"
}

# Export functions for use by validation scripts
export -f check_dependencies
export -f parse_calls_remaining