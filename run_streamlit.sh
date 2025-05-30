#!/bin/bash

# Function to clean up processes and reset
cleanup() {
    echo "Cleaning up..."
    pkill -f streamlit
    pkill -f "python3 -m streamlit"
    # Clear all Python processes that might be holding onto Streamlit
    pkill -f python
    
    # Clear temporary Python files
    find . -type d -name "__pycache__" -exec rm -r {} + 2>/dev/null
    find . -type f -name "*.pyc" -delete
    find . -type f -name "*.pyo" -delete
    find . -type f -name ".streamlit_app.py" -delete
    
    # Clear Streamlit cache directory
    rm -rf ~/.streamlit/cache
    
    echo "Cleanup complete"
}

# Set up trap for cleanup
trap cleanup EXIT INT TERM

# Initial cleanup
cleanup

# Wait for processes to fully terminate
sleep 2

# Set file limits
ulimit -n 65536 2>/dev/null || ulimit -n 4096 2>/dev/null || ulimit -n 2048

# Verify file limits
echo "Current file limit: $(ulimit -n)"

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

# Set environment variables
export PYTHONUNBUFFERED=1
export PYTHONPATH="${PYTHONPATH}:${PWD}"
export STREAMLIT_SERVER_MAX_UPLOAD_SIZE=200
export STREAMLIT_SERVER_ADDRESS=localhost
export STREAMLIT_SERVER_PORT=8501
# Disable watchdog to prevent file watching issues
export STREAMLIT_SERVER_FILE_WATCHER_TYPE=none
# Force single-threaded mode
export STREAMLIT_SERVER_WORKERS=1

# Function to run streamlit
run_streamlit() {
    echo "Starting Streamlit..."
    if ! streamlit run --server.runOnSave true --server.fileWatcherType none HOME.py; then
        echo "First attempt failed, trying alternative method..."
        if ! python3 -m streamlit run --server.runOnSave true --server.fileWatcherType none HOME.py; then
            echo "Both attempts failed. Please check the error messages above."
            return 1
        fi
    fi
}

# Main execution
echo "Setting up environment..."
run_streamlit 