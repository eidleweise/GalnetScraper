#!/usr/bin/env bash

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
USE_UV=false
VENV_PYTHON="$SCRIPT_DIR/.venv/bin/python"

setup_python_environment() {
    if command -v uv >/dev/null 2>&1; then
        USE_UV=true
        return 0
    fi

    local python_command
    if command -v python3 >/dev/null 2>&1; then
        python_command=python3
    elif command -v python >/dev/null 2>&1; then
        python_command=python
    else
        echo "Error: Python 3 is required but was not found." >&2
        return 1
    fi

    if [ ! -x "$VENV_PYTHON" ] || ! "$VENV_PYTHON" -m pip --version >/dev/null 2>&1; then
        "$python_command" -m venv --clear "$SCRIPT_DIR/.venv" || return 1
    fi

    "$VENV_PYTHON" -m pip install -r "$SCRIPT_DIR/requirements.txt"
}

run_python() {
    if [ "$USE_UV" = true ]; then
        uv run --with-requirements "$SCRIPT_DIR/requirements.txt" "$@"
    else
        "$VENV_PYTHON" "$@"
    fi
}

ensure_playwright_browser() {
    run_python -m playwright install chromium
}
