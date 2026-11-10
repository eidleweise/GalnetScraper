#!/bin/bash

# Navigate to the script's directory
cd "$(dirname "$0")"

source ./python_env.sh

echo "Checking Python dependencies..."
setup_python_environment || exit 1

echo "Ensuring Playwright Chromium is installed..."
ensure_playwright_browser || exit 1

echo "Starting Galnet Scraper..."
run_python galnet_scraper.py
