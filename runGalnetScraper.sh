#!/bin/bash

# Navigate to the script's directory
cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

echo "Installing/Updating requirements..."
pip install -r requirements.txt

echo "Ensuring Playwright browsers are installed..."
playwright install

echo "Starting Galnet Scraper..."
python galnetScraper.py
