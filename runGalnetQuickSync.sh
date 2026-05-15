#!/bin/bash

# Navigate to the script's directory
cd "$(dirname "$0")"

# Activate virtual environment if it exists
if [ -d ".venv" ]; then
    source .venv/bin/activate
fi

echo "Starting Galnet Quick Sync..."
python galnet_scraper.py 1

# Stage all changes and commit with article dates
git add -A

# Extract unique dates from newly staged JSON filenames (format: YYYY-MM-DD_slug.json)
DATES=$(git diff --cached --name-only -- 'GalnetNewsArchive/*.json' | sed 's|GalnetNewsArchive/||' | grep -oP '^\d{4}-\d{2}-\d{2}' | sort -u | paste -sd ', ')

if [ -n "$DATES" ]; then
    git commit -m "Committing galnet articles dated $DATES"
    echo "Committed with dates: $DATES"
    git push
    echo "Pushed to remote."
else
    echo "No new or changed articles to commit."
    git reset HEAD -- . > /dev/null 2>&1
fi

sleep 4
