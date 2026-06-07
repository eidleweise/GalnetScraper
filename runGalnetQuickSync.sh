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

# Force-add the full archive files (they may be gitignored)
git add -f galnet_news_full.json galnet_news_full.7z > /dev/null 2>&1 || true

# Extract unique dates from newly staged JSON filenames (format: YYYY-MM-DD_slug.json)
DATES=$(git diff --cached --name-only -- 'GalnetNewsArchive/*.json' | sed 's|GalnetNewsArchive/||' | grep -oP '^\d{4}-\d{2}-\d{2}' | sort -u | paste -sd ', ')

# Check if galnet_news_full files are staged
FULL_CHANGED=$(git diff --cached --name-only -- 'galnet_news_full.json' 'galnet_news_full.7z')

if [ -n "$DATES" ] || [ -n "$FULL_CHANGED" ]; then
    if [ -n "$DATES" ]; then
        MSG="Committing galnet articles dated $DATES"
    else
        MSG="Committing galnet_news_full files"
    fi
    git commit -m "$MSG"
    echo "Committed: $MSG"
    git push
    echo "Pushed to remote."
else
    echo "No new or changed articles to commit."
    git reset HEAD -- . > /dev/null 2>&1
fi

sleep 4
