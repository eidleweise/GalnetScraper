@echo off
setlocal

:: Navigate to the script's directory
cd /d "%~dp0"

:: Activate virtual environment if it exists
if exist .venv\Scripts\activate.bat (
    call .venv\Scripts\activate.bat
)

echo Installing/Updating requirements...
pip install -r requirements.txt

echo Ensuring Playwright browsers are installed...
playwright install

echo Starting Galnet Scraper...
python galnetScraper.py

pause
