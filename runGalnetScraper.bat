@echo off
setlocal

:: Navigate to the script's directory
cd /d "%~dp0"

where uv >nul 2>nul
if not errorlevel 1 goto use_uv

if exist .venv\Scripts\python.exe (
    .venv\Scripts\python.exe -m pip --version >nul 2>nul
    if not errorlevel 1 goto environment_ready
)

where py >nul 2>nul
if not errorlevel 1 (
    py -3 -m venv --clear .venv
) else (
    python -m venv --clear .venv
)
if errorlevel 1 exit /b 1

:environment_ready
.venv\Scripts\python.exe -m pip install -r requirements.txt
if errorlevel 1 exit /b 1

.venv\Scripts\python.exe -m playwright install chromium
if errorlevel 1 exit /b 1

echo Starting Galnet Scraper...
.venv\Scripts\python.exe galnet_scraper.py
set "EXIT_CODE=%ERRORLEVEL%"
goto finish

:use_uv
uv run --with-requirements requirements.txt python -m playwright install chromium
if errorlevel 1 exit /b 1

echo Starting Galnet Scraper...
uv run --with-requirements requirements.txt python galnet_scraper.py
set "EXIT_CODE=%ERRORLEVEL%"

:finish
if not "%EXIT_CODE%"=="0" echo Galnet Scraper failed with exit code %EXIT_CODE%.

pause
exit /b %EXIT_CODE%
