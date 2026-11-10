# Galnet Scraper

A tool to archive Galnet news articles from various sources, created because up-to-date RSS feeds for Elite Dangerous news have become difficult to find. For other users I've also added the galnet_news_full.json and zip file, mostly for posterity.

## Credits & Data Sources

This tool gathers data from the following excellent community resources and official channels:
* **Frontier Developments**: Official source of Galnet News.
* **Inara.cz**: For their comprehensive archive and tagging system.
* **Elite.Drinkybird.net**: Used as a fallback to recover missing article dates.

## Disclaimer & Usage

* **Personal Use Only**: This script is intended for personal archival and data visualization purposes.
* **Be Respectful**: The script includes delays and sequential processing to avoid putting unnecessary load on these services. Please use it responsibly.
* **No Warranty**: This script is provided "as is". While every effort is made to ensure it works correctly, the author is not responsible for any issues it may cause or for changes in source websites that might break the scraping logic.

## Setup

1. Run the scraper:
   ```bash
   ./runGalnetScraper.sh
   ```
   The launcher uses `uv` if it is installed. Otherwise, it creates and maintains a project-local `.venv`. It installs the Python requirements and Playwright's Chromium browser when needed.
2. On Windows, run `runGalnetScraper.bat`.
3. Use **Browse Articles** from the main menu to choose a source and inspect articles from its
   latest listing page. This mode previews scraped data without saving changes to the archive.
   Scraping, archive maintenance, and export/rebuild actions are grouped into their own submenus.
