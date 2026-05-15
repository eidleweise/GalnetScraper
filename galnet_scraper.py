import json
import logging
import os
import random
import re
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple, Any, Callable

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, BrowserContext, TimeoutError as PlaywrightTimeoutError

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO, # Reverted to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

try:
    import py7zr
    HAS_PY7ZR = True
except ImportError:
    HAS_PY7ZR = False
    logger.warning("py7zr not installed. 7z archive creation will be unavailable.")


# --- Configuration ---
class Config:
    """
    Centralized configuration for the Galnet Scraper.
    Uses pathlib.Path for robust cross-platform path handling.
    """
    ARCHIVE_DIRECTORY = Path("GalnetNewsArchive")
    MASTER_JSON_FILE = Path("galnet_news_full.json")
    MASTER_7Z_ARCHIVE = Path("galnet_news_full.7z")
    USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
    GLOBAL_RATE_LIMIT_SECONDS = 0.5  # Seconds between requests to any external source

    # Priority for merging articles from different sources. Higher number means higher priority.
    SOURCE_PRIORITY = {"Frontier": 3, "Community": 2, "Inara": 1, "Drinkybird": 0, "unknown": -1}
    # Tolerance in days for considering articles with the same slug but different dates as the same article.
    DATE_MATCH_TOLERANCE_DAYS = 3


# --- Thread-safe Locks ---
# Lock to ensure only one external request is made at a time, respecting GLOBAL_RATE_LIMIT_SECONDS.
rate_limit_lock = threading.Lock()
# Lock to protect access to the community_hrefs list during initialization.
community_hrefs_lock = threading.Lock()
# Lock to protect access and modification of the global article index cache.
article_index_lock = threading.Lock()

# --- Global State Variables ---
last_request_timestamp = 0.0
community_hrefs: List[str] = []  # Cache for Community site's date-specific URLs.
# Global in-memory index of all archived articles: slug -> list of article dictionaries.
# Each article dict includes internal metadata like '_filepath' and '_date_obj'.
article_index_cache: Dict[str, List[Dict[str, Any]]] = {}


# --- Utility Functions ---

def slugify(input_text: str) -> str:
    """
    Converts input text into a file-safe slug by removing non-alphanumeric characters
    and replacing spaces with underscores.
    """
    slug_text = input_text.lower()
    slug_text = re.sub(r'[^a-z0-9]', '_', slug_text)
    return re.sub(r'_+', '_', slug_text).strip('_')


def parse_galnet_date(date_string: Optional[str]) -> Optional[datetime]:
    """
    Attempts to parse a Galnet date string in various formats and returns
    a datetime object. Handles the conversion from Elite Dangerous years
    to real-world years (Elite Year - 1286).

    Supported formats:
    - "DD Month YYYY" (e.g., "21 April 3308")
    - "DDth Month YYYY" (e.g., "21st April 3308")
    - "DD/MM/YYYY" (e.g., "21/04/3308")
    """
    if not date_string or date_string == "unknown_date":
        return None

    # Regex patterns to capture different date formats
    # Pattern 1: "DD Month YYYY" or "DDth Month YYYY"
    match_month_year = re.search(r'(\d{1,2})(?:st|nd|rd|th)?\s+([a-zA-Z]+)\s+(\d{4})', date_string)
    # Pattern 2: "DD/MM/YYYY"
    match_slash_date = re.search(r'(\d{1,2})/(\d{1,2})/(\d{4})', date_string)

    day_string, month_part, year_string = None, None, None
    is_slash_format = False

    if match_month_year:
        day_string = re.sub(r'(st|nd|rd|th)$', '', match_month_year.group(1)) # Extract day and remove ordinal
        month_part = match_month_year.group(2)
        year_string = match_month_year.group(3)
    elif match_slash_date:
        day_string, month_part, year_string = match_slash_date.groups()
        is_slash_format = True
    else:
        return None # No recognized pattern found

    try:
        year_integer = int(year_string)
        # Elite Dangerous years are 1286 years ahead of real-world years.
        # We assume years > 3000 are Elite years.
        # If user inputs a "real-world" year (e.g., 2026), we treat it as such.
        # If user inputs an "Elite" year (e.g., 3312), we convert it to its real-world equivalent.
        real_world_year = year_integer - 1286 if year_integer > 3000 else year_integer

        # Format real_world_year to always be 4 digits, padding with leading zeros if necessary
        formatted_real_world_year = f"{real_world_year:04d}"

        if is_slash_format:
            # For DD/MM/YYYY, month_part is already a number
            normalized_date_string = f"{day_string}/{month_part}/{formatted_real_world_year}"
            date_format_patterns = ["%d/%m/%Y"]
        else:
            # For DD Month YYYY, month_part is a month name
            normalized_date_string = f"{day_string} {month_part} {formatted_real_world_year}"
            date_format_patterns = ["%d %b %Y", "%d %B %Y"]

        for date_format_pattern in date_format_patterns:
            try:
                parsed_date = datetime.strptime(normalized_date_string, date_format_pattern)
                return parsed_date
            except ValueError:
                continue
    except Exception as e:
        logger.error(f"An unexpected error occurred during date parsing for '{date_string}': {e}")

    return None


def format_elite_date(date_object: Optional[datetime]) -> str:
    """
    Formats a datetime object into the Elite Dangerous Galnet format
    (e.g., '21 April 3312').
    """
    if date_object:
        return f"{date_object.day} {date_object.strftime('%B')} {date_object.year + 1286}"
    return "unknown_date"


def set_file_timestamps(file_path: Path, date_object: Optional[datetime]):
    """
    Sets the access and modified times of a file to match the article's
    publication date.
    """
    if date_object:
        try:
            timestamp_value = date_object.timestamp()
            os.utime(file_path, (timestamp_value, timestamp_value))
        except OSError as e:
            logger.warning(f"Could not set timestamp for {file_path}: {e}")


def load_article_index(force_reload: bool = False):
    """
    Populates the global in-memory article index by reading all JSON files
    in the archive directory. This avoids repeated disk I/O and O(N^2)
    directory listings during scraping and maintenance operations.
    """
    global article_index_cache
    if not Config.ARCHIVE_DIRECTORY.exists():
        logger.warning(f"Archive directory not found: {Config.ARCHIVE_DIRECTORY.absolute()}")
        return

    with article_index_lock:
        if article_index_cache and not force_reload:
            return

        # Use .clear() to maintain references in other modules that imported this dict
        article_index_cache.clear()

        for article_file_path in Config.ARCHIVE_DIRECTORY.glob("*.json"):
            try:
                with article_file_path.open('r', encoding='utf-8') as file_handle:
                    article_data = json.load(file_handle)
                article_slug = slugify(article_data.get('header', ''))
                if article_slug not in article_index_cache:
                    article_index_cache[article_slug] = []

                # Attach internal metadata for performance and easier access
                article_data['_filepath'] = article_file_path
                article_data['_date_obj'] = parse_galnet_date(article_data.get('article_date'))
                article_index_cache[article_slug].append(article_data)
            except (IOError, json.JSONDecodeError) as e:
                logger.warning(f"Could not load {article_file_path} into index: {e}")

        total_articles = sum(len(articles) for articles in article_index_cache.values())
        logger.debug(f"Index populated: {total_articles} articles across {len(article_index_cache)} unique slugs.")

        if article_index_cache:
            sample_size = min(len(article_index_cache), random.randint(5, 10))
            sample_keys = random.sample(list(article_index_cache.keys()), sample_size)
            logger.debug(f"Random sample of {sample_size} index entries:")
            for key in sample_keys:
                logger.debug(f"Slug: {key}\nArticles: {json.dumps(article_index_cache[key], indent=2, default=str)}")
        else:
            logger.warning("Index is empty.")




def enforce_global_rate_limit():
    """
    Ensures that a minimum amount of time (GLOBAL_RATE_LIMIT_SECONDS) has passed
    since the last external request to prevent overwhelming servers.
    """
    global last_request_timestamp
    with rate_limit_lock:
        current_time = time.time()
        elapsed_time = current_time - last_request_timestamp
        if elapsed_time < Config.GLOBAL_RATE_LIMIT_SECONDS:
            time.sleep(Config.GLOBAL_RATE_LIMIT_SECONDS - elapsed_time)
            last_request_timestamp = time.time()  # Update after sleep
        else:
            last_request_timestamp = current_time  # Update immediately if no sleep needed


# --- Core Logic Functions ---

def merge_article_data(new_article_data: Dict[str, Any], existing_article_data: Optional[Dict[str, Any]],
                       new_source_type: str) -> Dict[str, Any]:
    """
    Merges two versions of the same article, returning the 'best' combined version
    based on source priority. If no existing data, the new data is returned.
    """
    merged_article = {
        "header": new_article_data.get("header", ""),
        "body": new_article_data.get("body", ""),
        "article_date": new_article_data.get("article_date", "unknown_date"),
        "source": new_source_type,
        "scrape_date": datetime.now().isoformat(),
        "tags": list(new_article_data.get("tags", [])),
        "article_url": new_article_data.get("article_url", "")
    }

    if not existing_article_data:
        merged_article["tags"] = sorted(list(set(merged_article["tags"])))
        return merged_article

    existing_source_type = existing_article_data.get("source", "unknown")
    new_source_priority = Config.SOURCE_PRIORITY.get(new_source_type, -1)
    existing_source_priority = Config.SOURCE_PRIORITY.get(existing_source_type, -1)

    # Combine tags from both versions
    all_combined_tags = set(merged_article["tags"]) | set(existing_article_data.get("tags", []))

    if new_source_priority < existing_source_priority:
        # If the new source has lower priority, keep the existing content
        merged_article.update({
            "header": existing_article_data.get("header"),
            "body": existing_article_data.get("body"),
            "article_date": existing_article_data.get("article_date"),
            "source": existing_article_data.get("source"),
            "article_url": existing_article_data.get("article_url")
        })
    elif new_source_priority == existing_source_priority:
        # If sources have equal priority, prefer the version with more content or a later date
        merged_article["header"] = merged_article["header"] or existing_article_data.get("header", "")
        merged_article["body"] = merged_article["body"] or existing_article_data.get("body", "")
        merged_article["article_url"] = merged_article["article_url"] or existing_article_data.get("article_url", "")

        new_date_object = parse_galnet_date(merged_article["article_date"])
        existing_date_object = parse_galnet_date(existing_article_data.get("article_date"))

        if existing_date_object and (not new_date_object or existing_date_object > new_date_object):
            merged_article["article_date"] = existing_article_data.get("article_date")

    merged_article["tags"] = sorted(list(all_combined_tags))
    return merged_article


def save_article_to_archive(article_data: Dict[str, Any], source_type: str) -> bool:
    """
    Saves article data to an individual JSON file in the archive directory.
    Handles merging with existing duplicates based on source priority and date.
    Updates the in-memory article index.
    Returns True if a new or updated file was saved, False otherwise.
    """
    Config.ARCHIVE_DIRECTORY.mkdir(exist_ok=True)
    load_article_index()  # Ensure index is loaded before checking for existing articles

    article_header = article_data.get('header', 'no_header')
    article_slug = slugify(article_header)

    existing_best_match, existing_file_path = get_existing_article_from_index(article_header,
                                                                              article_data.get("article_date",
                                                                                               "unknown_date"))
    merged_article_data = merge_article_data(article_data, existing_best_match, source_type)

    # Check if the merged data is identical to the best existing data (to avoid unnecessary writes)
    if existing_best_match:
        is_content_identical = all(merged_article_data.get(key) == existing_best_match.get(key) for key in
                                   ["header", "body", "article_date", "source", "article_url"])
        if is_content_identical and set(merged_article_data["tags"]) == set(existing_best_match.get("tags", [])):
            final_date_object = parse_galnet_date(merged_article_data["article_date"])
            iso_date_prefix = final_date_object.strftime("%Y-%m-%d") if final_date_object else "unknown_date"
            final_target_path = Config.ARCHIVE_DIRECTORY / f"{iso_date_prefix}_{article_slug}.json"
            if final_target_path.exists() and final_target_path == existing_file_path:
                return False  # Article is identical and already correctly saved

    final_date_object = parse_galnet_date(merged_article_data["article_date"])
    iso_date_prefix = final_date_object.strftime("%Y-%m-%d") if final_date_object else "unknown_date"
    final_target_path = Config.ARCHIVE_DIRECTORY / f"{iso_date_prefix}_{article_slug}.json"

    # Clean up any old duplicate files if the final path has changed
    if existing_file_path and existing_file_path != final_target_path:
        try:
            existing_file_path.unlink(missing_ok=True)
            logger.info(f"Removed duplicate/lower priority file: {existing_file_path.name}")
        except OSError as e:
            logger.warning(f"Could not remove old file {existing_file_path.name}: {e}")

    try:
        with final_target_path.open('w', encoding='utf-8') as file_handle:
            json.dump(merged_article_data, file_handle, indent=4, ensure_ascii=False)
        set_file_timestamps(final_target_path, final_date_object)

        # Update the global in-memory index
        with article_index_lock:
            # Remove any entries pointing to the old file path for this slug
            if article_slug in article_index_cache:
                article_index_cache[article_slug] = [
                    article_entry for article_entry in article_index_cache[article_slug]
                    if article_entry.get('_filepath') != existing_file_path
                ]
            else:
                article_index_cache[article_slug] = []

            # Add/Update the current entry in the index
            index_entry = merged_article_data.copy()
            index_entry['_filepath'] = final_target_path
            index_entry['_date_obj'] = final_date_object
            article_index_cache[article_slug].append(index_entry)

        logger.info(f"Saved ({merged_article_data['source']}): {final_target_path.name}")
        return True
    except (IOError, json.JSONDecodeError) as error:
        logger.error(f"Error saving {final_target_path.name}: {error}")
        return False


def get_existing_article_from_index(article_header: str, new_article_date_string: str) -> Tuple[
    Optional[Dict[str, Any]], Optional[Path]]:
    """
    Checks if an article with the given header already exists in the local archive index,
    and returns the "best" version based on source priority and date, considering
    a date tolerance for articles with the same slug.
    """
    load_article_index()
    article_slug = slugify(article_header)
    new_article_date_object = parse_galnet_date(new_article_date_string)

    best_matching_data = None
    best_matching_filepath = None
    highest_priority_found = -2  # Lower than any actual priority

    with article_index_lock:
        candidate_articles = article_index_cache.get(article_slug, [])

        matched_candidates = []
        for current_article_data in candidate_articles:
            current_article_date_object = current_article_data.get("_date_obj")

            # Check date proximity
            is_date_match = False
            if new_article_date_object and current_article_date_object:
                date_difference_days = abs((new_article_date_object - current_article_date_object).days)
                if date_difference_days <= Config.DATE_MATCH_TOLERANCE_DAYS:
                    is_date_match = True
            elif not new_article_date_object or not current_article_date_object:
                # If either date is unknown, consider it a match for merging purposes
                is_date_match = True

            if is_date_match:
                matched_candidates.append(current_article_data)

        for current_article_data in matched_candidates:
            current_source_type = current_article_data.get("source", "unknown")
            current_source_priority = Config.SOURCE_PRIORITY.get(current_source_type, -1)
            current_article_date_object = current_article_data.get("_date_obj")

            if current_source_priority > highest_priority_found:
                highest_priority_found = current_source_priority
                best_matching_data = current_article_data
                best_matching_filepath = current_article_data.get("_filepath")
            elif current_source_priority == highest_priority_found:
                # If priorities are equal, prefer the article with a later date
                best_date_object_found = best_matching_data.get("_date_obj") if best_matching_data else None

                if current_article_date_object and (
                        not best_date_object_found or current_article_date_object > best_date_object_found):
                    best_matching_data = current_article_data
                    best_matching_filepath = current_article_data.get("_filepath")

    return best_matching_data, best_matching_filepath


# --- Scraper Helper Functions ---

def playwright_browser_session(scrape_function: Callable[..., Any]):
    """
    Decorator to manage the Playwright browser lifecycle for scraping functions.
    It launches a browser, creates a context, passes it to the decorated function,
    and ensures the browser is closed afterwards.
    If a browser_context is already provided, it uses that instead.
    """

    def wrapper(*args, **kwargs):
        browser_context = kwargs.get('browser_context')
        if browser_context:
            # If context is already provided (e.g., for bulk operations), just call the function
            return scrape_function(*args, **kwargs)

        # Otherwise, manage a new Playwright session
        with sync_playwright() as playwright_instance:
            browser = playwright_instance.chromium.launch(headless=True)
            context = browser.new_context(user_agent=Config.USER_AGENT)
            try:
                kwargs['browser_context'] = context
                return scrape_function(*args, **kwargs)
            finally:
                browser.close()

    return wrapper


def fetch_drinkybird_date(article_header: str, browser_context: BrowserContext) -> str:
    """
    Scrapes elite.drinkybird.net to find the publication date for an article
    header when it is missing from other sources.
    """
    enforce_global_rate_limit()
    logger.info(f"Searching Drinkybird for missing date: '{article_header}'")
    try:
        current_year = datetime.now().year + 5  # Search ahead into the future
        encoded_title = urllib.parse.quote(article_header)
        search_url = f"https://elite.drinkybird.net/?title={encoded_title}&text=&from=2014-01-01&to={current_year}-01-01"
        page = browser_context.new_page()
        page.goto(search_url, timeout=30000)
        soup = BeautifulSoup(page.content(), 'html.parser')
        date_element = soup.find('p', class_='article-date')
        page.close()
        if date_element:
            found_date_string = date_element.get_text(strip=True)
            logger.info(f"Drinkybird found date: {found_date_string}")
            return found_date_string
    except Exception as error:
        logger.error(f"Drinkybird search failed for '{article_header}': {error}")
    return "unknown_date"


# --- Scraper Implementations ---

@playwright_browser_session
def scrape_inara_page(page_number: int, browser_context: BrowserContext = None) -> Tuple[int, int]:
    """
    Scrapes a single page of Galnet news from Inara.cz.
    Returns (found_count, processed_count).
    """
    enforce_global_rate_limit()
    target_url = f"https://inara.cz/elite/galnet/?page={page_number}"
    logger.info(f"Scanning Inara page {page_number} ({target_url})...")
    page = browser_context.new_page()
    processed_article_count = 0
    found_article_count = 0
    try:
        page.goto(target_url, timeout=60000)
        challenge_button = page.locator("#challenge")
        if challenge_button.is_visible():
            logger.info(f"Inara challenge detected on page {page_number}. Attempting to click.")
            challenge_button.click()
            try:
                page.wait_for_selector(".mainblock", timeout=15000)
            except PlaywrightTimeoutError:
                logger.warning(
                    f"Timeout waiting for .mainblock after challenge on Inara page {page_number}. Page might be empty or slow.")
            except Exception as e:
                logger.error(f"Error interacting with Inara challenge on page {page_number}: {e}")

        soup = BeautifulSoup(page.content(), 'html.parser')
        article_elements = soup.find_all('div', class_='mainblock')

        for article_element in article_elements:
            header_element = article_element.find('h2')
            if not header_element: continue
            
            # Found a valid article header
            found_article_count += 1
            
            article_header_text = header_element.get_text(strip=True)
            date_element = article_element.find('span', class_='date') or article_element.find('span',
                                                                                               class_='text-muted')
            article_date_string = date_element.get_text(strip=True) if date_element else "unknown_date"

            if article_date_string == "unknown_date":
                # Attempt to find date locally or via Drinkybird if missing
                article_date_string = find_date_locally(article_header_text) or fetch_drinkybird_date(
                    article_header_text, browser_context)

            body_element = article_element.find('article')
            article_body_text = ""
            if body_element:
                article_body_text = "\n\n".join([
                    paragraph.get_text(strip=True) for paragraph in body_element.find_all('p')
                    if paragraph.get_text(strip=True)  # Ensure paragraph is not empty
                ])

            article_data = {
                "header": article_header_text,
                "body": article_body_text,
                "article_date": article_date_string,
                "tags": [tag.get_text(strip=True) for tag in article_element.find_all('a', class_=['tag', 'inaratag'])]
            }
            if save_article_to_archive(article_data, "Inara"):
                processed_article_count += 1

        return found_article_count, processed_article_count
    except Exception as e:
        logger.error(f"Error scraping Inara page {page_number}: {e}")
        return 0, 0
    finally:
        page.close()


@playwright_browser_session
def scrape_frontier_page(page_number: int, browser_context: BrowserContext = None) -> Tuple[int, int]:
    """
    Scrapes a single page of Galnet news from the official Elite Dangerous news site.
    Returns (found_count, processed_count).
    """
    enforce_global_rate_limit()
    target_url = f"https://www.elitedangerous.com/news/galnet?page={page_number}"
    logger.info(f"Scanning Frontier page {page_number} ({target_url})...")
    page = browser_context.new_page()
    processed_article_count = 0
    found_article_count = 0
    try:
        page.goto(target_url, timeout=60000)
        try:
            page.wait_for_selector("article.o-news-article", timeout=30000)
        except PlaywrightTimeoutError:
            logger.warning(f"Timeout waiting for articles on Frontier page {page_number}. Page might be empty or slow.")
            return 0, 0
        except Exception as e:
            logger.error(f"Error waiting for Frontier articles on page {page_number}: {e}")
            return 0, 0

        soup = BeautifulSoup(page.content(), 'html.parser')
        article_elements = soup.find_all('article', class_='o-news-article')

        for article_element in article_elements:
            header_element = article_element.find('h3')
            if not header_element: continue
            
            # Found a valid article header
            found_article_count += 1
            
            article_header_text = header_element.get_text(strip=True)
            date_element = article_element.find('time', class_='datetime')
            article_date_string = date_element.get_text(strip=True) if date_element else "unknown_date"

            if article_date_string == "unknown_date":
                article_date_string = find_date_locally(article_header_text) or fetch_drinkybird_date(
                    article_header_text, browser_context)

            existing_best_match, _ = get_existing_article_from_index(article_header_text, article_date_string)

            article_link_element = article_element.find('a', href=True)
            article_full_url = urllib.parse.urljoin("https://www.elitedangerous.com", article_link_element[
                'href']) if article_link_element else ""
            article_body_text = ""
            is_body_scrape_successful = False

            if article_full_url:
                article_detail_page = browser_context.new_page()
                enforce_global_rate_limit()
                try:
                    article_detail_page.goto(article_full_url, timeout=60000)
                    article_detail_page.wait_for_selector(".v-galnet-details__main-body", timeout=15000)
                    article_detail_soup = BeautifulSoup(article_detail_page.content(), 'html.parser')
                    body_content_element = article_detail_soup.select_one(".v-galnet-details__main-body")
                    if body_content_element:
                        extracted_body = "\n\n".join([
                            paragraph.get_text(strip=True) for paragraph in body_content_element.find_all('p')
                            if paragraph.get_text(strip=True)
                        ])
                        if extracted_body:  # Only consider successful if content is not empty
                            article_body_text = extracted_body
                            is_body_scrape_successful = True
                except Exception as e:
                    logger.warning(f"Failed body scrape for Frontier article {article_full_url}: {e}")
                finally:
                    article_detail_page.close()

            should_save_frontier_version = True
            if not is_body_scrape_successful and existing_best_match and existing_best_match.get("body"):
                # If Frontier failed to get a body, but an existing (lower priority) article has one,
                # we should NOT overwrite it with the empty Frontier body.
                should_save_frontier_version = False
                logger.info(
                    f"Skipping Frontier article '{article_header_text}' due to empty body, existing article has content.")

            if should_save_frontier_version:
                article_data = {
                    "header": article_header_text,
                    "body": article_body_text,
                    "article_date": article_date_string,
                    "article_url": article_full_url
                }
                if save_article_to_archive(article_data, "Frontier"):
                    processed_article_count += 1
        return found_article_count, processed_article_count
    except Exception as e:
        logger.error(f"Error scraping Frontier page {page_number}: {e}")
        return 0, 0
    finally:
        page.close()


@playwright_browser_session
def scrape_community_page(page_index: int, browser_context: BrowserContext = None) -> Tuple[int, int]:
    """
    Scrapes a specific date's articles from the Elite Dangerous Community site.
    The page_index refers to an index in the `community_hrefs` list, not a page number.
    Returns (found_count, processed_count).
    """
    global community_hrefs
    found_article_count = 0
    processed_article_count = 0

    with community_hrefs_lock:
        if not community_hrefs:
            enforce_global_rate_limit()
            logger.info("Fetching Community Galnet index of date links...")
            index_page = browser_context.new_page()
            try:
                index_page.goto("https://community.elitedangerous.com/galnet", timeout=60000)
                more_filters_button = index_page.locator("a[onclick*='show_extra_filters']")
                if more_filters_button.is_visible():
                    logger.info("Clicking 'Show More Filters' on Community site.")
                    more_filters_button.click()
                    try:
                        index_page.wait_for_selector("a.galnetLinkBoxLink", timeout=15000)
                    except PlaywrightTimeoutError:
                        logger.warning(f"Timeout waiting for date links on Community site after clicking filters.")
                    except Exception as e:
                        logger.error(f"Error interacting with Community filters: {e}")

                soup = BeautifulSoup(index_page.content(), 'html.parser')
                date_link_elements = soup.find_all('a', class_='galnetLinkBoxLink')
                community_hrefs = [urllib.parse.urljoin("https://community.elitedangerous.com", link_element['href'])
                                   for link_element in date_link_elements if 'href' in link_element.attrs]
                logger.info(f"Found {len(community_hrefs)} date links on Community site.")
            except Exception as e:
                logger.error(f"Failed to fetch Community date link index: {e}")
                return 0, 0
            finally:
                index_page.close()

    if page_index >= len(community_hrefs):
        return 0, 0  # No more date links to process

    target_article_list_url = community_hrefs[page_index]
    logger.info(f"Scanning Community Galnet (Index {page_index}): {target_article_list_url}...")
    page = browser_context.new_page()
    try:
        page.goto(target_article_list_url, timeout=60000)
        soup = BeautifulSoup(page.content(), 'html.parser')
        article_elements = soup.find_all('div', class_='article')

        for article_element in article_elements:
            header_element = article_element.find('h3', class_='galnetNewsArticleTitle')
            if not header_element: continue
            
            # Found a valid article header
            found_article_count += 1
            
            article_header_text = header_element.get_text(strip=True)
            date_element = article_element.find('p', class_='small')
            article_date_string = date_element.get_text(strip=True) if date_element else "unknown_date"

            if article_date_string == "unknown_date":
                article_date_string = find_date_locally(article_header_text) or fetch_drinkybird_date(
                    article_header_text, browser_context)

            existing_best_match, _ = get_existing_article_from_index(article_header_text, article_date_string)
            if not existing_best_match or Config.SOURCE_PRIORITY.get(existing_best_match.get("source"), -1) < \
                    Config.SOURCE_PRIORITY["Community"]:
                # Replace <br> tags with newlines for better text extraction
                for br_tag in article_element.find_all("br"):
                    br_tag.replace_with("\n")

                body_paragraphs = [
                    paragraph.get_text() for paragraph in article_element.find_all('p')
                    if 'small' not in paragraph.get('class', []) and paragraph.get_text().strip()
                ]
                article_body_text = "\n\n".join(body_paragraphs)

                article_link_element = header_element.find('a', href=True)
                article_full_url = urllib.parse.urljoin("https://community.elitedangerous.com", article_link_element[
                    'href']) if article_link_element else target_article_list_url

                article_data = {"header": article_header_text, "body": article_body_text,
                                "article_date": article_date_string, "article_url": article_full_url}
                if save_article_to_archive(article_data, "Community"):
                    processed_article_count += 1
        return found_article_count, processed_article_count
    except Exception as e:
        logger.error(f"Error scraping Community page (URL: {target_article_list_url}): {e}")
        return 0, 0
    finally:
        page.close()


def sync_all_sources():
    """
    Fetches new articles from Frontier, Community, and Inara sources.
    Maintains source preference: Frontier > Community > Inara.
    Starting with the highest priority minimizes redundant disk writes.
    This function stops when a source returns a page with no *newly processed* articles,
    indicating that it has caught up with the latest content.
    """
    logger.info("=== Fetching New Galnet Articles from All Sources (Quick Update) ===")
    load_article_index(force_reload=True)  # Ensure index is fresh before starting sync
    with sync_playwright() as playwright_instance:
        browser = playwright_instance.chromium.launch(headless=True)
        browser_context = browser.new_context(user_agent=Config.USER_AGENT)
        try:
            # Scrape Frontier
            logger.info("\n--- Checking Frontier ---")
            page_number = 1
            total_processed_frontier = 0
            while True:
                found, processed = scrape_frontier_page(page_number, browser_context=browser_context)
                total_processed_frontier += processed
                if processed == 0 and found == 0: # Stop if no articles found on page
                    logger.info(f"End of Frontier site reached at page {page_number - 1}. Total processed: {total_processed_frontier}")
                    break
                elif processed == 0 and found > 0: # Stop if articles found but none were new/updated
                    logger.info(f"Frontier: No new or updated articles found on page {page_number}. Assuming caught up. Total processed: {total_processed_frontier}")
                    break
                page_number += 1

            # Scrape Community
            logger.info("\n--- Checking Community ---")
            page_index = 0
            total_processed_community = 0
            while True:
                found, processed = scrape_community_page(page_index, browser_context=browser_context)
                total_processed_community += processed
                if processed == 0 and found == 0: # Stop if no articles found on page
                    logger.info(f"End of Community site reached at index {page_index - 1}. Total processed: {total_processed_community}")
                    break
                elif processed == 0 and found > 0: # Stop if articles found but none were new/updated
                    logger.info(f"Community: No new or updated articles found on index {page_index}. Assuming caught up. Total processed: {total_processed_community}")
                    break
                page_index += 1

            # Scrape Inara
            logger.info("\n--- Checking Inara ---")
            page_number = 1
            total_processed_inara = 0
            while True:
                found, processed = scrape_inara_page(page_number, browser_context=browser_context)
                total_processed_inara += processed
                if processed == 0 and found == 0: # Stop if no articles found on page
                    logger.info(f"End of Inara site reached at page {page_number - 1}. Total processed: {total_processed_inara}")
                    break
                elif processed == 0 and found > 0: # Stop if articles found but none were new/updated
                    logger.info(f"Inara: No new or updated articles found on page {page_number}. Assuming caught up. Total processed: {total_processed_inara}")
                    break
                page_number += 1
        finally:
            browser.close()
    combine_json_files()
    logger.info("\n--- Sync Complete ---")


def force_sync_all_articles():
    """
    Performs a full sweep of all sources using threading for maximum speed.
    This bypasses the 'quick sync' checks and re-scans a large range of history.
    """
    logger.info("=== FORCE SYNC: Full Historical Sweep of All Sources ===")
    load_article_index(force_reload=True)
    
    with sync_playwright() as playwright_instance:
        browser = playwright_instance.chromium.launch(headless=True)
        browser_context = browser.new_context(user_agent=Config.USER_AGENT)
        try:
            # 1. Frontier (Typically ~100 pages, setting to 150 for safety)
            logger.info("\n--- Force Checking Frontier (Threaded) ---")
            with ThreadPoolExecutor(max_workers=5) as executor:
                list(executor.map(lambda idx: scrape_frontier_page(idx, browser_context=browser_context), range(1, 151)))

            # 2. Community (Typically ~800+ indexes, setting to 1001 for safety)
            logger.info("\n--- Force Checking Community (Threaded) ---")
            # We first need to initialize the community_hrefs cache
            scrape_community_page(0, browser_context=browser_context) 
            with ThreadPoolExecutor(max_workers=5) as executor:
                list(executor.map(lambda idx: scrape_community_page(idx, browser_context=browser_context), range(0, 1001)))

            # 3. Inara (Typically ~300 pages, setting to 401 for safety)
            logger.info("\n--- Force Checking Inara (Threaded) ---")
            with ThreadPoolExecutor(max_workers=5) as executor:
                list(executor.map(lambda idx: scrape_inara_page(idx, browser_context=browser_context), range(1, 401)))
                
        finally:
            browser.close()
            
    combine_json_files()
    logger.info("\n=== Force Sync Complete ===")


# --- Maintenance Functions ---

def combine_json_files(output_file_path: Path = Config.MASTER_JSON_FILE, create_7z_archive: bool = True):
    """
    Rebuilds the master JSON file from the individual archive index,
    ensuring correct chronological order and uniqueness.
    Optionally creates a 7-zip archive of the result.
    """
    load_article_index()
    if not article_index_cache:
        logger.info("No articles in archive to combine.")
        return

    logger.info(f"Rebuilding master JSON file: {output_file_path}...")

    # Flatten index and use pre-calculated date objects for fast sorting
    all_articles_for_sorting = []
    with article_index_lock:
        for article_list_for_slug in article_index_cache.values():
            for article_entry in article_list_for_slug:
                # Remove internal metadata before exporting to the final JSON file
                clean_article_data = {key: value for key, value in article_entry.items() if not key.startswith('_')}
                all_articles_for_sorting.append((clean_article_data, article_entry.get('_date_obj')))

    # Optimized sort using pre-cached date objects
    all_articles_for_sorting.sort(key=lambda item: item[1].timestamp() if item[1] else 0, reverse=True)
    final_sorted_article_list = [item[0] for item in all_articles_for_sorting]

    try:
        with output_file_path.open('w', encoding='utf-8') as file_handle:
            json.dump(final_sorted_article_list, file_handle, indent=4, ensure_ascii=False)
        logger.info(f"Combined {len(final_sorted_article_list)} articles into {output_file_path}")
    except (IOError, json.JSONDecodeError) as e:
        logger.error(f"Failed to write master JSON to {output_file_path}: {e}")
        return

    if create_7z_archive and HAS_PY7ZR:
        logger.info(f"Creating 7z archive: {Config.MASTER_7Z_ARCHIVE}...")
        try:
            with py7zr.SevenZipFile(Config.MASTER_7Z_ARCHIVE, 'w') as archive_handle:
                archive_handle.write(output_file_path, arcname=output_file_path.name)
            logger.info(f"Successfully archived to {Config.MASTER_7Z_ARCHIVE}")
        except Exception as e:  # py7zr can raise various exceptions
            logger.error(f"Error creating 7z archive {Config.MASTER_7Z_ARCHIVE}: {e}")


def find_date_locally(article_header: str) -> Optional[str]:
    """
    Searches the local archive index for an existing article with the same header
    to retrieve a previously found date string.
    """
    load_article_index()
    article_slug = slugify(article_header)
    with article_index_lock:
        candidate_articles = article_index_cache.get(article_slug, [])
        for article_entry in candidate_articles:
            if article_entry.get('article_date') != "unknown_date":
                return article_entry['article_date']
    return None


def remove_duplicate_articles():
    """
    Scans the local archive index for duplicate articles and merges them.
    Duplicates are identified by header slug and date proximity.
    """
    load_article_index(force_reload=True)
    if not article_index_cache:
        logger.info("No articles in archive to deduplicate.")
        return
    logger.info("\n--- Removing Duplicate Articles ---")

    with article_index_lock:
        # Work on a copy of the slugs to safely modify the index via save_article_to_archive
        all_slugs = list(article_index_cache.keys())

    for article_slug in all_slugs:
        with article_index_lock:
            articles_for_slug = article_index_cache.get(article_slug, [])
        if len(articles_for_slug) < 2:
            continue  # No duplicates for this slug

        # Sort entries by date to group them chronologically
        articles_for_slug.sort(key=lambda x: x['_date_obj'].timestamp() if x['_date_obj'] else 0)

        # Cluster articles that are close in date
        date_clusters = []
        if articles_for_slug:
            current_cluster = [articles_for_slug[0]]
            for i in range(1, len(articles_for_slug)):
                previous_date_object = articles_for_slug[i - 1]['_date_obj']
                current_date_object = articles_for_slug[i]['_date_obj']

                if previous_date_object and current_date_object and \
                        abs((current_date_object - previous_date_object).days) <= Config.DATE_MATCH_TOLERANCE_DAYS:
                    current_cluster.append(articles_for_slug[i])
                elif not previous_date_object and not current_date_object:
                    # Both dates unknown, group them
                    current_cluster.append(articles_for_slug[i])
                else:
                    date_clusters.append(current_cluster)
                    current_cluster = [articles_for_slug[i]]
            date_clusters.append(current_cluster)  # Add the last cluster

        for cluster_of_articles in date_clusters:
            if len(cluster_of_articles) < 2:
                continue  # No duplicates within this cluster

            best_entry_in_cluster = None
            highest_priority_in_cluster = -2
            all_cluster_tags = set()

            for entry_in_cluster in cluster_of_articles:
                all_cluster_tags.update(entry_in_cluster.get('tags', []))
                current_priority = Config.SOURCE_PRIORITY.get(entry_in_cluster.get('source', 'unknown'), -1)

                if current_priority > highest_priority_in_cluster:
                    highest_priority_in_cluster = current_priority
                    best_entry_in_cluster = entry_in_cluster
                elif current_priority == highest_priority_in_cluster:
                    # If priorities are equal, prefer the article with a later date
                    best_date_object_in_cluster = best_entry_in_cluster.get(
                        "_date_obj") if best_entry_in_cluster else None
                    if entry_in_cluster['_date_obj'] and \
                            (not best_date_object_in_cluster or entry_in_cluster[
                                '_date_obj'] > best_date_object_in_cluster):
                        best_entry_in_cluster = entry_in_cluster

            # Clean up all redundant files in the cluster first
            for entry in cluster_of_articles:
                if entry != best_entry_in_cluster:
                    redundant_path = entry.get('_filepath')
                    if redundant_path and redundant_path.exists():
                        try:
                            redundant_path.unlink()
                            logger.info(f"Removed redundant duplicate: {redundant_path.name}")
                        except OSError as e:
                            logger.warning(f"Could not remove redundant file {redundant_path}: {e}")

            # Use save_article_to_archive to handle the merge and index update.
            merged_data_for_save = {key: value for key, value in best_entry_in_cluster.items() if
                                    not key.startswith('_')}
            merged_data_for_save['tags'] = sorted(list(all_cluster_tags))

            save_article_to_archive(merged_data_for_save, merged_data_for_save.get('source', 'unknown'))

    combine_json_files()  # Rebuild master JSON after deduplication


def perform_maintenance_task(task_type: str, browser_context: BrowserContext = None):
    """
    Performs various maintenance tasks on the archive files:
    - 'normalize': Ensures dates are in the standard Elite format.
    - 'rename': Renames files to follow the 'YYYY-MM-DD_slug.json' format.
    - 'remove_duplicates': Merges and removes redundant articles.
    - 'unknown_date': Attempts to find missing dates using Drinkybird.
    """
    load_article_index(force_reload=True)
    if task_type == "remove_duplicates":
        return remove_duplicate_articles()

    with article_index_lock:
        all_article_entries = [article_entry for article_list_for_slug in article_index_cache.values() for article_entry
                               in article_list_for_slug]

    for article_entry in all_article_entries:
        article_file_path = article_entry.get('_filepath')
        article_date_object = article_entry.get('_date_obj')

        if task_type == "unknown_date":
            if article_entry.get('article_date') == "unknown_date":
                article_header = article_entry.get('header')
                if article_header and browser_context:
                    new_found_date = fetch_drinkybird_date(article_header, browser_context)
                    if new_found_date != "unknown_date":
                        article_entry['article_date'] = new_found_date
                        # Create a clean dict for save_article_to_archive to avoid internal keys
                        clean_article_data = {key: value for key, value in article_entry.items() if
                                              not key.startswith('_')}
                        save_article_to_archive(clean_article_data, article_entry.get('source', 'Drinkybird'))
            continue  # Move to next article after attempting to fix date

        # For 'normalize' and 'rename' tasks, we need a valid date and file path
        if not article_date_object or not article_file_path:
            continue

        try:
            if task_type == "normalize":
                normalized_date_string = format_elite_date(article_date_object)
                if article_entry.get('article_date') != normalized_date_string:
                    article_entry['article_date'] = normalized_date_string
                    with article_file_path.open('w', encoding='utf-8') as file_handle:
                        # Strip internal metadata before saving back to file
                        clean_article_data = {key: value for key, value in article_entry.items() if
                                              not key.startswith('_')}
                        json.dump(clean_article_data, file_handle, indent=4, ensure_ascii=False)
                    logger.info(f"Normalized date for {article_file_path.name}")

            elif task_type == "rename":
                iso_date_prefix = article_date_object.strftime("%Y-%m-%d")
                new_file_name = f"{iso_date_prefix}_{slugify(article_entry.get('header', '')).replace('.', '')}.json"
                target_file_path = Config.ARCHIVE_DIRECTORY / new_file_name
                if article_file_path.name != new_file_name:
                    if target_file_path.exists():
                        article_file_path.unlink()  # Delete old file if new one already exists (e.g., due to previous rename attempt)
                        logger.info(
                            f"Removed old file {article_file_path.name} as new one exists at {target_file_path.name}")
                    else:
                        article_file_path.rename(target_file_path)
                        logger.info(f"Renamed {article_file_path.name} to {target_file_path.name}")
                        article_entry['_filepath'] = target_file_path  # Update the index entry's path
                set_file_timestamps(article_entry['_filepath'], article_date_object)
        except Exception as e:
            logger.error(f"Maintenance task '{task_type}' failed for {article_file_path}: {e}")

    load_article_index(force_reload=True)  # Re-load index after renames to ensure paths are correct
    combine_json_files()  # Rebuild master JSON after maintenance


# --- Command Line Interface (CLI) ---

@playwright_browser_session
def fix_unknown_dates_cli_handler(browser_context: BrowserContext = None):
    """CLI handler for fixing unknown dates."""
    perform_maintenance_task("unknown_date", browser_context=browser_context)


def execute_menu_choice(user_choice: str):
    """
    Executes a single menu action by choice number.
    Returns True if the choice was valid and executed, False otherwise.
    """
    if user_choice == '1':
        sync_all_sources()
    elif user_choice == '2':
        force_sync_all_articles()
    elif user_choice == '3':
        try:
            start_index = int(input("Enter Start Page (default 1): ") or 1)
            end_index = int(input("Enter End Page (default 10): ") or 10)

            load_article_index(force_reload=True)
            with sync_playwright() as playwright_instance:
                browser = playwright_instance.chromium.launch(headless=True)
                browser_context = browser.new_context(user_agent=Config.USER_AGENT)
                try:
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        list(executor.map(lambda idx: scrape_frontier_page(idx, browser_context=browser_context),
                                          range(start_index, end_index + 1)))
                finally:
                    browser.close()
            combine_json_files()
        except ValueError:
            logger.error("Invalid input. Please enter integer values for page ranges.")
        except Exception as ex:
            logger.error(f"An unexpected error occurred during bulk scraping: {ex}")
    elif user_choice == '4':
        try:
            start_index = int(input("Enter Start Index (default 0): ") or 0)
            end_index = int(input("Enter End Index (default 10): ") or 10)

            load_article_index(force_reload=True)
            with sync_playwright() as playwright_instance:
                browser = playwright_instance.chromium.launch(headless=True)
                browser_context = browser.new_context(user_agent=Config.USER_AGENT)
                try:
                    scrape_community_page(0, browser_context=browser_context)
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        list(executor.map(lambda idx: scrape_community_page(idx, browser_context=browser_context),
                                          range(start_index, end_index + 1)))
                finally:
                    browser.close()
            combine_json_files()
        except ValueError:
            logger.error("Invalid input. Please enter integer values for index ranges.")
        except Exception as ex:
            logger.error(f"An unexpected error occurred during bulk scraping: {ex}")
    elif user_choice == '5':
        try:
            start_index = int(input("Enter Start Page (default 1): ") or 1)
            end_index = int(input("Enter End Page (default 10): ") or 10)

            load_article_index(force_reload=True)
            with sync_playwright() as playwright_instance:
                browser = playwright_instance.chromium.launch(headless=True)
                browser_context = browser.new_context(user_agent=Config.USER_AGENT)
                try:
                    with ThreadPoolExecutor(max_workers=5) as executor:
                        list(executor.map(lambda idx: scrape_inara_page(idx, browser_context=browser_context),
                                          range(start_index, end_index + 1)))
                finally:
                    browser.close()
            combine_json_files()
        except ValueError:
            logger.error("Invalid input. Please enter integer values for page ranges.")
        except Exception as ex:
            logger.error(f"An unexpected error occurred during bulk scraping: {ex}")
    elif user_choice == '6':
        perform_maintenance_task("rename")
    elif user_choice == '7':
        fix_unknown_dates_cli_handler()
    elif user_choice == '8':
        create_7z_input = input("Create 7z archive? (Y/n): ").strip().lower()
        should_create_7z = create_7z_input != 'n'
        combine_json_files(create_7z_archive=should_create_7z)
    elif user_choice == '9':
        perform_maintenance_task("normalize")
    elif user_choice == '10':
        perform_maintenance_task("remove_duplicates")
    else:
        return False
    return True


def main_menu():
    """
    The main interactive menu for the Galnet Scraper tool.
    """
    while True:
        print("\n--- Galnet Scraper Menu ---")
        print("1. Sync New Articles (Quick Update)")
        print("2. Force Sync All Articles (Full Historical Sweep)")
        print("3. Bulk Scrape Frontier Galnet (Page Range)")
        print("4. Bulk Scrape Community Galnet (Index Range)")
        print("5. Bulk Scrape Inara Galnet (Page Range)")
        print("6. Rename Files & Sync Timestamps")
        print("7. Fix Unknown Date Files (Search Drinkybird)")
        print("8. Rebuild Master JSON & 7z Archive")
        print("9. Normalize Dates (Elite Format)")
        print("10. Remove Duplicate Articles")
        print("0. Quit")

        user_choice = input("\nEnter your choice: ").strip()
        if user_choice == '0':
            break

        if not execute_menu_choice(user_choice):
            print("Invalid choice. Please try again.")


if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1:
        choice = sys.argv[1].strip()
        logger.info(f"Running menu option {choice} from command line argument...")
        if not execute_menu_choice(choice):
            logger.error(f"Invalid menu option: {choice}")
            sys.exit(1)
        logger.info("Done. Exiting in 5 seconds...")
        time.sleep(5)
    else:
        main_menu()