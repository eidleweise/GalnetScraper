import json
import re
import time
import os
import threading
import urllib.parse
import random
import numpy as np
from PIL import Image
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

try:
    from wordcloud import WordCloud, STOPWORDS
    import matplotlib.pyplot as plt
    HAS_WORDCLOUD = True
except ImportError:
    HAS_WORDCLOUD = False

try:
    import py7zr
    HAS_PY7ZR = True
except ImportError:
    HAS_PY7ZR = False

# --- Configuration ---
ARCHIVE_DIR = "GalnetNewsArchive"
MASTER_JSON = "galnet_news_full.json"
MASTER_7Z = "galnet_news_full.7z"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"
GLOBAL_RATE_LIMIT = 1.0  # Seconds between requests to any external source

# List of paths to .ttf or .otf font files. 
FONT_PATHS = [
    "~/.local/share/fonts/n/NanamiPro_Normal.otf",
    "~/.local/share/fonts/e/EUROCAPS.ttf",
    "EuroStyle_Normal.ttf",
    "~/.local/share/fonts/p/Protomolecule.otf",
    "~/.local/share/fonts/g/Garde.ttf",
    "~/.local/share/fonts/b/b5.ttf"
]

# Path to an image file to use as a mask (e.g. "mask.png")
MASK_PATH = "lave_radio_wordcloud_mask.png"

# Locks for thread-safe operations
log_lock = threading.Lock()
rate_limit_lock = threading.Lock()
community_lock = threading.Lock()

# Rate limiting state
last_request_time = 0
community_hrefs = []

# --- Utilities ---

def slugify(text):
    """
    Convert text into a file-safe slug by removing non-alphanumeric characters
    and replacing spaces with underscores.
    """
    text = text.lower()
    text = re.sub(r'[^a-z0-9]', '_', text)
    return re.sub(r'_+', '_', text).strip('_')

def parse_date(date_str):
    """
    Attempts to parse Galnet date format (e.g., '21 April 3308') and returns
    a datetime object. Handles the conversion from Elite Dangerous years
    to real-world years (Elite Year - 1286).
    """
    if not date_str or date_str == "unknown_date":
        return None

    match = re.search(r'(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})', date_str)
    if match:
        day_str, month_str, year_str = match.groups()
        year_int = int(year_str)
        # Elite Year -> Real Year conversion if needed
        real_year = year_int - 1286 if year_int > 3000 else year_int
        normalized_str = f"{day_str} {month_str} {real_year}"

        for date_format in ("%d %b %Y", "%d %B %Y"):
            try:
                return datetime.strptime(normalized_str, date_format)
            except ValueError:
                continue
    return None

def format_date_elite(date_object):
    """
    Formats a datetime object into the Elite Dangerous Galnet format
    (e.g., '21 April 3312').
    """
    return f"{date_object.day} {date_object.strftime('%B')} {date_object.year + 1286}" if date_object else "unknown_date"

def set_file_timestamps(filepath, date_object):
    """
    Sets the access and modified times of a file to match the article's
    publication date.
    """
    if date_object:
        try:
            timestamp_value = date_object.timestamp()
            os.utime(filepath, (timestamp_value, timestamp_value))
        except:
            pass

def find_date_locally(header):
    """
    Searches the local archive for an existing article with the same header
    to retrieve a previously found date.
    """
    if not os.path.exists(ARCHIVE_DIR): return None
    slug = slugify(header)
    for filename in os.listdir(ARCHIVE_DIR):
        if filename.endswith(f"_{slug}.json") and not filename.startswith("unknown_date"):
            try:
                with open(os.path.join(ARCHIVE_DIR, filename), 'r', encoding='utf-8') as file_handle:
                    article_data = json.load(file_handle)
                    return article_data.get('article_date')
            except: pass
    return None

def get_existing_article(header, date_str):
    """
    Checks if an article with the given header and date already exists
    in the local archive.
    """
    if date_str == "unknown_date":
        local_date = find_date_locally(header)
        if local_date: date_str = local_date

    date_object = parse_date(date_str)
    iso_prefix = date_object.strftime("%Y-%m-%d") if date_object else "unknown_date"
    filename = f"{iso_prefix}_{slugify(header)}.json"
    filepath = os.path.join(ARCHIVE_DIR, filename)
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as file_handle:
                return json.load(file_handle)
        except:
            pass
    return None

def enforce_rate_limit():
    """
    Ensures that a minimum amount of time has passed since the last external request.
    """
    global last_request_time
    with rate_limit_lock:
        elapsed = time.time() - last_request_time
        if elapsed < GLOBAL_RATE_LIMIT:
            time.sleep(GLOBAL_RATE_LIMIT - elapsed)
        last_request_time = time.time()

def fetch_drinkybird_date(header, browser_context):
    """
    Scrapes elite.drinkybird.net to find the publication date for an article
    header when it is missing from other sources.
    """
    enforce_rate_limit()
    print(f"Searching Drinkybird for missing date: {header}")
    try:
        encoded_title = urllib.parse.quote(header)
        search_url = f"https://elite.drinkybird.net/?title={encoded_title}&text=&from=2014-01-01&to=2026-04-21"
        page = browser_context.new_page()
        page.goto(search_url, timeout=30000)
        soup = BeautifulSoup(page.content(), 'html.parser')
        date_element = soup.find('p', class_='article-date')
        page.close()
        if date_element:
            found_date = date_element.get_text(strip=True)
            print(f"Drinkybird found: {found_date}")
            return found_date
    except Exception as error:
        print(f"Drinkybird search failed for '{header}': {error}")
    return "unknown_date"

def save_article(article_data, source_type):
    """
    Saves article data to an individual JSON file in the archive directory.
    Merges data if a file with an 'unknown_date' already exists for this header.
    """
    os.makedirs(ARCHIVE_DIR, exist_ok=True)

    header = article_data.get('header', 'no_header')
    date_object = parse_date(article_data.get('article_date'))

    iso_prefix = date_object.strftime("%Y-%m-%d") if date_object else "unknown_date"
    filename = f"{iso_prefix}_{slugify(header)}.json"
    filepath = os.path.join(ARCHIVE_DIR, filename)

    existing_data = {}
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as file_handle:
                existing_data = json.load(file_handle)
        except: pass

    if date_object:
        unknown_path = os.path.join(ARCHIVE_DIR, f"unknown_date_{slugify(header)}.json")
        if os.path.exists(unknown_path):
            try:
                with open(unknown_path, 'r', encoding='utf-8') as file_handle:
                    legacy_data = json.load(file_handle)
                    for key, value in legacy_data.items():
                        if value and not existing_data.get(key):
                            existing_data[key] = value
                os.remove(unknown_path)
                print(f"Merged and removed legacy unknown_date file for: {header}")
            except: pass

    final_article_data = {
        "header": article_data.get("header") or existing_data.get("header", ""),
        "body": article_data.get("body") or existing_data.get("body", ""),
        "article_date": format_date_elite(date_object) if date_object else article_data.get("article_date", "unknown_date"),
        "source": source_type,
        "scrape_date": datetime.now().isoformat()
    }

    for field in ["tags", "article_url"]:
        value = article_data.get(field) or existing_data.get(field)
        if value: final_article_data[field] = value

    try:
        with open(filepath, 'w', encoding='utf-8') as file_handle:
            json.dump(final_article_data, file_handle, indent=4, ensure_ascii=False)
        if date_object: set_file_timestamps(filepath, date_object)
        print(f"Saved ({source_type}): {filename}")
    except Exception as error:
        print(f"Error saving {filename}: {error}")

# --- Scrapers ---

def get_browser_context(playwright_instance):
    """
    Initializes a Playwright browser instance and returns a browser context
    with a custom user agent.
    """
    browser = playwright_instance.chromium.launch(headless=True)
    return browser, browser.new_context(user_agent=USER_AGENT)

def _run_inara_scrape(page_number, browser_context):
    """
    Internal helper to scrape a single page of Galnet news from Inara.cz.
    """
    enforce_rate_limit()
    url = f"https://inara.cz/elite/galnet/?page={page_number}"
    print(f"Scanning Inara page {page_number}...")
    page = browser_context.new_page()
    try:
        page.goto(url, timeout=60000)
        challenge_button = page.locator("#challenge")
        if challenge_button.is_visible():
            challenge_button.click()
            try: page.wait_for_selector(".mainblock", timeout=15000)
            except: pass

        soup = BeautifulSoup(page.content(), 'html.parser')
        article_elements = soup.find_all('div', class_='mainblock')

        processed_count = 0
        for element in article_elements:
            header_element = element.find('h2')
            if not header_element: continue
            header_text = header_element.get_text(strip=True)
            date_element = element.find('span', class_='date') or element.find('span', class_='text-muted')
            date_str = date_element.get_text(strip=True) if date_element else "unknown_date"

            if date_str == "unknown_date":
                local_date = find_date_locally(header_text)
                date_str = local_date if local_date else fetch_drinkybird_date(header_text, browser_context)

            existing_article = get_existing_article(header_text, date_str)
            if not existing_article or not existing_article.get("tags"):
                body_element = element.find('article')
                article_data = {
                    "header": header_text,
                    "body": "\n\n".join([paragraph.get_text(strip=True) for paragraph in body_element.find_all('p') if paragraph.get_text(strip=True)]) if body_element else "",
                    "article_date": date_str,
                    "tags": [tag.get_text(strip=True) for tag in element.find_all('a', class_=['tag', 'inaratag'])]
                }
                save_article(article_data, "Inara")
                processed_count += 1
        return processed_count
    finally:
        page.close()

def scrape_inara_page(page_number, browser_context=None):
    """
    Public method to scrape a single page of Inara.cz Galnet news.
    Manages Playwright lifecycle if no context is provided.
    """
    if browser_context:
        return _run_inara_scrape(page_number, browser_context)
    with sync_playwright() as playwright_instance:
        browser, browser_context = get_browser_context(playwright_instance)
        articles_processed = _run_inara_scrape(page_number, browser_context)
        browser.close()
        return articles_processed

def _run_frontier_scrape(page_number, browser_context):
    """
    Internal helper to scrape a single page of Galnet news from the official
    Elite Dangerous news site.
    """
    enforce_rate_limit()
    url = f"https://www.elitedangerous.com/news/galnet?page={page_number}"
    print(f"Scanning Frontier page {page_number}...")
    page = browser_context.new_page()
    try:
        page.goto(url, timeout=60000)
        try: page.wait_for_selector("article.o-news-article", timeout=30000)
        except: return 0

        soup = BeautifulSoup(page.content(), 'html.parser')
        article_elements = soup.find_all('article', class_='o-news-article')

        processed_count = 0
        for element in article_elements:
            header_element = element.find('h3')
            if not header_element: continue
            header_text = header_element.get_text(strip=True)
            date_element = element.find('time', class_='datetime')
            date_str = date_element.get_text(strip=True) if date_element else "unknown_date"

            if date_str == "unknown_date":
                local_date = find_date_locally(header_text)
                date_str = local_date if local_date else fetch_drinkybird_date(header_text, browser_context)

            existing_article = get_existing_article(header_text, date_str)
            if not existing_article or existing_article.get("source") != "Frontier":
                link_element = element.find('a', href=True)
                article_url = "https://www.elitedangerous.com" + link_element['href'] if link_element else ""
                body_text = ""

                if article_url:
                    article_page = browser_context.new_page()
                    enforce_rate_limit()
                    try:
                        article_page.goto(article_url, timeout=60000)
                        article_page.wait_for_selector(".v-galnet-details__main-body", timeout=15000)
                        article_soup = BeautifulSoup(article_page.content(), 'html.parser')
                        body_element = article_soup.select_one(".v-galnet-details__main-body")
                        if body_element:
                            body_text = "\n\n".join([paragraph.get_text(strip=True) for paragraph in body_element.find_all('p') if paragraph.get_text(strip=True)])
                    except: pass
                    finally: article_page.close()

                save_article({
                    "header": header_text,
                    "body": body_text,
                    "article_date": date_str,
                    "article_url": article_url
                }, "Frontier")
                processed_count += 1
        return processed_count
    finally:
        page.close()

def scrape_frontier_page(page_number, browser_context=None):
    """
    Public method to scrape a single page of official Frontier Galnet news.
    Manages Playwright lifecycle if no context is provided.
    """
    if browser_context:
        return _run_frontier_scrape(page_number, browser_context)
    with sync_playwright() as playwright_instance:
        browser, browser_context = get_browser_context(playwright_instance)
        articles_processed = _run_frontier_scrape(page_number, browser_context)
        browser.close()
        return articles_processed

def _run_community_scrape(page_number, browser_context):
    """
    Internal helper to scrape Galnet news from the Community site.
    """
    global community_hrefs
    with community_lock:
        if not community_hrefs:
            enforce_rate_limit()
            url = "https://community.elitedangerous.com/galnet"
            print("Fetching Community Galnet date list...")
            page = browser_context.new_page()
            try:
                page.goto(url, timeout=60000)
                more_link = page.locator("a[onclick*='show_extra_filters']")
                if more_link.is_visible():
                    more_link.click()
                    page.wait_for_selector("a.galnetLinkBoxLink", timeout=15000)
                soup = BeautifulSoup(page.content(), 'html.parser')
                links = soup.find_all('a', class_='galnetLinkBoxLink')
                community_hrefs = ["https://community.elitedangerous.com" + link['href'] for link in links if 'href' in link.attrs]
                print(f"Found {len(community_hrefs)} date links on Community site.")
            except Exception as error:
                print(f"Failed to fetch Community list: {error}")
                return 0
            finally:
                page.close()

    if page_number >= len(community_hrefs):
        return 0

    target_url = community_hrefs[page_number]
    print(f"Scanning Community Galnet: {target_url}...")
    page = browser_context.new_page()
    try:
        page.goto(target_url, timeout=60000)
        soup = BeautifulSoup(page.content(), 'html.parser')
        article_elements = soup.find_all('div', class_='article')
        processed_count = 0
        for element in article_elements:
            header_element = element.find('h3', class_='galnetNewsArticleTitle')
            if not header_element: continue
            header_text = header_element.get_text(strip=True)
            date_element = element.find('p', class_='small')
            date_str = date_element.get_text(strip=True) if date_element else "unknown_date"

            if date_str == "unknown_date":
                local_date = find_date_locally(header_text)
                date_str = local_date if local_date else fetch_drinkybird_date(header_text, browser_context)

            existing_article = get_existing_article(header_text, date_str)
            if not existing_article or existing_article.get("source") not in ["Frontier", "Community"]:
                body_paragraphs = [p.get_text(strip=True) for p in element.find_all('p') if 'small' not in p.get('class', [])]
                body_text = "\n\n".join([p for p in body_paragraphs if p])
                link_element = header_element.find('a', href=True)
                article_url = "https://community.elitedangerous.com" + link_element['href'] if link_element else target_url
                save_article({"header": header_text, "body": body_text, "article_date": date_str, "article_url": article_url}, "Community")
                processed_count += 1
        return processed_count
    finally:
        page.close()

def scrape_community_page(page_number, browser_context=None):
    """
    Public method to scrape a specific date from the Community site.
    """
    if browser_context:
        return _run_community_scrape(page_number, browser_context)
    with sync_playwright() as playwright_instance:
        browser, browser_context = get_browser_context(playwright_instance)
        articles_processed = _run_community_scrape(page_number, browser_context)
        browser.close()
        return articles_processed

def sync_source(source_name, scrape_func, browser_context):
    """
    Utility to scan a source sequentially starting from page 0 until
    no new articles are found.
    """
    print(f"\n--- Checking {source_name} ---")
    page_num = 0
    while True:
        processed_count = scrape_func(page_num, browser_context=browser_context)
        if processed_count == 0:
            print(f"Caught up with {source_name}.")
            break
        page_num += 1

def fetch_new_articles():
    """
    Fetches only new articles from Inara, Community and Frontier sources.
    Maintains preference: Frontier > Community > Inara.
    """
    print("\n=== Fetching New Galnet Articles ===")
    with sync_playwright() as playwright_instance:
        browser, browser_context = get_browser_context(playwright_instance)
        # Order ensures highest priority source overwrites and sets final source label
        sync_source("Inara", scrape_inara_page, browser_context)
        sync_source("Community", scrape_community_page, browser_context)
        sync_source("Frontier", scrape_frontier_page, browser_context)
        browser.close()
    combine_json_files()
    print("\n--- Sync Complete ---")

# --- Maintenance ---

def combine_json_files(output_file=MASTER_JSON, create_7z=True):
    """
    Rebuilds the master JSON file from the individual archive files,
    ensuring correct chronological order and uniqueness.
    Optionally creates a 7-zip archive of the result.
    """
    if not os.path.exists(ARCHIVE_DIR): return
    print(f"Updating {output_file}...")
    all_articles = []
    for filename in os.listdir(ARCHIVE_DIR):
        if not filename.endswith(".json"): continue
        try:
            with open(os.path.join(ARCHIVE_DIR, filename), 'r', encoding='utf-8') as file_handle:
                all_articles.append(json.load(file_handle))
        except: pass

    # Sort: Newest first (unknown dates at the bottom)
    all_articles.sort(key=lambda article: parse_date(article.get('article_date')).timestamp() if parse_date(article.get('article_date')) else 0, reverse=True)

    with open(output_file, 'w', encoding='utf-8') as file_handle:
        json.dump(all_articles, file_handle, indent=4, ensure_ascii=False)
    print(f"Combined {len(all_articles)} articles into {output_file}")

    if create_7z:
        if not HAS_PY7ZR:
            print("Warning: py7zr not installed. Skipping 7z creation.")
            return
        
        print(f"Creating 7z archive: {MASTER_7Z}...")
        try:
            with py7zr.SevenZipFile(MASTER_7Z, 'w') as archive:
                archive.write(output_file, arcname=output_file)
            print(f"Successfully archived to {MASTER_7Z}")
        except Exception as error:
            print(f"Error creating 7z archive: {error}")

def fix_unknown_date_files():
    """
    Identifies files in the archive with 'unknown_date' and attempts to
    recover their publication dates using Drinkybird.
    """
    unknown_files = [f for f in os.listdir(ARCHIVE_DIR) if f.startswith("unknown_date") and f.endswith(".json")]
    if not unknown_files:
        return print("No files with unknown dates found.")

    print(f"Attempting to fix {len(unknown_files)} files via Drinkybird...")

    with sync_playwright() as playwright_instance:
        browser, browser_context = get_browser_context(playwright_instance)
        for filename in unknown_files:
            filepath = os.path.join(ARCHIVE_DIR, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as file_handle:
                    article_data = json.load(file_handle)
                header_text = article_data.get('header')
                if not header_text: continue

                date_str = fetch_drinkybird_date(header_text, browser_context)
                if date_str != "unknown_date":
                    article_data['article_date'] = date_str
                    save_article(article_data, article_data.get("source", "Drinkybird"))
                else:
                    print(f"No date found for: {header_text}")
                time.sleep(1)
            except Exception as error:
                print(f"Error processing {filename}: {error}")
        browser.close()
    combine_json_files()

def fix_maintenance(task_type):
    """
    Performs maintenance tasks on the archive files:
    - 'normalize': Ensures dates are in the standard Elite format.
    - 'rename': Renames files to follow the 'YYYY-MM-DD_slug.json' format.
    """
    if not os.path.exists(ARCHIVE_DIR): return
    files = [f for f in os.listdir(ARCHIVE_DIR) if f.endswith(".json")]

    for filename in files:
        filepath = os.path.join(ARCHIVE_DIR, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as file_handle:
                article_data = json.load(file_handle)
            date_object = parse_date(article_data.get('article_date'))
            if not date_object: continue

            if task_type == "normalize":
                norm_date = format_date_elite(date_object)
                if article_data.get('article_date') != norm_date:
                    article_data['article_date'] = norm_date
                    with open(filepath, 'w', encoding='utf-8') as file_handle:
                        json.dump(article_data, file_handle, indent=4, ensure_ascii=False)

            if task_type == "rename":
                iso_prefix = date_object.strftime("%Y-%m-%d")
                new_name = f"{iso_prefix}_{slugify(article_data.get('header', ''))}.json"
                target_path = os.path.join(ARCHIVE_DIR, new_name)
                if filename != new_name:
                    if os.path.exists(target_path): os.remove(filepath)
                    else: os.rename(filepath, target_path)
                set_file_timestamps(target_path, date_object)
        except Exception as error:
            print(f"Error maintenance on {filename}: {error}")
    combine_json_files()


def core_generate_wordcloud(start_str=None, end_str=None, cloud_title=None):
    """
    Generates a word cloud image based on articles within a specific date range.
    Uses custom fonts, stop words, and an optional image mask.
    """
    if not HAS_WORDCLOUD: return print("Error: wordcloud/matplotlib missing.")

    start_date_object = parse_date(start_str) if start_str else None
    end_date_object = parse_date(end_str) if end_str else None

    text_content = []
    if not os.path.exists(ARCHIVE_DIR): return

    for filename in os.listdir(ARCHIVE_DIR):
        if not filename.endswith(".json"): continue
        try:
            with open(os.path.join(ARCHIVE_DIR, filename), 'r', encoding='utf-8') as file_handle:
                article_data = json.load(file_handle)
                article_date_object = parse_date(article_data.get('article_date'))
                if article_date_object:
                    if start_date_object and article_date_object < start_date_object: continue
                    if end_date_object and article_date_object > end_date_object: continue
                body_text = article_data.get('body', '')
                if body_text: text_content.append(body_text)
        except: continue

    if not text_content: return print(f"No articles for {cloud_title}")

    full_text = " ".join(text_content)

    elite_stopwords = {
        "Galnet", "Elite", "Dangerous", "System", "Federation", "Empire", "Alliance", "Independent",
        "article", "report", "news", "will", "one", "new", "said", "systems", "starport", "starports",
        "station", "stations", "commander", "commanders", "pilot", "pilots", "citizens", "people",
        "continue", "continues", "remains", "remain", "reporting", "reported", "spokesperson",
        "spokesman", "officials", "official", "stated", "claimed", "noted", "added", "announced",
        "confirmed", "according", "recently", "now", "current", "currently", "well", "may",
        "could", "should", "might", "can", "first", "many", "two", "three", "four", "five",
        "six", "seven", "eight", "nine", "ten", "last", "week", "year", "years", "month",
        "months", "day", "days", "time", "times", "part", "also", "including", "across",
        "number", "several", "known", "seen", "made", "become", "since", "within", "around"
    }

    custom_stopwords = set(STOPWORDS).union(elite_stopwords)

    # Use os.path.expanduser to correctly handle '~' in font paths
    valid_font_paths = [os.path.expanduser(path) for path in FONT_PATHS if os.path.exists(os.path.expanduser(path))]
    selected_font = random.choice(valid_font_paths) if valid_font_paths else None

    mask_image = np.array(Image.open(MASK_PATH)) if MASK_PATH and os.path.exists(MASK_PATH) else None

    wordcloud = WordCloud(
        width=3840, height=2160, background_color='black', colormap='YlOrRd',
        stopwords=custom_stopwords,
        font_path=selected_font, mask=mask_image
    ).generate(full_text)

    fig_width, fig_height = (20, 10)
    if mask_image is not None:
        fig_height = 20 * (mask_image.shape[0] / mask_image.shape[1])

    plt.figure(figsize=(fig_width, fig_height), facecolor='k')
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    if cloud_title:
        plt.title(cloud_title, color='#FF8C00', fontsize=30, fontweight='bold', pad=20)

    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_title = slugify(cloud_title) if cloud_title else "cloud"
    output_path = f"wordcloud_{safe_title}_{timestamp_str}.png"
    plt.savefig(output_path, facecolor='k', bbox_inches='tight')
    plt.close()
    print(f"Generated: {output_path}")

def run_custom_wordcloud():
    """
    Prompts the user for a date range and title to generate a specific word cloud.
    """
    start_date_input = input("Start Date (Elite): 01 January 3300").strip()
    end_date_input = input("End Date (Elite): 31 December 3312").strip()
    title_input = input("Title: ").strip()
    core_generate_wordcloud(start_date_input, end_date_input, title_input)

def run_yearly_wordclouds():
    """
    Generates a word cloud for every year from 3300 to 3312.
    """
    for year in range(3300, 3313):
        start_date = f"01 January {year}"
        end_date = f"31 December {year}"
        title = f"Galnet News Archive: {year}"
        print(f"Processing year {year}...")
        core_generate_wordcloud(start_date, end_date, title)

# --- Main ---

def main_menu():
    """
    The main interactive menu for the Galnet Scraper tool.
    """
    while True:
        print(f"\n--- Galnet Scraper Menu ---")
        print("1. Sync New Articles (All Sources)")
        print("2. Bulk Scrape Inara Galnet (Page Range)")
        print("3. Bulk Scrape Community Galnet (Index Range)")
        print("4. Bulk Scrape Frontier Galnet (Page Range)")
        print("5. Rename Files & Sync Timestamps")
        print("6. Fix Unknown Date Files (Search Drinkybird)")
        print("7. Combine All into Single JSON & 7z")
        print("8. Normalize Dates")
        print("9. Generate Custom Word Cloud")
        print("10. Generate Yearly Word Clouds (3300-3312)")
        print("0. Quit")

        choice = input("\nEnter choice: ").strip()
        if choice == '0': break

        if choice == '1':
            fetch_new_articles()
        elif choice in ('2', '3', '4'):
            try:
                # start_page = int(input("Start index/page: ") or 0)
                # end_page = int(input("End index/page: ") or 10)
                start_page = 0
                end_page = 10
                if choice == '2': scrape_function = scrape_inara_page
                elif choice == '3': scrape_function = scrape_community_page
                else: scrape_function = scrape_frontier_page
                with ThreadPoolExecutor(max_workers=5) as thread_executor:
                    thread_executor.map(scrape_function, range(start_page, end_page))
                combine_json_files()
            except: print("Invalid input.")
        elif choice == '5': fix_maintenance("rename")
        elif choice == '6': fix_unknown_date_files()
        elif choice == '7': 
            use_7z = input("Create 7z archive? (Y/n): ").strip().lower() != 'n'
            combine_json_files(create_7z=use_7z)
        elif choice == '8': fix_maintenance("normalize")
        elif choice == '9': run_custom_wordcloud()
        elif choice == '10': run_yearly_wordclouds()

if __name__ == "__main__":
    main_menu()
