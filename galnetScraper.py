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

# --- Configuration ---
ARCHIVE_DIR = "GalnetNewsArchive"
MASTER_JSON = "galnet_news_full.json"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36"

# List of paths to .ttf or .otf font files. 
FONT_PATHS = [
    "/home/ben/.local/share/fonts/n/NanamiPro_Normal.otf",
    "/home/ben/.local/share/fonts/e/EUROCAPS.ttf",
    "EuroStyle_Normal.ttf",
    "/home/ben/.local/share/fonts/p/Protomolecule.otf",
    "/home/ben/.local/share/fonts/g/Garde.ttf",
    "/home/ben/.local/share/fonts/b/b5.ttf"
]

# Path to an image file to use as a mask (e.g. "mask.png")
MASK_PATH = "lave_radio_wordcloud_mask.png"

# Lock for thread-safe logging
log_lock = threading.Lock()

# --- Utilities ---

def slugify(text):
    """Convert text into a file-safe slug."""
    text = text.lower()
    text = re.sub(r'[^a-z0-9]', '_', text)
    return re.sub(r'_+', '_', text).strip('_')

def parse_date(date_str):
    """Attempts to parse Galnet date format and returns a datetime object (Real World year)."""
    if not date_str or date_str == "unknown_date":
        return None
    
    match = re.search(r'(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})', date_str)
    if match:
        d, m, y = match.groups()
        year = int(y)
        # Elite Year -> Real Year conversion if needed
        real_year = year - 1286 if year > 3000 else year
        normalized_str = f"{d} {m} {real_year}"
        
        for fmt in ("%d %b %Y", "%d %B %Y"):
            try:
                return datetime.strptime(normalized_str, fmt)
            except ValueError:
                continue
    return None

def format_date_elite(dt):
    """Formats a datetime object into Elite format (e.g. '21 April 3312')."""
    return f"{dt.day} {dt.strftime('%B')} {dt.year + 1286}" if dt else "unknown_date"

def set_file_timestamps(filepath, dt):
    """Sets the access and modified times of a file."""
    if dt:
        try:
            ts = dt.timestamp()
            os.utime(filepath, (ts, ts))
        except:
            pass

def find_date_locally(header):
    """Searches the local archive for an existing article with a real date."""
    if not os.path.exists(ARCHIVE_DIR): return None
    slug = slugify(header)
    for filename in os.listdir(ARCHIVE_DIR):
        if filename.endswith(f"_{slug}.json") and not filename.startswith("unknown_date"):
            try:
                with open(os.path.join(ARCHIVE_DIR, filename), 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    return data.get('article_date')
            except: pass
    return None

def get_existing_article(header, date_str):
    """Returns the existing article data if it exists, otherwise None."""
    if date_str == "unknown_date":
        local_date = find_date_locally(header)
        if local_date: date_str = local_date

    dt = parse_date(date_str)
    iso_prefix = dt.strftime("%Y-%m-%d") if dt else "unknown_date"
    filename = f"{iso_prefix}_{slugify(header)}.json"
    filepath = os.path.join(ARCHIVE_DIR, filename)
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return json.load(f)
        except:
            pass
    return None

def fetch_drinkybird_date(header, context):
    """Attempts to find a date for a given header on Drinkybird."""
    print(f"Searching Drinkybird for missing date: {header}")
    try:
        encoded_title = urllib.parse.quote(header)
        url = f"https://elite.drinkybird.net/?title={encoded_title}&text=&from=2014-01-01&to=2026-04-21"
        page = context.new_page()
        page.goto(url, timeout=30000)
        soup = BeautifulSoup(page.content(), 'html.parser')
        date_el = soup.find('p', class_='article-date')
        page.close()
        if date_el:
            found_date = date_el.get_text(strip=True)
            print(f"Drinkybird found: {found_date}")
            return found_date
    except Exception as e:
        print(f"Drinkybird search failed for '{header}': {e}")
    return "unknown_date"

def save_article(data, source_type):
    """Unified helper to save/merge article data to JSON."""
    os.makedirs(ARCHIVE_DIR, exist_ok=True)
    
    header = data.get('header', 'no_header')
    dt = parse_date(data.get('article_date'))
    
    iso_prefix = dt.strftime("%Y-%m-%d") if dt else "unknown_date"
    filename = f"{iso_prefix}_{slugify(header)}.json"
    filepath = os.path.join(ARCHIVE_DIR, filename)

    existing = {}
    if os.path.exists(filepath):
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                existing = json.load(f)
        except: pass

    if dt:
        unknown_path = os.path.join(ARCHIVE_DIR, f"unknown_date_{slugify(header)}.json")
        if os.path.exists(unknown_path):
            try:
                with open(unknown_path, 'r', encoding='utf-8') as f:
                    u_data = json.load(f)
                    for k, v in u_data.items():
                        if v and not existing.get(k): existing[k] = v
                os.remove(unknown_path)
                print(f"Merged and removed legacy unknown_date file for: {header}")
            except: pass

    final_data = {
        "header": data.get("header") or existing.get("header", ""),
        "body": data.get("body") or existing.get("body", ""),
        "article_date": format_date_elite(dt) if dt else data.get("article_date", "unknown_date"),
        "source": source_type,
        "scrape_date": datetime.now().isoformat()
    }

    for field in ["tags", "article_url"]:
        val = data.get(field) or existing.get(field)
        if val: final_data[field] = val

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(final_data, f, indent=4, ensure_ascii=False)
        if dt: set_file_timestamps(filepath, dt)
        print(f"Saved ({source_type}): {filename}")
    except Exception as e:
        print(f"Error saving {filename}: {e}")

# --- Scrapers ---

def get_browser_context(p):
    browser = p.chromium.launch(headless=True)
    return browser, browser.new_context(user_agent=USER_AGENT)

def _run_inara_scrape(page_number, context):
    url = f"https://inara.cz/elite/galnet/?page={page_number}"
    print(f"Scanning Inara page {page_number}...")
    page = context.new_page()
    try:
        page.goto(url, timeout=60000)
        challenge_btn = page.locator("#challenge")
        if challenge_btn.is_visible():
            challenge_btn.click()
            try: page.wait_for_selector(".mainblock", timeout=15000)
            except: pass

        soup = BeautifulSoup(page.content(), 'html.parser')
        articles = soup.find_all('div', class_='mainblock')

        processed = 0
        for art in articles:
            header_el = art.find('h2')
            if not header_el: continue
            header = header_el.get_text(strip=True)
            date_el = art.find('span', class_='date') or art.find('span', class_='text-muted')
            date_str = date_el.get_text(strip=True) if date_el else "unknown_date"
            
            if date_str == "unknown_date":
                local_date = find_date_locally(header)
                date_str = local_date if local_date else fetch_drinkybird_date(header, context)

            existing = get_existing_article(header, date_str)
            if not existing or not existing.get("tags"):
                body_el = art.find('article')
                data = {
                    "header": header,
                    "body": "\n\n".join([p.get_text(strip=True) for p in body_el.find_all('p') if p.get_text(strip=True)]) if body_el else "",
                    "article_date": date_str,
                    "tags": [t.get_text(strip=True) for t in art.find_all('a', class_=['tag', 'inaratag'])]
                }
                save_article(data, "Inara")
                processed += 1
        return processed
    finally:
        page.close()

def scrape_inara_page(page_number, context=None):
    if context:
        return _run_inara_scrape(page_number, context)
    with sync_playwright() as p:
        browser, context = get_browser_context(p)
        res = _run_inara_scrape(page_number, context)
        browser.close()
        return res

def _run_frontier_scrape(page_number, context):
    url = f"https://www.elitedangerous.com/news/galnet?page={page_number}"
    print(f"Scanning Frontier page {page_number}...")
    page = context.new_page()
    try:
        page.goto(url, timeout=60000)
        try: page.wait_for_selector("article.o-news-article", timeout=30000)
        except: return 0

        soup = BeautifulSoup(page.content(), 'html.parser')
        articles = soup.find_all('article', class_='o-news-article')

        processed = 0
        for art in articles:
            title_el = art.find('h3')
            if not title_el: continue
            header = title_el.get_text(strip=True)
            date_el = art.find('time', class_='datetime')
            date_str = date_el.get_text(strip=True) if date_el else "unknown_date"
            
            if date_str == "unknown_date":
                local_date = find_date_locally(header)
                date_str = local_date if local_date else fetch_drinkybird_date(header, context)

            existing = get_existing_article(header, date_str)
            if not existing or not existing.get("article_url"):
                link_el = art.find('a', href=True)
                article_url = "https://www.elitedangerous.com" + link_el['href'] if link_el else ""
                body = ""
                
                if article_url:
                    art_page = context.new_page()
                    try:
                        art_page.goto(article_url, timeout=60000)
                        art_page.wait_for_selector(".v-galnet-details__main-body", timeout=15000)
                        art_soup = BeautifulSoup(art_page.content(), 'html.parser')
                        body_el = art_soup.select_one(".v-galnet-details__main-body")
                        if body_el:
                            body = "\n\n".join([p.get_text(strip=True) for p in body_el.find_all('p') if p.get_text(strip=True)])
                    except: pass
                    finally: art_page.close()

                save_article({
                    "header": header,
                    "body": body,
                    "article_date": date_str,
                    "article_url": article_url
                }, "Frontier")
                processed += 1
        return processed
    finally:
        page.close()

def scrape_frontier_page(page_number, context=None):
    if context:
        return _run_frontier_scrape(page_number, context)
    with sync_playwright() as p:
        browser, context = get_browser_context(p)
        res = _run_frontier_scrape(page_number, context)
        browser.close()
        return res

def sync_source(name, scrape_func, context):
    """Utility to scan a source sequentially starting from page 0 until no new articles are found."""
    print(f"\n--- Checking {name} ---")
    page_num = 0
    while True:
        processed = scrape_func(page_num, context=context)
        if processed == 0:
            print(f"Caught up with {name}.")
            break
        page_num += 1

def fetch_new_articles():
    """Fetches only new articles from both Inara and Frontier."""
    print("\n=== Fetching New Galnet Articles ===")
    with sync_playwright() as p:
        browser, context = get_browser_context(p)
        sync_source("Inara", scrape_inara_page, context)
        sync_source("Frontier", scrape_frontier_page, context)
        browser.close()
    combine_json_files()
    print("\n--- Sync Complete ---")

# --- Maintenance ---

def combine_json_files(output_file=MASTER_JSON):
    """Rebuilds the master JSON from the individual archive files, ensuring order and uniqueness."""
    if not os.path.exists(ARCHIVE_DIR): return
    print(f"Updating {output_file}...")
    all_articles = []
    for f in os.listdir(ARCHIVE_DIR):
        if not f.endswith(".json"): continue
        try:
            with open(os.path.join(ARCHIVE_DIR, f), 'r', encoding='utf-8') as file:
                all_articles.append(json.load(file))
        except: pass

    # Sort: Newest first (unknown dates at the bottom)
    all_articles.sort(key=lambda x: parse_date(x.get('article_date')).timestamp() if parse_date(x.get('article_date')) else 0, reverse=True)
    
    with open(output_file, 'w', encoding='utf-8') as file:
        json.dump(all_articles, file, indent=4, ensure_ascii=False)
    print(f"Combined {len(all_articles)} articles into {output_file}")

def fix_unknown_date_files():
    """Searches for missing dates on elite.drinkybird.net and updates the local archive."""
    unknown_files = [f for f in os.listdir(ARCHIVE_DIR) if f.startswith("unknown_date") and f.endswith(".json")]
    if not unknown_files:
        return print("No files with unknown dates found.")

    print(f"Attempting to fix {len(unknown_files)} files via Drinkybird...")

    with sync_playwright() as p:
        browser, context = get_browser_context(p)
        for filename in unknown_files:
            filepath = os.path.join(ARCHIVE_DIR, filename)
            try:
                with open(filepath, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                header = data.get('header')
                if not header: continue
                
                date_str = fetch_drinkybird_date(header, context)
                if date_str != "unknown_date":
                    data['article_date'] = date_str
                    save_article(data, data.get("source", "Drinkybird"))
                else:
                    print(f"No date found for: {header}")
                time.sleep(1)
            except Exception as e:
                print(f"Error processing {filename}: {e}")
        browser.close()
    combine_json_files()

def fix_maintenance(task_type):
    if not os.path.exists(ARCHIVE_DIR): return
    files = [f for f in os.listdir(ARCHIVE_DIR) if f.endswith(".json")]
    
    for filename in files:
        filepath = os.path.join(ARCHIVE_DIR, filename)
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            dt = parse_date(data.get('article_date'))
            if not dt: continue

            if task_type == "normalize":
                norm_date = format_date_elite(dt)
                if data.get('article_date') != norm_date:
                    data['article_date'] = norm_date
                    with open(filepath, 'w', encoding='utf-8') as f:
                        json.dump(data, f, indent=4, ensure_ascii=False)

            if task_type == "rename":
                iso_prefix = dt.strftime("%Y-%m-%d")
                new_name = f"{iso_prefix}_{slugify(data.get('header', ''))}.json"
                target_path = os.path.join(ARCHIVE_DIR, new_name)
                if filename != new_name:
                    if os.path.exists(target_path): os.remove(filepath)
                    else: os.rename(filepath, target_path)
                set_file_timestamps(target_path, dt)
        except Exception as e:
            print(f"Error maintenance on {filename}: {e}")
    combine_json_files()


def core_generate_wordcloud(start_str=None, end_str=None, cloud_title=None):
    """Core logic to generate a word cloud image."""
    if not HAS_WORDCLOUD: return print("Error: wordcloud/matplotlib missing.")
    
    start_dt = parse_date(start_str) if start_str else None
    end_dt = parse_date(end_str) if end_str else None

    text_content = []
    if not os.path.exists(ARCHIVE_DIR): return
    
    for filename in os.listdir(ARCHIVE_DIR):
        if not filename.endswith(".json"): continue
        try:
            with open(os.path.join(ARCHIVE_DIR, filename), 'r', encoding='utf-8') as f:
                data = json.load(f)
                dt = parse_date(data.get('article_date'))
                if dt:
                    if start_dt and dt < start_dt: continue
                    if end_dt and dt > end_dt: continue
                body = data.get('body', '')
                if body: text_content.append(body)
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
    selected_font = random.choice([p for p in FONT_PATHS if os.path.exists(p)]) if FONT_PATHS else None
    mask = np.array(Image.open(MASK_PATH)) if MASK_PATH and os.path.exists(MASK_PATH) else None

    wordcloud = WordCloud(
        width=3840, height=2160, background_color='black', colormap='YlOrRd',
        stopwords=custom_stopwords,
        font_path=selected_font, mask=mask
    ).generate(full_text)

    fig_w, fig_h = (20, 10)
    if mask is not None:
        fig_h = 20 * (mask.shape[0] / mask.shape[1])

    plt.figure(figsize=(fig_w, fig_h), facecolor='k')
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis("off")
    if cloud_title:
        plt.title(cloud_title, color='#FF8C00', fontsize=30, fontweight='bold', pad=20)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_title = slugify(cloud_title) if cloud_title else "cloud"
    output_path = f"wordcloud_{safe_title}_{timestamp}.png"
    plt.savefig(output_path, facecolor='k', bbox_inches='tight')
    plt.close()
    print(f"Generated: {output_path}")

def run_custom_wordcloud():
    s = input("Start Date (Elite): 01 January 3300").strip()
    e = input("End Date (Elite): 31 December 3312").strip()
    t = input("Title: ").strip()
    core_generate_wordcloud(s, e, t)

def run_yearly_wordclouds():
    """Generates a word cloud for every year from 3300 to 3312."""
    for year in range(3300, 3313):
        start = f"01 January {year}"
        end = f"31 December {year}"
        title = f"Galnet News Archive: {year}"
        print(f"Processing year {year}...")
        core_generate_wordcloud(start, end, title)

# --- Main ---

def main_menu():
    while True:
        print(f"\n--- Galnet Scraper Menu ---")
        print("1. Sync New Articles (Both Sources)")
        print("2. Bulk Scrape Inara Galnet (Page Range)")
        print("3. Bulk Scrape Frontier Galnet (Page Range)")
        print("4. Rename Files & Sync Timestamps")
        print("5. Fix Unknown Date Files (Search Drinkybird)")
        print("6. Combine All into Single JSON")
        print("7. Normalize Dates")
        print("8. Generate Custom Word Cloud")
        print("9. Generate Yearly Word Clouds (3300-3312)")
        print("0. Quit")
        
        choice = input("\nEnter choice: ").strip()
        if choice == '0': break
        
        if choice == '1':
            fetch_new_articles()
        elif choice in ('2', '3'):
            try:
                start = int(input("Start page: ") or 0)
                end = int(input("End page: ") or 10)
                func = scrape_inara_page if choice == '2' else scrape_frontier_page
                with ThreadPoolExecutor(max_workers=5) as exe:
                    exe.map(func, range(start, end))
                combine_json_files()
            except: print("Invalid input.")
        elif choice == '4': fix_maintenance("rename")
        elif choice == '5': fix_unknown_date_files()
        elif choice == '6': combine_json_files()
        elif choice == '7': fix_maintenance("normalize")
        elif choice == '8': run_custom_wordcloud()
        elif choice == '9': run_yearly_wordclouds()

if __name__ == "__main__":
    main_menu()
