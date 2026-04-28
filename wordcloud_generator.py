import random
import logging
import re
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
from pathlib import Path
from PIL import Image
from datetime import datetime
from typing import List, Optional

# --- Logging Configuration ---
logging.basicConfig(
    level=logging.INFO, # Reverted to INFO
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Import necessary components from galnet_scraper
from galnet_scraper import (
    slugify,
    load_article_index,
    article_index_lock,
    article_index_cache
)

try:
    from wordcloud import WordCloud, STOPWORDS
    HAS_WORDCLOUD = True
except ImportError:
    HAS_WORDCLOUD = False
    logger.warning("WordCloud or Matplotlib not installed. Word cloud generation will be unavailable.")


logger = logging.getLogger(__name__)

# --- Configuration for Word Clouds ---
# List of paths to .ttf or .otf font files for word cloud generation.
# Paths are expanded using os.path.expanduser to handle '~'.
# Entries can also be font names (e.g., "Arial", "DejaVu Sans") which will be resolved by matplotlib's font manager.
FONT_PATHS = [
    "Nanami Pro",
    "Euro Caps",
    "EuroStyle",
    "Protomolecule",
    "Garde",
    "Babylon5",
]
MASK_IMAGE_PATH = Path("lave_radio_wordcloud_mask.png")


# --- Elite Specific Stopwords ---
ELITE_STOPWORDS = {
    "Galnet", "Elite", "Dangerous", "System", "Federation", "Empire", "Alliance", "Independent",
    "article", "report", "news", "will", "one", "new", "said", "systems", "starport", "starports",
    "station", "stations", "commander", "commanders", "pilot", "pilots", "citizens", "people",
    "continue", "continues", "remains", "remain",
    "reporting", "reported", "spokesperson", "spokesman", "officials", "official", "stated",
    "claimed", "noted", "added", "announced", "confirmed", "according", "recently", "now",
    "current", "currently", "well", "may", "could", "should", "might", "can", "first", "many",
    "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "last", "week",
    "year", "years", "month", "months", "day", "days", "time", "times", "part", "also",
    "including", "across", "number", "several", "known", "seen", "made", "become", "since",
    "within", "around"
}

def generate_word_cloud(
    start_date_string: Optional[str] = None,
    end_date_string: Optional[str] = None,
    cloud_title: Optional[str] = None,
    font_name: Optional[str] = None,
    use_random_system_font: bool = False
):
    """
    Generates a word cloud image based on articles within a specific date range.
    Uses custom fonts, stop words, and an optional image mask.
    """
    if not HAS_WORDCLOUD:
        logger.error("Wordcloud or Matplotlib dependencies are missing. Please install them.")
        return

    start_date_object = parse_input_date(start_date_string)
    end_date_object = parse_input_date(end_date_string)

    logger.info(f"Generating word cloud for date range: {start_date_object.strftime('%Y-%m-%d') if start_date_object else 'None'} to {end_date_object.strftime('%Y-%m-%d') if end_date_object else 'None'}")

    load_article_index() # Ensure the article index is loaded
    
    with article_index_lock:
        total_in_cache = sum(len(articles) for articles in article_index_cache.values())
        logger.debug(f"Article cache state in generator: {len(article_index_cache)} unique slugs, {total_in_cache} total articles.")

    all_article_body_text = []

    with article_index_lock:
        for article_list_for_slug in article_index_cache.values():
            for article_entry in article_list_for_slug:
                article_date_object: Optional[datetime] = article_entry.get('_date_obj')
                if article_date_object:
                    if start_date_object and article_date_object < start_date_object: continue
                    if end_date_object and article_date_object > end_date_object: continue

                article_body = article_entry.get('body')
                if article_body:
                    all_article_body_text.append(article_body)

    if not all_article_body_text:
        logger.warning(
            f"No articles found for word cloud generation within the specified date range "
            f"'{start_date_string}' to '{end_date_string}' for title: '{cloud_title}'")
        return

    full_text_corpus = " ".join(all_article_body_text)

    combined_stopwords_set = set(STOPWORDS).union(ELITE_STOPWORDS)

    # --- Font Selection Logic ---
    selected_font_path = None

    # 1. Try to use a specifically named font if provided
    if font_name:
        try:
            found_font = fm.findfont(font_name, fontext='ttf')
            if found_font and Path(found_font).exists():
                selected_font_path = found_font
                logger.info(f"Using specified font '{font_name}' found at: {selected_font_path}")
            else:
                logger.warning(f"Specified font '{font_name}' not found by font_manager. Trying other options.")
        except Exception as e:
            logger.warning(f"Error finding font '{font_name}' with font_manager: {e}. Trying other options.")

    # 2. If no specific font or it wasn't found, try a random system font if requested
    if not selected_font_path and use_random_system_font:
        system_fonts = fm.findSystemFonts(fontpaths=None, fontext='ttf')
        if system_fonts:
            selected_font_path = random.choice(system_fonts)
            logger.info(f"Using random system font: {selected_font_path}")
        else:
            logger.warning("No system fonts found. Falling back to FONT_PATHS.")

    # 3. If still no font, try to resolve from FONT_PATHS (which can now be names or paths)
    if not selected_font_path:
        resolved_font_paths_from_config = []
        for entry in FONT_PATHS:
            # First, try to treat it as a direct file path
            expanded_path = Path(entry).expanduser()
            if expanded_path.exists():
                resolved_font_paths_from_config.append(str(expanded_path))
            else:
                # If not a file path, try to treat it as a font name
                try:
                    found_by_name = fm.findfont(entry, fontext='ttf')
                    if found_by_name and Path(found_by_name).exists():
                        resolved_font_paths_from_config.append(found_by_name)
                        logger.debug(f"Resolved font name '{entry}' to path: {found_by_name}")
                    else:
                        logger.debug(f"Config entry '{entry}' is neither a file path nor a resolvable font name.")
                except Exception as e:
                    logger.debug(f"Error resolving config entry '{entry}' as font name: {e}")

        if resolved_font_paths_from_config:
            selected_font_path = random.choice(resolved_font_paths_from_config)
            logger.info(f"Using random font from FONT_PATHS (resolved): {selected_font_path}")
        else:
            logger.warning("No valid font paths or names found in FONT_PATHS. Word cloud will use default font.")
    # --- End Font Selection Logic ---

    mask_image_array = None
    if MASK_IMAGE_PATH.exists():
        try:
            mask_image_array = np.array(Image.open(MASK_IMAGE_PATH))
        except IOError as e:
            logger.warning(
                f"Could not load mask image from {MASK_IMAGE_PATH}: {e}. Word cloud will not use a mask.")

    wordcloud_instance = WordCloud(
        width=3840, height=2160, background_color='black', colormap='YlOrRd',
        stopwords=combined_stopwords_set,
        font_path=selected_font_path,
        mask=mask_image_array
    ).generate(full_text_corpus)

    figure_width, figure_height = (20, 10)
    if mask_image_array is not None:
        # Adjust figure height to maintain aspect ratio of the mask
        figure_height = 20 * (mask_image_array.shape[0] / mask_image_array.shape[1])

    plt.figure(figsize=(figure_width, figure_height), facecolor='k')
    plt.imshow(wordcloud_instance, interpolation='bilinear')
    plt.axis("off")  # Hide axes
    if cloud_title:
        plt.title(cloud_title, color='#FF8C00', fontsize=30, fontweight='bold', pad=20)

    timestamp_string = datetime.now().strftime("%Y%m%d_%H%M%S")
    safe_title_slug = slugify(cloud_title) if cloud_title else "cloud"
    output_filename = f"wordcloud_{safe_title_slug}_{timestamp_string}.png"
    try:
        plt.savefig(output_filename, facecolor='k', bbox_inches='tight')
        logger.info(f"Generated word cloud: {output_filename}")
    except Exception as e:
        logger.error(f"Error saving word cloud to {output_filename}: {e}")
    finally:
        plt.close()  # Close the plot to free memory
        

def parse_input_date(input_date: Optional[str]) -> Optional[datetime]:
    """
    Parses a date string in several formats and converts Elite Dangerous years to real-world years.
    Supported formats: "%d/%m/%Y", "%d %b %Y", "%d %B %Y".
    Also handles ordinals like 1st, 2nd, 3rd, 4th.
    """
    if not input_date:
        return None

    # Handle ordinals (e.g., '21st April 3308' -> '21 April 3308')
    cleaned_date = re.sub(r'(\d+)(st|nd|rd|th)', r'\1', input_date)

    formats = ["%d/%m/%Y", "%d %b %Y", "%d %B %Y"]
    for date_format in formats:
        try:
            date_obj = datetime.strptime(cleaned_date, date_format)

            # Elite Dangerous years are 1286 years ahead of real-world years.
            # If the year is > 3000, assume it's an Elite year and convert it to its real-world equivalent.
            if date_obj.year > 3000:
                real_world_year = date_obj.year - 1286
                date_obj = date_obj.replace(year=real_world_year)

            return date_obj
        except ValueError:
            continue

    return None


def run_custom_wordcloud_cli():
    """
    Prompts the user for a date range and title to generate a specific word cloud.
    Continuously prompts until valid start and end dates are provided.
    Supports multiple date formats: '21 April 3308', '21st April 3308', '21/04/3308', etc.
    """
    print("\n--- Generate Custom Word Cloud ---")
    print("Accepted Date Formats: '21 April 3308', '21st April 3308', '21/04/3308', etc.")
    print("NOTE: Years > 3000 are treated as Elite Dangerous years (Elite = Real + 1286).")
    print("      Years <= 3000 are treated as real-world years.")

    start_date_input = ""
    while True:
        start_date_input = input("Enter Start Date: ").strip()
        if parse_input_date(start_date_input):
            break
        print("Invalid Start Date format. Please try again.")

    end_date_input = ""
    while True:
        end_date_input = input("Enter End Date: ").strip()
        if parse_input_date(end_date_input):
            break
        print("Invalid End Date format. Please try again.")

    title_input = input("Enter Word Cloud Title: ").strip()
    
    font_name_input = input("Enter Font Name (optional, leave blank for random): ").strip()
    use_random_system_font_input = input("Use a random system font (y/N)? ").strip().lower()
    use_random_system_font = use_random_system_font_input == 'y'

    generate_word_cloud(
        start_date_input,
        end_date_input,
        title_input,
        font_name=font_name_input if font_name_input else None,
        use_random_system_font=use_random_system_font
    )


def run_yearly_wordclouds_cli():
    """
    Generates a word cloud for every year from 3300 to 3312.
    """
    for year_number in range(3300, 3313):
        start_date = f"01 January {year_number}"
        end_date = f"31 December {year_number}"
        title = f"Galnet News Archive: {year_number}"
        logger.info(f"Processing word cloud for year {year_number}...")
        generate_word_cloud(start_date, end_date, title)


def wordcloud_main_menu():
    """
    The main interactive menu for word cloud generation.
    """
    while True:
        print("\n--- Word Cloud Generation Menu ---")
        print("1. Generate Custom Word Cloud")
        print("2. Generate Yearly Word Clouds (3300-3312)")
        print("0. Quit")

        user_choice = input("\nEnter your choice: ").strip()
        if user_choice == '0':
            break
        elif user_choice == '1':
            run_custom_wordcloud_cli()
        elif user_choice == '2':
            run_yearly_wordclouds_cli()
        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    wordcloud_main_menu()