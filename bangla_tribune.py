# Modified Prothom Alo Scraper for Bangla Tribune's "Country" section
#
# This script is designed to scrape articles from the "Country" section of Bangla Tribune.
# It navigates the website, clicks the "Load More" button to reveal older articles,
# and extracts the URL, title, description, and publication date for each article.
# It includes features for robustness, such as checkpointing, retries, and human-like browsing patterns.

import time
import pandas as pd
from datetime import datetime
import os
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException, ElementClickInterceptedException
from selenium.webdriver.common.action_chains import ActionChains
import random
import argparse
import logging
import sys
import signal

# The dateparser library is excellent for handling various date formats, including those with Bengali text.
try:
    import dateparser
except ImportError:
    print("Error: 'dateparser' library not found. Please install it using: pip install dateparser")
    sys.exit(1)

# --- Configuration ---

# Setup logging to file and console
LOG_FILENAME = 'scraper_banglatribune_log.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        # Use 'w' mode to start with a fresh log file for each run
        # Use 'utf-8' encoding to correctly log Bengali characters
        logging.FileHandler(LOG_FILENAME, 'w', 'utf-8'),
        logging.StreamHandler()
    ]
)

# Target URL for scraping
SECTIONS_TO_SCRAPE = ["https://www.banglatribune.com/country"]

# Date range for filtering articles. The scraper will stop when it encounters an article older than FROM_DATE.
FROM_DATE = datetime(2024, 8, 5)
TO_DATE = datetime(2025, 7, 7) # Using a future date to effectively mean "up to today"

# --- CSS Selectors for Bangla Tribune (Verified as of August 2024) ---
# Each article is contained within a div with these classes.
ARTICLE_BLOCK_SELECTOR = "div.each.col_in.has_image"
# The title is within a span with class 'title'.
TITLE_SELECTOR = "span.title"
# The summary/description is in a div with class 'summery'.
DESCRIPTION_SELECTOR = "div.summery"
# The dateline span contains the visible date and a 'data-published' attribute with a machine-readable timestamp.
DATELINE_SELECTOR = "span.time.aitm"
# The "Load More" button has a specific ID.
LOAD_MORE_BUTTON_SELECTOR = "#ajax_load_more_11049_btn"
# The link for each article is an anchor tag with this class, acting as an overlay.
LINK_SELECTOR = "a.link_overlay"


# --- Scraper Settings ---
CHECKPOINT_FILE = "checkpoint_banglatribune_last_batch.csv" # Saves the last successful batch to prevent data loss.
MAX_RETRIES = 5             # Number of times to retry the entire process on failure.
BATCH_SIZE = 100            # Number of articles to collect before saving to CSV.
MAX_NO_NEW_ARTICLES = 5     # Stop if "Load More" is clicked this many times without new articles appearing.

class HumanBehavior:
    """A class to encapsulate human-like browsing actions to reduce detection risk."""
    def __init__(self, driver):
        self.driver = driver

    def random_sleep(self, min_seconds=0.5, max_seconds=1.5):
        """Pauses for a random duration."""
        time.sleep(random.uniform(min_seconds, max_seconds))

    def scroll_like_human(self, target_element=None):
        """Performs a series of small, random scrolls to mimic a user."""
        if target_element and target_element.is_displayed():
            # Scroll the target element into the center of the view for a more natural interaction.
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", target_element)
            self.random_sleep(0.5, 1.0)
        else:
            # Fallback to generic scrolling if the element isn't available.
            for _ in range(random.randint(2, 4)):
                scroll_amount = random.randint(300, 600)
                self.driver.execute_script(f'window.scrollBy(0, {scroll_amount});')
                self.random_sleep(0.4, 0.8)

    def move_to_and_safely_click(self, element, min_hesitation=0.4, max_hesitation=1.0):
        """Moves to an element, hesitates, and then clicks using JavaScript to bypass potential interception."""
        try:
            ActionChains(self.driver).move_to_element(element).perform()
            self.random_sleep(min_hesitation, max_hesitation)
            # JavaScript click can sometimes be more reliable than Selenium's .click().
            self.driver.execute_script("arguments[0].click();", element)
        except Exception as e:
            logging.error(f"Safe click failed: {e}")
            raise

    def simulate_reading_pause(self, min_seconds=2, max_seconds=5):
        """Simulates a user pausing to read content on the page."""
        self.random_sleep(min_seconds, max_seconds)

def parse_banglatribune_date(date_str):
    """
    Parses a date string from the 'data-published' attribute using the dateparser library.
    The attribute format is typically ISO 8601 (e.g., '2024-08-07T14:48:00+06:00').
    """
    if not date_str:
        return None
    try:
        # dateparser can handle the ISO format directly.
        parsed_dt = dateparser.parse(date_str, languages=['bn', 'en'])
        # Return a timezone-naive datetime object for consistent comparison.
        return parsed_dt.replace(tzinfo=None) if parsed_dt else None
    except Exception as e:
        logging.error(f"Date parse error for string '{date_str}': {e}")
        return None

def save_progress(articles_batch, main_csv_path, checkpoint_path):
    """Saves a batch of articles to the main CSV and updates the checkpoint file."""
    if not articles_batch:
        return
    batch_df = pd.DataFrame(articles_batch)
    is_new_file = not os.path.exists(main_csv_path)
    # Append to the main file.
    batch_df.to_csv(main_csv_path, mode='a', header=is_new_file, index=False, encoding='utf-8-sig')
    # Overwrite the checkpoint file with the latest batch.
    batch_df.to_csv(checkpoint_path, 'w', index=False, encoding='utf-8-sig')
    logging.info(f"Saved a batch of {len(articles_batch)} articles.")

def load_processed_urls(main_csv_path):
    """Loads URLs from the main CSV file to avoid scraping duplicates."""
    if not os.path.exists(main_csv_path):
        return set()
    try:
        df = pd.read_csv(main_csv_path, usecols=['url'])
        return set(df['url'].dropna())
    except (FileNotFoundError, pd.errors.EmptyDataError, ValueError):
        # Handle cases where the file exists but is empty or doesn't have a 'url' column.
        logging.warning(f"Could not load URLs from {main_csv_path}. Starting with an empty set.")
        return set()

# --- Signal Handler for Graceful Shutdown ---
stop_requested = False
def signal_handler(sig, frame):
    """Handles Ctrl+C interruption to allow the script to save its current progress."""
    global stop_requested
    if not stop_requested:
        stop_requested = True
        logging.warning("SIGINT received! The scraper will stop and save progress after the current batch.")

signal.signal(signal.SIGINT, signal_handler)

def main(args):
    global stop_requested
    processed_urls = load_processed_urls(args.output_file)
    logging.info(f"Loaded {len(processed_urls)} previously scraped URLs.")

    # Main retry loop
    for attempt in range(MAX_RETRIES):
        driver = None
        articles_batch = []
        try:
            options = uc.ChromeOptions()
            options.add_argument(f"--user-data-dir={args.profile_path}")
            # Run in headless mode for automation, or set to False for debugging.
            driver = uc.Chrome(options=options, use_subprocess=True, headless=True)
            wait = WebDriverWait(driver, 20)
            human_behavior = HumanBehavior(driver)

            for section_url in SECTIONS_TO_SCRAPE:
                if stop_requested: break
                logging.info(f"Starting to scrape section: {section_url}")
                driver.get(section_url)
                human_behavior.simulate_reading_pause(3, 6)
                
                last_article_count = 0
                no_new_articles_count = 0

                while not stop_requested:
                    article_elements = driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)
                    
                    # Only process newly loaded article elements
                    new_articles = article_elements[last_article_count:]
                    if not new_articles:
                        logging.warning("No new articles found on the page. Checking for 'Load More' button.")

                    for element in new_articles:
                        if stop_requested: break
                        try:
                            # Extract the raw URL from the overlay link
                            link_el = element.find_element(By.CSS_SELECTOR, LINK_SELECTOR)
                            raw_url = link_el.get_attribute("href")

                            # Construct the absolute URL
                            if raw_url.startswith("/"):
                                url = "https://www.banglatribune.com" + raw_url
                            else:
                                url = raw_url

                            if url in processed_urls:
                                continue

                            # Extract data using the defined selectors
                            title = element.find_element(By.CSS_SELECTOR, TITLE_SELECTOR).text.strip()
                            description = element.find_element(By.CSS_SELECTOR, DESCRIPTION_SELECTOR).text.strip()
                            date_str = element.find_element(By.CSS_SELECTOR, DATELINE_SELECTOR).get_attribute("data-published")
                            published_date = parse_banglatribune_date(date_str)

                            if not (url and title and published_date):
                                logging.warning(f"Skipping article due to missing data. URL: {url}")
                                continue

                            # --- Date Filtering Logic ---
                            # If we've reached articles older than our start date, stop this section.
                            if published_date < FROM_DATE:
                                logging.info(f"Reached an article from {published_date.date()}, which is before the target start date of {FROM_DATE.date()}. Stopping.")
                                stop_requested = True # Use the main stop flag
                                break

                            # If the article is within our desired date range, add it to the batch.
                            if FROM_DATE <= published_date <= TO_DATE:
                                article_data = {
                                    'url': url,
                                    'title': title,
                                    'description': description,
                                    'published_date': published_date
                                }
                                articles_batch.append(article_data)
                                processed_urls.add(url)

                                # Optional: Stop if max articles limit is reached.
                                if args.max_articles and len(processed_urls) >= args.max_articles:
                                    logging.info(f"Reached max articles limit of {args.max_articles}.")
                                    stop_requested = True
                                    break

                                # Save progress when the batch is full.
                                if len(articles_batch) >= BATCH_SIZE:
                                    save_progress(articles_batch, args.output_file, CHECKPOINT_FILE)
                                    articles_batch = [] # Reset the batch
                                    human_behavior.simulate_reading_pause(3, 7) # Pause after saving

                        except (NoSuchElementException, AttributeError) as e:
                            logging.error(f"Could not process an article block: {e}")
                            continue

                    if stop_requested:
                        break

                    last_article_count = len(article_elements)
                    
                    # --- "Load More" Logic ---
                    try:
                        load_more_button = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, LOAD_MORE_BUTTON_SELECTOR)))
                        human_behavior.scroll_like_human(load_more_button)
                        human_behavior.move_to_and_safely_click(load_more_button)
                        logging.info("Clicked 'Load More' button.")
                        human_behavior.random_sleep(3, 5) # Wait for new content to load

                        # Check if new articles actually loaded to prevent infinite loops.
                        new_count = len(driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR))
                        if new_count == last_article_count:
                            no_new_articles_count += 1
                            logging.warning(f"No new articles loaded after click. Count: {no_new_articles_count}")
                        else:
                            no_new_articles_count = 0 # Reset counter on success

                        if no_new_articles_count >= MAX_NO_NEW_ARTICLES:
                            logging.info("Stopping: 'Load More' button did not provide new articles multiple times.")
                            break
                    except (TimeoutException, NoSuchElementException):
                        logging.info("No more 'Load More' button found. Reached the end of the page.")
                        break # Exit the loop if the button disappears

            # If the loop was broken by a stop request, break the outer retry loop as well.
            if stop_requested:
                break

        except Exception as e:
            logging.error(f"An unexpected error occurred on attempt {attempt + 1}: {e}", exc_info=True)
            if attempt < MAX_RETRIES - 1:
                logging.info(f"Retrying in 30 seconds...")
                time.sleep(30)
            else:
                logging.error("Max retries reached. Exiting.")
        finally:
            # Always save any remaining articles in the batch before quitting.
            if articles_batch:
                save_progress(articles_batch, args.output_file, CHECKPOINT_FILE)
            if driver:
                driver.quit()
            
            # If the process was meant to stop, don't retry.
            if stop_requested:
                break

    # --- Final Cleanup ---
    if os.path.exists(args.output_file):
        logging.info("Performing final cleanup of the output file...")
        df = pd.read_csv(args.output_file)
        df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
        df.dropna(subset=['url', 'published_date'], inplace=True)
        # Sort by date and remove duplicates, keeping the first occurrence.
        df = df.sort_values(by='published_date', ascending=False).drop_duplicates(subset=['url'], keep='first')
        df.to_csv(args.output_file, index=False, encoding='utf-8-sig')
        logging.info(f"✅ Done. Total unique articles saved: {len(df)}")
        # Clean up the checkpoint file on successful completion.
        if os.path.exists(CHECKPOINT_FILE):
            os.remove(CHECKPOINT_FILE)
    else:
        logging.info("✅ Done. No articles were saved.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Scrapes articles from the Bangla Tribune 'Country' section.",
        formatter_class=argparse.RawTextHelpFormatter
    )
    parser.add_argument(
        "--profile-path",
        required=True,
        help="Path to your Chrome user profile directory. This helps with logins and avoiding bot detection.\nExample: C:\\Users\\YourUser\\AppData\\Local\\Google\\Chrome\\User Data"
    )
    parser.add_argument(
        "--output-file",
        default="banglatribune_articles.csv",
        help="Name of the output CSV file. (default: banglatribune_articles.csv)"
    )
    parser.add_argument(
        "--max-articles",
        type=int,
        default=None,
        help="Maximum number of new articles to scrape in this session."
    )
    args = parser.parse_args()
    main(args)