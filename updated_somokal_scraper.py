"""
Final, Production-Ready Web Scraper for samakal.com.
VERSION 3.3 - CORRECTED SHUTDOWN LOGIC & REFINED SCROLLING

This script scrapes ALL articles from the specified sections within a given date range.
It uses:
1. Human Profile Emulation via command-line to bypass security.
2. Intelligent infinite scrolling that stops based on article dates.
3. Efficient append-only saving to the main CSV and a small, temporary checkpoint for crash recovery.
4. Corrected logic to ensure Ctrl+C and other global stop conditions work as intended.

** INSTRUCTIONS **
1. CLOSE ALL CHROME WINDOWS BEFORE RUNNING.
2. Run this script from your terminal.

** HOW TO RUN (Example): **
python your_script_name.py --profile-path "C:/Users/YourUser/AppData/Local/Google/Chrome/User Data/Profile 5"
"""

# 1. Imports
import time
import pandas as pd
from datetime import datetime
import os
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
import random
import re
import argparse
import logging
import sys
import signal
import shutil
try:
    import psutil
except ImportError:
    psutil = None

# 2. Setup Logging
LOG_FILENAME = 'scraper_log.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILENAME, 'w', 'utf-8'),
        logging.StreamHandler()
    ]
)

# 3. Configuration
SECTIONS_TO_SCRAPE = [
    "https://samakal.com/whole-country"
]
# Define the date range for articles to be collected
FROM_DATE = datetime(2024, 8, 5)
TO_DATE = datetime(2025, 7, 7)

# --- CSS Selectors ---
ARTICLE_BLOCK_SELECTOR = "div.CatListNews"
TITLE_SELECTOR = ".CatListhead h3"
DESCRIPTION_SELECTOR = ".ListDesc p"
DATELINE_SELECTOR = "span.publishTime"
FOOTER_SELECTOR = "footer.common-footer"

# --- Operational Parameters ---
CHECKPOINT_FILE = "checkpoint_samakal_last_batch.csv" # Small, temporary file
MAX_RETRIES = 5
PROGRESS_LOG_INTERVAL = 300
MIN_DISK_GB = 2
BATCH_SIZE = 200 # Number of articles to hold in memory before saving

# 4. Helper Functions
BENGALI_TO_ENG_MAP = {
    'জানুয়ারি': 'January', 'ফেব্রুয়ারি': 'February', 'মার্চ': 'March', 'এপ্রিল': 'April',
    'মে': 'May', 'জুন': 'June', 'জুলাই': 'July', 'আগস্ট': 'August', 'সেপ্টেম্বর': 'September',
    'অক্টোবর': 'October', 'নভেম্বর': 'November', 'ডিসেম্বর': 'December',
    '০': '0', '১': '1', '২': '2', '৩': '3', '৪': '4', '৫': '5', '৬': '6', '৭': '7', '৮': '8', '৯': '9'
}

def parse_samakal_date(date_str):
    if not date_str:
        return None
    try:
        clean_str = date_str.replace('প্রকাশিত:', '').replace('আপডেটঃ', '').strip()
        if '|' in clean_str:
            clean_str = clean_str.split('|')[0].strip()

        for bn, en in BENGALI_TO_ENG_MAP.items():
            clean_str = clean_str.replace(bn, en)

        match = re.search(r'(\d{1,2})\s+(\w+)\s+(\d{4})', clean_str)
        if not match:
             logging.warning(f"Could not parse date format: {date_str} -> {clean_str}")
             return None

        day, month_en, year = match.groups()
        cleaned_for_strptime = f"{int(day)} {month_en} {year}"
        return datetime.strptime(cleaned_for_strptime, '%d %B %Y')

    except (ValueError, TypeError) as e:
        logging.error(f"Error parsing date string '{date_str}': {e}")
        return None

def save_progress(articles_batch, main_csv_path, checkpoint_path):
    if not articles_batch: return
    batch_df = pd.DataFrame(articles_batch)
    is_new_file = not os.path.exists(main_csv_path)
    batch_df.to_csv(main_csv_path, mode='a', header=is_new_file, index=False, encoding='utf-8-sig')
    logging.info(f"Appended {len(articles_batch)} new articles to '{main_csv_path}'")
    batch_df.to_csv(checkpoint_path, mode='w', header=True, index=False, encoding='utf-8-sig')

def load_processed_urls(main_csv_path):
    if os.path.exists(main_csv_path):
        try:
            df = pd.read_csv(main_csv_path, usecols=['url'])
            urls = set(df['url'])
            logging.info(f"Loaded {len(urls)} existing URLs from '{main_csv_path}' to prevent duplicates.")
            return urls
        except Exception as e:
            logging.warning(f"Could not load URLs from main CSV file '{main_csv_path}': {e}. Starting with an empty set.")
    return set()

def check_disk_space(path="."):
    total, used, free = shutil.disk_usage(path)
    free_gb = free / (1024 ** 3)
    if free_gb < MIN_DISK_GB: logging.warning(f"Low disk space: {free_gb:.2f} GB remaining!")
    return free_gb

def log_memory_usage():
    if psutil:
        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().rss / (1024 ** 2)
        logging.info(f"Current memory usage: {mem_mb:.2f} MB")

# Global flag for handling Ctrl+C gracefully
stop_requested = False
def signal_handler(sig, frame):
    global stop_requested
    stop_requested = True
    logging.warning("KeyboardInterrupt detected. Will save progress and exit after current batch.")
signal.signal(signal.SIGINT, signal_handler)

# 5. Main Scraping Logic
def main(args):
    global stop_requested
    chrome_profile_path = args.profile_path
    csv_output_filename = args.output_file
    max_articles = getattr(args, 'max_articles', None)

    if not os.path.exists(chrome_profile_path):
        logging.critical(f"Chrome profile path is not valid: {chrome_profile_path}")
        return

    processed_urls = load_processed_urls(csv_output_filename)
    session_article_count = 0

    logging.info("--- Starting Date-Driven Scraping ---")

    for attempt in range(MAX_RETRIES):
        driver = None
        articles_batch = []
        try:
            options = uc.ChromeOptions()
            options.add_argument(f"--user-data-dir={chrome_profile_path}")
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15"
            ]
            options.add_argument(f'user-agent={random.choice(user_agents)}')
            driver = uc.Chrome(options=options, use_subprocess=True, headless=False)
            wait = WebDriverWait(driver, 45)

            for section_url in SECTIONS_TO_SCRAPE:
                if stop_requested: break
                logging.info(f"\n--- Starting section: {section_url} ---")
                driver.get(section_url)
                try:
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)))
                    logging.info("✅ Initial page loaded successfully.")
                    time.sleep(random.uniform(2, 4))
                except TimeoutException:
                    logging.error(f"Page did not load articles within timeout for {section_url}. Skipping section.")
                    continue

                last_article_count = 0
                no_new_articles_count = 0
                MAX_NO_NEW_ARTICLES = 10
                
                # Flag to stop scrolling for THIS section only (e.g., date limit reached)
                should_stop_this_section = False

                while True:
                    article_elements = driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)
                    
                    for element in article_elements[last_article_count:]:
                        try:
                            a_tag = element.find_element(By.TAG_NAME, "a")
                            url = a_tag.get_attribute("href")
                            if not url or url in processed_urls: continue
                            
                            title = a_tag.find_element(By.CSS_SELECTOR, TITLE_SELECTOR).text.strip()
                            date_str = a_tag.find_element(By.CSS_SELECTOR, DATELINE_SELECTOR).text.strip()
                            published_date = parse_samakal_date(date_str)
                            
                            if not (url and title and published_date): continue
                            
                            # **CORRECTED LOGIC**: Use a local flag for the date limit stop
                            if published_date < FROM_DATE:
                                logging.info(f"Reached articles before target start date ({FROM_DATE.date()}). Stopping scroll for this section.")
                                should_stop_this_section = True 
                                break # Break from the for-element loop

                            if FROM_DATE <= published_date <= TO_DATE:
                                description = ""
                                try:
                                    description = a_tag.find_element(By.CSS_SELECTOR, DESCRIPTION_SELECTOR).text.strip()
                                except NoSuchElementException: pass

                                processed_urls.add(url)
                                articles_batch.append({'url': url, 'title': title, 'description': description, 'published_date': published_date})
                                session_article_count += 1
                                
                                if session_article_count > 0 and session_article_count % PROGRESS_LOG_INTERVAL == 0:
                                    logging.info(f"Progress: {session_article_count} new articles collected this session.")
                                    log_memory_usage()
                                
                                # **CORRECTED LOGIC**: Use the global flag for a global stop condition
                                if max_articles and len(processed_urls) >= max_articles:
                                    logging.info(f"Reached max_articles limit of {max_articles}. Stopping.")
                                    stop_requested = True # This is a global stop
                                    break
                                
                                if len(articles_batch) >= BATCH_SIZE:
                                    save_progress(articles_batch, csv_output_filename, CHECKPOINT_FILE)
                                    articles_batch = []
                        except (NoSuchElementException, AttributeError) as e:
                            logging.warning(f"Could not parse an article element. Skipping. Error: {e}")
                        except Exception as e:
                            logging.error(f"Unexpected error while processing article: {e}")
                    
                    # **CORRECTED LOGIC**: Break from the scroll loop if either a global or local stop is triggered
                    if stop_requested or should_stop_this_section: break

                    last_article_count = len(article_elements)
                    
                    # **IMPROVED SCROLLING**: scrollTo is more reliable than scrollBy for this purpose
                    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                    time.sleep(random.uniform(2.5, 4.0))
                    
                    new_article_count_after_scroll = len(driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR))
                    if new_article_count_after_scroll == last_article_count:
                        no_new_articles_count += 1
                    else:
                        no_new_articles_count = 0
                        logging.info(f"Loaded {new_article_count_after_scroll - last_article_count} new articles.")
                    
                    if no_new_articles_count >= MAX_NO_NEW_ARTICLES:
                        logging.info("Stopped scrolling: Reached end of content for this section.")
                        break

            break
        except Exception as e:
            logging.error(f"Scraper crashed: {e}")
            if attempt < MAX_RETRIES - 1:
                logging.info("Restarting browser and retrying...")
                time.sleep(5)
            else:
                logging.critical("Max retries reached. Exiting.")
        finally:
            if articles_batch:
                logging.info("Saving remaining articles before exit...")
                save_progress(articles_batch, csv_output_filename, CHECKPOINT_FILE)
                articles_batch = []
            if driver:
                driver.quit()
    
    logging.info("\n--- Finalizing Data ---")
    if os.path.exists(csv_output_filename):
        try:
            logging.info("Performing final de-duplication and sort on main CSV file...")
            df = pd.read_csv(csv_output_filename)
            df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
            df.dropna(subset=['url', 'published_date'], inplace=True)
            
            initial_rows = len(df)
            df = df.sort_values(by='published_date', ascending=False).drop_duplicates(subset=['url'], keep='first')
            final_rows = len(df)
            
            df.to_csv(csv_output_filename, index=False, encoding='utf-8-sig')
            
            logging.info(f"Removed {initial_rows - final_rows} potential duplicates.")
            logging.info(f"✅ Success! {final_rows} unique articles saved to '{csv_output_filename}'")
            logging.info("--- Head of the saved data ---\n" + df.head().to_string())

            if os.path.exists(CHECKPOINT_FILE):
                os.remove(CHECKPOINT_FILE)
                logging.info(f"Removed temporary checkpoint file: '{CHECKPOINT_FILE}'")

        except Exception as e:
            logging.critical(f"Could not perform final cleanup on CSV file. Error: {e}")
    else:
        logging.warning("\nNo articles found or saved matching the criteria.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape all articles from Samakal.com within a specified date range.")
    parser.add_argument(
        "--profile-path",
        required=True,
        help="Absolute path to the Chrome User Data profile directory (e.g., 'C:\\Users\\YourUser\\AppData\\Local\\Google\\Chrome\\User Data\\Profile 5')."
    )
    parser.add_argument(
        "--output-file",
        default="samakal_articles_scraped.csv",
        help="Name of the output CSV file. Defaults to 'samakal_articles_scraped.csv'."
    )
    parser.add_argument(
        "--max-articles",
        type=int,
        default=None,
        help="Maximum number of total articles to have in the CSV before stopping (for testing or emergency stop)."
    )
    parsed_args = parser.parse_args()
    main(parsed_args)