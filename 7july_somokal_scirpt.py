"""
Final, Production-Ready Web Scraper for samakal.com.
VERSION 3.1 - DATE-DRIVEN & UNLIMITED SCROLL

This script scrapes ALL articles from the specified sections within a given date range.
It uses:
1. Human Profile Emulation via command-line to bypass security.
2. Intelligent infinite scrolling that stops based on article dates, not an arbitrary limit.
3. Robust logging for monitoring and debugging.

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

CHECKPOINT_FILE = "checkpoint_samakal_articles.csv"
MAX_RETRIES = 5  # Increased for long runs
PROGRESS_LOG_INTERVAL = 500  # Log progress every 500 articles
MIN_DISK_GB = 2  # Warn if less than 2GB free
BATCH_SIZE = 200  # Slightly larger batch for efficiency

# 4. Helper Function
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

        match = re.match(r'(\d{1,2})\s*(\w+)\s*(\d{4})', clean_str)
        if not match:
             logging.warning(f"Could not parse date format: {date_str} -> {clean_str}")
             return None

        day, month_en, year = match.groups()
        cleaned_for_strptime = f"{int(day)} {month_en} {year}"
        return datetime.strptime(cleaned_for_strptime, '%d %B %Y')

    except (ValueError, TypeError) as e:
        logging.error(f"Error parsing date string '{date_str}': {e}")
        return None

# Helper to save progress
def save_checkpoint(articles, filename):
    if articles:
        df = pd.DataFrame(articles)
        df = df.sort_values(by='published_date', ascending=False).drop_duplicates(subset=['url'])
        df.to_csv(filename, index=False, encoding='utf-8-sig')
        logging.info(f"Checkpoint saved: {len(df)} articles to '{filename}'")

# Helper to load checkpoint
def load_checkpoint(filename):
    if os.path.exists(filename):
        try:
            df = pd.read_csv(filename)
            articles = df.to_dict('records')
            urls = set(df['url'])
            logging.info(f"Loaded checkpoint: {len(articles)} articles from '{filename}'")
            return articles, urls
        except Exception as e:
            logging.warning(f"Could not load checkpoint: {e}")
    return [], set()

# Check disk space
def check_disk_space(path="."):
    total, used, free = shutil.disk_usage(path)
    free_gb = free / (1024 ** 3)
    if free_gb < MIN_DISK_GB:
        logging.warning(f"Low disk space: {free_gb:.2f} GB remaining!")
    return free_gb

# Log memory usage
def log_memory_usage():
    if psutil:
        process = psutil.Process(os.getpid())
        mem_mb = process.memory_info().rss / (1024 ** 2)
        logging.info(f"Current memory usage: {mem_mb:.2f} MB")

# Handle Ctrl+C gracefully
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

    # Load checkpoint if exists
    all_articles, processed_urls = load_checkpoint(CHECKPOINT_FILE)
    total_saved = len(all_articles)

    logging.info("--- Starting Date-Driven Scraping ---")

    for attempt in range(MAX_RETRIES):
        driver = None
        try:
            options = uc.ChromeOptions()
            options.add_argument(f"--user-data-dir={chrome_profile_path}")
            # User-Agent rotation (simple example)
            user_agents = [
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15",
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ]
            options.add_argument(f'user-agent={random.choice(user_agents)}')
            driver = uc.Chrome(options=options, use_subprocess=True, headless=False)
            wait = WebDriverWait(driver, 45)
            actions = ActionChains(driver)

            try:
                for section_url in SECTIONS_TO_SCRAPE:
                    if stop_requested:
                        break
                    logging.info(f"\n--- Starting section: {section_url} ---")
                    driver.get(section_url)
                    try:
                        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)))
                        wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)))
                        wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                        logging.info("✅ Initial page loaded successfully.")
                        time.sleep(random.uniform(2, 4))
                    except TimeoutException:
                        logging.error(f"Page did not load articles within timeout for {section_url}. Skipping section.")
                        continue

                    last_article_count = 0
                    no_new_articles_count = 0
                    MAX_NO_NEW_ARTICLES = 10

                    while True:
                        if stop_requested:
                            break
                        check_disk_space()
                        if psutil:
                            log_memory_usage()
                        article_elements = driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)
                        current_article_count = len(article_elements)

                        for element in article_elements[last_article_count:]:
                            try:
                                a_tag = element.find_element(By.TAG_NAME, "a")
                                url = a_tag.get_attribute("href")
                                if not url or url in processed_urls:
                                    continue
                                # Explicit waits for title and date
                                title = wait.until(lambda d: a_tag.find_element(By.CSS_SELECTOR, TITLE_SELECTOR)).text.strip()
                                date_str = wait.until(lambda d: a_tag.find_element(By.CSS_SELECTOR, DATELINE_SELECTOR)).text.strip()
                                published_date = parse_samakal_date(date_str)
                                if not (url and title and published_date):
                                    continue
                                description = ""
                                try:
                                    description = wait.until(lambda d: a_tag.find_element(By.CSS_SELECTOR, DESCRIPTION_SELECTOR)).text.strip()
                                except NoSuchElementException:
                                    pass
                                # Only save if in date range
                                if FROM_DATE <= published_date <= TO_DATE:
                                    processed_urls.add(url)
                                    all_articles.append({
                                        'url': url,
                                        'title': title,
                                        'description': description,
                                        'published_date': published_date
                                    })
                                    total_saved += 1
                                    if total_saved % PROGRESS_LOG_INTERVAL == 0:
                                        logging.info(f"Progress: {total_saved} articles saved so far.")
                                    if max_articles and total_saved >= max_articles:
                                        logging.info(f"Reached max_articles={max_articles}. Stopping early.")
                                        stop_requested = True
                                        break
                                    # Periodically save and clear memory
                                    if len(all_articles) % BATCH_SIZE == 0:
                                        save_checkpoint(all_articles, CHECKPOINT_FILE)
                                        all_articles = []
                            except (NoSuchElementException, AttributeError) as e:
                                logging.warning(f"Could not parse an article element. Skipping. Error: {e}")
                            except Exception as e:
                                logging.error(f"Unexpected error while processing article: {e}")
                        last_article_count = current_article_count
                        save_checkpoint(all_articles, CHECKPOINT_FILE)
                        all_articles = []  # Clear memory after each scroll batch
                        # PRIMARY STOPPING CONDITION
                        if article_elements:
                            try:
                                last_element_date_str = article_elements[-1].find_element(By.CSS_SELECTOR, DATELINE_SELECTOR).text
                                last_element_date = parse_samakal_date(last_element_date_str)
                                if last_element_date and last_element_date < FROM_DATE:
                                    logging.info(f"Reached articles before target date. Stopping scroll for this section.")
                                    break
                            except NoSuchElementException:
                                pass
                        # Human Scrolling Simulation
                        body = wait.until(EC.presence_of_element_located((By.TAG_NAME, 'body')))
                        for _ in range(random.randint(4, 8)):
                            if stop_requested:
                                break
                            key = Keys.PAGE_DOWN if random.random() > 0.3 else Keys.ARROW_DOWN
                            actions.move_to_element(body).send_keys(key).perform()
                            time.sleep(random.uniform(0.2, 0.6))
                        time.sleep(random.uniform(1.5, 3.0))
                        new_article_count_after_scroll = len(driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR))
                        if new_article_count_after_scroll == current_article_count:
                            no_new_articles_count += 1
                        else:
                            no_new_articles_count = 0
                            logging.info(f"Loaded {new_article_count_after_scroll - current_article_count} new articles.")
                        if no_new_articles_count >= MAX_NO_NEW_ARTICLES:
                            logging.info("Stopped scrolling: Reached end of content for this section.")
                            break
            finally:
                if driver:
                    driver.quit()
                logging.info("\n--- Browser closed. Scraping process finished. ---")
            break  # Success, exit retry loop
        except Exception as e:
            logging.error(f"Scraper crashed: {e}")
            if attempt < MAX_RETRIES - 1:
                logging.info("Restarting browser and retrying...")
                time.sleep(5)
            else:
                logging.critical("Max retries reached. Exiting.")
        finally:
            save_checkpoint(all_articles, CHECKPOINT_FILE)
            all_articles = []  # Clear memory after each retry
    # Final save
    if os.path.exists(CHECKPOINT_FILE):
        try:
            df = pd.read_csv(CHECKPOINT_FILE)
            df = df.sort_values(by='published_date', ascending=False).drop_duplicates(subset=['url'])
            df.to_csv(csv_output_filename, index=False, encoding='utf-8-sig')
            logging.info(f"\n✅ Success! {len(df)} unique articles saved to '{csv_output_filename}'")
            logging.info("--- Head of the saved data ---\n" + df.head().to_string())
        except Exception as e:
            logging.critical(f"Could not save data to CSV file. Error: {e}")
        os.remove(CHECKPOINT_FILE)
    else:
        logging.warning("\nNo articles found matching the date range.")

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
        help="Maximum number of articles to scrape (for testing or emergency stop)."
    )
    parsed_args = parser.parse_args()
    main(parsed_args)