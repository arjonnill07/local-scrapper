# 1. Imports
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
import shutil
try:
    import psutil
except ImportError:
    psutil = None
try:
    import dateparser
except ImportError:
    print("Error: 'dateparser' library not found. Please install it using: pip install dateparser")
    sys.exit(1)

# 2. Setup Logging
LOG_FILENAME = 'scraper_prothomalo_log.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILENAME, 'w', 'utf-8'),
        logging.StreamHandler()
    ]
)

# 3. Configuration for prothomalo.com
SECTIONS_TO_SCRAPE = [
    "https://www.prothomalo.com/bangladesh"
]
FROM_DATE = datetime(2024, 8, 5)
TO_DATE = datetime(2025, 7, 7)

# --- CSS Selectors for prothomalo.com ---
ARTICLE_BLOCK_SELECTOR = "div.news_item"
TITLE_SELECTOR = "h3 a.title-link"
DESCRIPTION_SELECTOR = "a.excerpt"
DATELINE_SELECTOR = "time.published-time"
LOAD_MORE_BUTTON_SELECTOR = "span.load-more-content"

# --- Operational Parameters ---
CHECKPOINT_FILE = "checkpoint_prothomalo_last_batch.csv"
MAX_RETRIES = 5
PROGRESS_LOG_INTERVAL = 300
BATCH_SIZE = 200
MAX_NO_NEW_ARTICLES = 5

# 4. Helper Classes and Functions

class HumanBehavior:
    """Encapsulates methods to simulate human-like interaction."""
    def __init__(self, driver):
        self.driver = driver

    def random_sleep(self, min_seconds=0.5, max_seconds=1.5):
        time.sleep(random.uniform(min_seconds, max_seconds))

    def scroll_like_human(self, target_element=None):
        last_pos = self.driver.execute_script('return window.pageYOffset;')
        for _ in range(random.randint(2, 5)):
            if target_element and target_element.is_displayed():
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", target_element)
                break
            scroll_amount = random.randint(300, 700)
            if random.random() < 0.15:
                scroll_amount = -random.randint(50, 100)
            self.driver.execute_script(f'window.scrollBy(0, {scroll_amount});')
            self.random_sleep(0.4, 0.8)

    def move_to_and_safely_click(self, element, min_hesitation=0.4, max_hesitation=1.0):
        """
        The most reliable way to click.
        1. Moves the mouse over the element to appear human.
        2. Executes a direct JavaScript click that cannot be intercepted by ads/pop-ups.
        """
        try:
            # Step 1: Human-like mouse movement
            actions = ActionChains(self.driver)
            actions.move_to_element(element).perform()
            logging.info("Human behavior: Mouse moved to target element.")

            # Step 2: Human-like hesitation
            self.random_sleep(min_hesitation, max_hesitation)

            # Step 3: Surgical JavaScript click for reliability
            self.driver.execute_script("arguments[0].click();", element)
            logging.info("Surgical Click: Executed JavaScript click to avoid interception.")
        except Exception as e:
            logging.error(f"Failed to perform safe click: {e}")
            # Re-raise the exception to be caught by the main loop's error handling
            raise

    def simulate_reading_pause(self, min_seconds=2, max_seconds=5):
        logging.info(f"Human behavior: Simulating reading for {min_seconds}-{max_seconds}s...")
        self.random_sleep(min_seconds, max_seconds)

def parse_prothomalo_date(date_str):
    if not date_str: return None
    try:
        parsed_dt = dateparser.parse(date_str, languages=['bn', 'en'])
        return parsed_dt.replace(tzinfo=None) if parsed_dt else None
    except Exception as e:
        logging.error(f"Error parsing date string '{date_str}': {e}")
        return None

def save_progress(articles_batch, main_csv_path, checkpoint_path):
    if not articles_batch: return
    batch_df = pd.DataFrame(articles_batch)
    is_new_file = not os.path.exists(main_csv_path)
    batch_df.to_csv(main_csv_path, mode='a', header=is_new_file, index=False, encoding='utf-8-sig')
    logging.info(f"Appended {len(articles_batch)} new articles to '{main_csv_path}'")
    batch_df.to_csv(checkpoint_path, 'w', header=True, index=False, encoding='utf-8-sig')

def load_processed_urls(main_csv_path):
    if not os.path.exists(main_csv_path): return set()
    try:
        df = pd.read_csv(main_csv_path, usecols=['url'])
        urls = set(df['url'])
        logging.info(f"Loaded {len(urls)} existing URLs from '{main_csv_path}'.")
        return urls
    except Exception as e:
        logging.warning(f"Could not load URLs from '{main_csv_path}': {e}. Starting fresh.")
        return set()

stop_requested = False
def signal_handler(sig, frame):
    global stop_requested
    stop_requested = True
    logging.warning("SIGINT detected. Saving progress and exiting after this batch.")
signal.signal(signal.SIGINT, signal_handler)

# 5. Main Scraping Logic
def main(args):
    global stop_requested
    processed_urls = load_processed_urls(args.output_file)
    session_article_count = 0

    logging.info("--- Starting Scraping for Prothom Alo with Surgical Clicking ---")

    for attempt in range(MAX_RETRIES):
        driver = None
        articles_batch = []
        try:
            options = uc.ChromeOptions()
            options.add_argument(f"--user-data-dir={args.profile_path}")
            driver = uc.Chrome(options=options, use_subprocess=True, headless=False)
            wait = WebDriverWait(driver, 20)
            human_behavior = HumanBehavior(driver)

            for section_url in SECTIONS_TO_SCRAPE:
                if stop_requested: break
                logging.info(f"\n--- Starting section: {section_url} ---")
                driver.get(section_url)
                human_behavior.simulate_reading_pause(3, 6)
                
                last_article_count = 0
                no_new_articles_count = 0
                should_stop_this_section = False

                while not stop_requested and not should_stop_this_section:
                    article_elements = driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)
                    
                    for element in article_elements[last_article_count:]:
                        try:
                            title_element = element.find_element(By.CSS_SELECTOR, TITLE_SELECTOR)
                            url = title_element.get_attribute("href")
                            if not url or url in processed_urls: continue
                            
                            title = title_element.text.strip()
                            date_str = element.find_element(By.CSS_SELECTOR, DATELINE_SELECTOR).text.strip()
                            published_date = parse_prothomalo_date(date_str)
                            
                            if not (url and title and published_date): continue
                            
                            if published_date < FROM_DATE:
                                logging.info(f"Reached articles before target start date ({FROM_DATE.date()}).")
                                should_stop_this_section = True 
                                break

                            if FROM_DATE <= published_date <= TO_DATE:
                                description = element.find_element(By.CSS_SELECTOR, DESCRIPTION_SELECTOR).text.strip()
                                processed_urls.add(url)
                                articles_batch.append({'url': url, 'title': title, 'description': description, 'published_date': published_date})
                                session_article_count += 1
                                
                                if args.max_articles and len(processed_urls) >= args.max_articles:
                                    stop_requested = True
                                    break
                                
                                if len(articles_batch) >= BATCH_SIZE:
                                    save_progress(articles_batch, args.output_file, CHECKPOINT_FILE)
                                    articles_batch = []
                                    human_behavior.simulate_reading_pause(5, 10)
                        except (NoSuchElementException, AttributeError):
                            pass

                    if should_stop_this_section or stop_requested: break

                    last_article_count = len(article_elements)
                    
                    try:
                        load_more_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, LOAD_MORE_BUTTON_SELECTOR)))
                        
                        # Use our new, ultra-reliable click method
                        human_behavior.scroll_like_human(target_element=load_more_button)
                        human_behavior.move_to_and_safely_click(load_more_button)
                        
                        human_behavior.random_sleep(3.5, 5.5)

                        new_count_after_click = len(driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR))
                        if new_count_after_click == last_article_count:
                            no_new_articles_count += 1
                        else:
                            no_new_articles_count = 0

                    except (NoSuchElementException, TimeoutException):
                        logging.info("No 'Load More' button found. End of content for this section.")
                        break
                    except Exception as e:
                        logging.error(f"Critical error during 'Load More' interaction: {e}. Assuming end of content.")
                        break

                    if no_new_articles_count >= MAX_NO_NEW_ARTICLES:
                        logging.warning("'Load More' button is not adding new content. Stopping.")
                        break
            
            if stop_requested: break
            
        except Exception as e:
            logging.error(f"Scraper crashed: {e}", exc_info=True)
            if attempt < MAX_RETRIES - 1: logging.info("Restarting...")
        finally:
            if articles_batch: save_progress(articles_batch, args.output_file, CHECKPOINT_FILE)
            if driver: driver.quit()
            if not stop_requested: break
    
    logging.info("\n--- Finalizing Data ---")
    if os.path.exists(args.output_file):
        try:
            df = pd.read_csv(args.output_file)
            df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
            df.dropna(subset=['url', 'published_date'], inplace=True)
            initial_rows = len(df)
            df = df.sort_values(by='published_date', ascending=False).drop_duplicates(subset=['url'], keep='first')
            final_rows = len(df)
            df.to_csv(args.output_file, index=False, encoding='utf-8-sig')
            logging.info(f"Removed {initial_rows - final_rows} duplicates.")
            logging.info(f"✅ Success! {final_rows} unique articles saved to '{args.output_file}'")
            if os.path.exists(CHECKPOINT_FILE): os.remove(CHECKPOINT_FILE)
        except Exception as e:
            logging.critical(f"Could not perform final cleanup on CSV. Error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape articles from Prothomalo.com with human-like behavior and reliable clicking.")
    parser.add_argument("--profile-path", required=True, help="Absolute path to Chrome User Data profile dir.")
    parser.add_argument("--output-file", default="prothomalo_articles_scraped.csv", help="Output CSV file name.")
    parser.add_argument("--max-articles", type=int, default=None, help="Max total articles to scrape.")
    parsed_args = parser.parse_args()
    main(parsed_args)