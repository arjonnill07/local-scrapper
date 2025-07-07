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
        """Pauses for a random duration."""
        time.sleep(random.uniform(min_seconds, max_seconds))

    def scroll_like_human(self, target_element=None):
        """Scrolls the page in small, random chunks to appear more human."""
        last_pos = self.driver.execute_script('return window.pageYOffset;')
        scroll_stutters = random.randint(2, 5)

        for _ in range(scroll_stutters):
            if target_element and target_element.is_displayed():
                # If the target is visible, stop scrolling.
                self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'smooth', block: 'center'});", target_element)
                break
            
            scroll_amount = random.randint(300, 700)
            # 15% chance to scroll up a little, a very human-like behavior
            if random.random() < 0.15:
                scroll_amount = -random.randint(50, 100)
                logging.info("Human behavior: Scrolling up slightly.")
            
            self.driver.execute_script(f'window.scrollBy(0, {scroll_amount});')
            self.random_sleep(0.4, 0.8)

        new_pos = self.driver.execute_script('return window.pageYOffset;')
        if new_pos == last_pos and not target_element:
            logging.info("Reached bottom of page during human-like scroll.")

    def move_and_click(self, element, min_hesitation=0.3, max_hesitation=1.2):
        """Moves mouse to an element realistically, hesitates, and then clicks."""
        actions = ActionChains(self.driver)
        # Move the mouse over the element
        actions.move_to_element(element).perform()
        logging.info("Human behavior: Mouse moved to element.")
        # Hesitate for a random time before clicking
        self.random_sleep(min_hesitation, max_hesitation)
        actions.click(element).perform()
        logging.info("Human behavior: Element clicked.")

    def simulate_reading_pause(self, min_seconds=2, max_seconds=5):
        """Simulates a user pausing to read, including random mouse drifts."""
        logging.info(f"Human behavior: Simulating reading for {min_seconds}-{max_seconds}s...")
        self.random_sleep(min_seconds, max_seconds)
        # Add a random mouse drift during the pause
        if random.random() < 0.7:
             try:
                body = self.driver.find_element(By.TAG_NAME, 'body')
                actions = ActionChains(self.driver)
                # Move to a random offset within the visible part of the page
                win_height = self.driver.execute_script("return window.innerHeight;")
                random_y = random.randint(100, win_height - 100)
                actions.move_by_offset(random.randint(-50, 50), random_y - body.location['y']).perform()
                logging.info("Human behavior: Mouse drifted.")
             except Exception:
                 pass # Ignore if it fails


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
    batch_df.to_csv(main_csv_path, mode='a', header=not os.path.exists(main_csv_path), index=False, encoding='utf-8-sig')
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
    # ... setup paths and load processed URLs ...
    processed_urls = load_processed_urls(args.output_file)
    session_article_count = 0

    logging.info("--- Starting Scraping for Prothom Alo with Human Emulation ---")

    for attempt in range(MAX_RETRIES):
        driver = None
        articles_batch = []
        try:
            options = uc.ChromeOptions()
            options.add_argument(f"--user-data-dir={args.profile_path}")
            driver = uc.Chrome(options=options, use_subprocess=True, headless=False)
            wait = WebDriverWait(driver, 20)
            human_behavior = HumanBehavior(driver) # Instantiate our behavior class

            for section_url in SECTIONS_TO_SCRAPE:
                if stop_requested: break
                logging.info(f"\n--- Starting section: {section_url} ---")
                driver.get(section_url)
                
                # Human-like pause after page load
                human_behavior.simulate_reading_pause(3, 6)

                last_article_count = 0
                no_new_articles_count = 0
                should_stop_this_section = False

                while not stop_requested and not should_stop_this_section:
                    article_elements = driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)
                    
                    for element in article_elements[last_article_count:]:
                        # ... (article parsing logic remains the same)
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
                                # Tiny pause between processing items
                                time.sleep(random.uniform(0.05, 0.2))
                                
                                if args.max_articles and len(processed_urls) >= args.max_articles:
                                    stop_requested = True
                                    break
                                
                                if len(articles_batch) >= BATCH_SIZE:
                                    save_progress(articles_batch, args.output_file, CHECKPOINT_FILE)
                                    articles_batch = []
                                    human_behavior.simulate_reading_pause(5, 10) # Pause after a batch save
                        except (NoSuchElementException, AttributeError):
                            pass

                    if should_stop_this_section or stop_requested: break

                    last_article_count = len(article_elements)
                    
                    try:
                        load_more_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, LOAD_MORE_BUTTON_SELECTOR)))
                        
                        # Use human-like behaviors to find and click the button
                        human_behavior.scroll_like_human(target_element=load_more_button)
                        human_behavior.move_and_click(load_more_button)
                        
                        human_behavior.random_sleep(3.5, 5.5) # Wait for content to load

                        new_count_after_click = len(driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR))
                        if new_count_after_click == last_article_count:
                            no_new_articles_count += 1
                        else:
                            no_new_articles_count = 0

                    except (NoSuchElementException, TimeoutException):
                        logging.info("No 'Load More' button found. End of content for this section.")
                        break
                    except Exception as e:
                        logging.error(f"Error interacting with 'Load More' button: {e}. Assuming end of content.")
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
            if not stop_requested: break # Exit retry loop on success
    
    # ... (Finalizing data section remains the same)
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
            logging.info(f"Removed {initial_rows - final_rows} potential duplicates.")
            logging.info(f"✅ Success! {final_rows} unique articles saved to '{args.output_file}'")
            if os.path.exists(CHECKPOINT_FILE): os.remove(CHECKPOINT_FILE)
        except Exception as e:
            logging.critical(f"Could not perform final cleanup on CSV. Error: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Scrape articles from Prothomalo.com with human-like behavior.")
    # ... (argparse definitions remain the same)
    parser.add_argument("--profile-path", required=True, help="Absolute path to Chrome User Data profile dir.")
    parser.add_argument("--output-file", default="prothomalo_articles_scraped.csv", help="Output CSV file name.")
    parser.add_argument("--max-articles", type=int, default=None, help="Max total articles to scrape.")
    parsed_args = parser.parse_args()
    main(parsed_args)