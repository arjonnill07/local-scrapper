# =============================================================================
# Bonik Barta Interactive Scraper (Final Version - Corrected)
#
# This version includes definitive fixes for date parsing and the "Next" button
# selector based on the latest logs and HTML analysis.
# =============================================================================

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

try:
    import dateparser
except ImportError:
    print("Error: 'dateparser' library not found. Please install it using: pip install dateparser")
    sys.exit(1)

# --- Configuration ---
LOG_FILENAME = 'scraper_bonikbarta_log.log'
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.FileHandler(LOG_FILENAME, 'w', 'utf-8'), logging.StreamHandler()])

# --- CSS Selectors ---
ARTICLE_BLOCK_SELECTOR = "div.grow.group"
LINK_SELECTOR = "h3 a"
DATELINE_SELECTOR = "p.text-bb-text"
# CORRECTED: The "Next" button is a <button> identified by its aria-label.
NEXT_PAGE_BUTTON_SELECTOR = "button[aria-label='Next']"

# --- Scraper Settings ---
CHECKPOINT_FILE = "checkpoint_bonikbarta_last_batch.csv"
MAX_RETRIES = 5 # Set to 1 as we are running interactively
BATCH_SIZE = 100
MAX_PAGES = 1200


class HumanBehavior:
    def __init__(self, driver):
        self.driver = driver
    def random_sleep(self, min_seconds=0.8, max_seconds=2.0):
        time.sleep(random.uniform(min_seconds, max_seconds))
    def scroll_like_human(self, target_element=None):
        if target_element and target_element.is_displayed():
            self.driver.execute_script("arguments[0].scrollIntoView({behavior: 'auto', block: 'center'});", target_element)
            self.random_sleep(0.5, 1.0)
        else:
            for _ in range(random.randint(2, 4)):
                self.driver.execute_script(f'window.scrollBy(0, {random.randint(300, 600)});')
                self.random_sleep(0.4, 0.8)
    def move_to_and_safely_click(self, element):
        try:
            ActionChains(self.driver).move_to_element(element).perform()
            self.random_sleep(0.3, 0.7)
            self.driver.execute_script("arguments[0].click();", element)
        except Exception as e:
            logging.error(f"Safe click failed: {e}")
            raise

def parse_bonikbarta_date(date_str):
    if not date_str: return None
    try:
        parsed_dt = dateparser.parse(date_str, languages=['bn', 'en'])
        return parsed_dt.replace(tzinfo=None) if parsed_dt else None
    except Exception as e:
        logging.error(f"Error parsing date string '{date_str}': {e}")
        return None

def save_progress(articles_batch, main_csv_path, checkpoint_path):
    if not articles_batch: return
    df = pd.DataFrame(articles_batch)
    df.to_csv(main_csv_path, mode='a', header=not os.path.exists(main_csv_path), index=False, encoding='utf-8-sig')
    df.to_csv(checkpoint_path, 'w', index=False, encoding='utf-8-sig')
    logging.info(f"Saved a batch of {len(articles_batch)} articles.")

def load_processed_urls(main_csv_path):
    if not os.path.exists(main_csv_path): return set()
    try:
        return set(pd.read_csv(main_csv_path, usecols=['url']).dropna()['url'])
    except:
        logging.warning(f"Could not load URLs from {main_csv_path}. Starting fresh.")
        return set()

stop_requested = False
def signal_handler(sig, frame):
    global stop_requested
    if not stop_requested:
        stop_requested = True
        logging.warning("SIGINT received! Stopping gracefully after this page.")
signal.signal(signal.SIGINT, signal_handler)

def main(args):
    global stop_requested
    processed_urls = load_processed_urls(args.output_file)
    logging.info(f"Loaded {len(processed_urls)} previously scraped URLs.")

    for attempt in range(MAX_RETRIES):
        driver = None
        articles_batch = []
        try:
            options = uc.ChromeOptions()
            options.add_argument(f"--user-data-dir={args.profile_path}")
            driver = uc.Chrome(options=options, use_subprocess=True, headless=False)
            wait = WebDriverWait(driver, 10)
            human_behavior = HumanBehavior(driver)

            driver.get("https://www.bonikbarta.com/search/")
            print("\n" + "="*80 + "\nSCRIPT PAUSED\n1. Perform your search in the browser.\n2. Wait for the first page of results to load." + "="*80)
            input("3. Once results are visible, press Enter here to continue...\n")
            logging.info("User has completed the search. Resuming automated scraping.")

            page_count = 0
            while not stop_requested and page_count < MAX_PAGES:
                page_count += 1
                logging.info(f"Scraping page {page_count}...")
                
                try:
                    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)))
                    article_elements = driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)
                except TimeoutException:
                    logging.info("No articles found on this page. Ending scrape.")
                    break

                human_behavior.scroll_like_human()

                for element in article_elements:
                    if stop_requested: break
                    try:
                        link_el = element.find_element(By.CSS_SELECTOR, LINK_SELECTOR)
                        date_el = element.find_element(By.CSS_SELECTOR, DATELINE_SELECTOR)
                        relative_url = link_el.get_attribute("href")
                        url = "https://www.bonikbarta.com" + relative_url if relative_url.startswith("/") else relative_url
                        if not url or url in processed_urls: continue
                        title = link_el.text.strip()
                        date_str = date_el.text.strip()
                        published_date = parse_bonikbarta_date(date_str)

                        if not (title and published_date):
                            logging.warning(f"Skipping article due to missing title or unparsable date. URL: {url}, Date Text: '{date_str}'")
                            continue

                        article_data = {'url': url, 'title': title, 'description': "", 'published_date': published_date}
                        articles_batch.append(article_data)
                        processed_urls.add(url)
                        logging.info(f"Successfully scraped: {title}")

                        if args.max_articles and len(processed_urls) >= args.max_articles:
                            logging.info(f"Reached max articles limit."); stop_requested = True; break
                        if len(articles_batch) >= BATCH_SIZE:
                            save_progress(articles_batch, args.output_file, CHECKPOINT_FILE); articles_batch = []
                    except NoSuchElementException:
                        logging.warning("An element matched the article selector but was missing an inner title/date. Skipping.")
                    except Exception as e:
                        logging.error(f"An unexpected error occurred while processing an article: {e}")

                if stop_requested: break

                # --- CORRECTED PAGINATION LOGIC ---
                try:
                    logging.info("Searching for 'Next' page button...")
                    # Wait for the button to be PRESENT in the DOM, which is more reliable.
                    next_page_button = wait.until(
                        EC.presence_of_element_located((By.CSS_SELECTOR, NEXT_PAGE_BUTTON_SELECTOR))
                    )
                    # Additionally, check if it's disabled, which indicates the last page.
                    if next_page_button.get_attribute("disabled"):
                        logging.info("'Next' button is disabled. Reached the end of the results.")
                        break
                    
                    logging.info("Found 'Next' page button. Clicking it.")
                    human_behavior.scroll_like_human(target_element=next_page_button)
                    human_behavior.move_to_and_safely_click(next_page_button)
                    human_behavior.random_sleep(2, 4)
                except TimeoutException:
                    logging.info("Could not find the 'Next' button after 10 seconds. Assuming end of results.")
                    break

            stop_requested = True
        except Exception as e:
            logging.error(f"An unexpected error occurred: {e}", exc_info=True)
        finally:
            if articles_batch: save_progress(articles_batch, args.output_file, CHECKPOINT_FILE)
            if driver:
                logging.info("Scraping finished. The browser window will close in 5 seconds.")
                time.sleep(5)
                driver.quit()
            if stop_requested: break
    
    if os.path.exists(args.output_file):
        logging.info("Performing final cleanup of the output file...")
        try:
            df = pd.read_csv(args.output_file)
            df.dropna(subset=['url', 'published_date'], inplace=True)
            df['published_date'] = pd.to_datetime(df['published_date'], errors='coerce')
            df.sort_values(by='published_date', ascending=False, inplace=True)
            df.drop_duplicates(subset=['url'], keep='first', inplace=True)
            df.to_csv(args.output_file, index=False, encoding='utf-8-sig')
            logging.info(f"✅ Done. Total unique articles saved: {len(df)}")
            if os.path.exists(CHECKPOINT_FILE): os.remove(CHECKPOINT_FILE)
        except Exception as e:
            logging.info(f"✅ Cleanup could not be completed. Error: {e}")
    else:
        logging.info("✅ Done. No articles were saved.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Interactively scrapes articles from a Bonik Barta search result.", formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--profile-path", required=True, help="Path to your Chrome user profile directory.")
    parser.add_argument("--output-file", default="bonikbarta_articles.csv", help="Name of the output CSV file.")
    parser.add_argument("--max-articles", type=int, default=None, help="Maximum number of new articles to scrape.")
    args = parser.parse_args()
    main(args)