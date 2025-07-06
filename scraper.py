"""
Final, Production-Ready Web Scraper for samakal.com.
This script uses:
1. Human Profile Emulation to bypass security.
2. Human Input Simulation (Page Down key) for reliable infinite scrolling.
3. On-the-fly parsing for efficiency.

** INSTRUCTIONS **
1. CLOSE ALL CHROME WINDOWS BEFORE RUNNING.
2. Ensure the CHROME_PROFILE_PATH is correct.
"""

# 1. Imports
import time
import pandas as pd
from datetime import datetime
import os
from bs4 import BeautifulSoup # Keep BeautifulSoup import though it's not directly used in the main loop, good practice
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException # Import NoSuchElementException for more specific error handling
from selenium.webdriver.common.keys import Keys # Import the Keys class for keyboard input
from selenium.webdriver.common.action_chains import ActionChains # Import ActionChains for human-like input
import random
import re # Import re for robust string cleaning

# 2. Configuration
# Ensure this path is correct for your system
CHROME_PROFILE_PATH = r"C:\Users\ASUS\AppData\Local\Google\Chrome\User Data\Profile 5"
SECTIONS_TO_SCRAPE = [
    "https://samakal.com/crime",
    "https://samakal.com/bangladesh",
    "https://samakal.com/whole-country"
]
KEYWORDS_TO_FIND = ["মব", "গণপিটুনি", "পিটিয়ে হত্যা", "হামলা", "বিক্ষুব্ধ জনতা"]
FROM_DATE = datetime(2024, 8, 5)
TO_DATE = datetime(2025, 6, 30)
CSV_OUTPUT_FILENAME = "samakal_mob_violence_articles.csv"

# --- CSS Selectors ---
ARTICLE_BLOCK_SELECTOR = "div.CatListNews"
TITLE_SELECTOR = ".CatListhead h3"
DESCRIPTION_SELECTOR = ".ListDesc p"
DATELINE_SELECTOR = "span.publishTime"

# 3. Helper Function
BENGALI_TO_ENG_MAP = {
    'জানুয়ারি': 'January', 'ফেব্রুয়ারি': 'February', 'মার্চ': 'March', 'এপ্রিল': 'April',
    'মে': 'May', 'জুন': 'June', 'জুলাই': 'July', 'আগস্ট': 'August', 'সেপ্টেম্বর': 'September',
    'অক্টোবর': 'October', 'নভেম্বর': 'November', 'ডিসেম্বর': 'December',
    '০': '0', '১': '1', '২': '2', '৩': '3', '৪': '4', '৫': '5', '৬': '6', '৭': '7', '৮': '8', '৯': '9'
}

def parse_samakal_date(date_str):
    """
    Parses the Bengali date string from Samakal.com, handling different prefixes
    and removing the time part.
    Expected format after cleaning: DD Month YYYY (e.g., 01 জুলাই 2025)
    """
    if not date_str:
        return None
    try:
        # Clean the string: remove prefixes and the time part after '|'
        clean_str = date_str.replace('প্রকাশিত:', '').replace('আপডেটঃ', '').strip()
        if '|' in clean_str:
            clean_str = clean_str.split('|')[0].strip()

        # Replace Bengali month names and digits with English equivalents
        for bn, en in BENGALI_TO_ENG_MAP.items():
            clean_str = clean_str.replace(bn, en)

        # Parse the cleaned English date string
        # Use regex to handle potential extra spaces around day/month/year
        match = re.match(r'(\d{1,2})\s*(\w+)\s*(\d{4})', clean_str)
        if not match:
             print(f"Warning: Could not parse date format: {date_str} -> {clean_str}")
             return None # Return None if regex doesn't match expected pattern

        day, month_en, year = match.groups()
        # Reconstruct the string in a format strptime can reliably parse
        cleaned_for_strptime = f"{int(day)} {month_en} {year}" # Ensure day is integer for potential leading zero removal

        return datetime.strptime(cleaned_for_strptime, '%d %B %Y')

    except (ValueError, TypeError) as e:
        print(f"Error parsing date string '{date_str}': {e}")
        return None

# 4. Main Scraping Logic
def main():
    if not os.path.exists(CHROME_PROFILE_PATH):
        print(f"FATAL: Chrome profile path is not valid: {CHROME_PROFILE_PATH}")
        print("Please ensure the path is correct and the profile exists.")
        return # Exit if profile path is invalid

    print("--- Starting Scraping with Human Input Simulation ---")

    options = uc.ChromeOptions()
    # Use the specified user data directory for profile emulation
    options.add_argument(f"--user-data-dir={CHROME_PROFILE_PATH}")
    # Optional: Add argument to specify a specific profile directory within User Data
    # options.add_argument("--profile-directory=Profile 5") # Uncomment if Profile 5 is a sub-directory like this

    try:
        # Increase the timeout for undetected_chromedriver initialization
        driver = uc.Chrome(options=options, use_subprocess=True, headless=False)# Specify a version if needed, keep headless=False for debugging
    except Exception as e:
        print(f"FATAL: Failed to start undetected_chromedriver. Ensure Chrome is installed,")
        print(f"Chrome windows are closed, and the version is compatible. Error: {e}")
        return # Exit if driver fails to start

    wait = WebDriverWait(driver, 25) # Increased wait time

    all_relevant_articles = []
    processed_urls = set()

    for section_url in SECTIONS_TO_SCRAPE:
        print(f"\n--- Starting section: {section_url} ---")
        driver.get(section_url)
        try:
            # Wait for at least one article block to be visible
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)))
            print("✅ Initial page loaded successfully. Waiting for content...")
            # Add a small buffer wait after initial load
            time.sleep(random.uniform(2, 4))

        except TimeoutException:
            print(f"❌ FATAL: Page did not load articles within timeout for {section_url}.")
            # Attempt to check if ANY body content loaded to differentiate from a completely blank page
            try:
                driver.find_element(By.TAG_NAME, 'body')
                print("Page body loaded, but no articles found with the selector.")
            except NoSuchElementException:
                 print("Could not even find body tag. Page load might have completely failed.")
            continue # Move to the next section URL

        body = driver.find_element(By.TAG_NAME, 'body')
        actions = ActionChains(driver)
        scroll_attempts = 0
        MAX_SCROLLS = 150 # Increased max scrolls
        last_article_count = 0
        no_new_articles_count = 0 # Counter for consecutive scrolls with no new articles
        MAX_NO_NEW_ARTICLES = 5 # Stop after this many scrolls with no new articles

        while True:
            # Get all article elements currently loaded
            article_elements = driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)
            current_article_count = len(article_elements)

            # Process newly loaded articles since the last check
            for element in article_elements[last_article_count:]:
                try:
                    a_tag = element.find_element(By.TAG_NAME, "a")
                    url = a_tag.get_attribute("href")
                    title = a_tag.find_element(By.CSS_SELECTOR, TITLE_SELECTOR).text.strip()
                    try:
                        description = a_tag.find_element(By.CSS_SELECTOR, DESCRIPTION_SELECTOR).text.strip()
                    except NoSuchElementException:
                        description = ""
                    date_str = a_tag.find_element(By.CSS_SELECTOR, DATELINE_SELECTOR).text.strip()

                    if url and url not in processed_urls:
                        processed_urls.add(url)
                        published_date = parse_samakal_date(date_str)
                        if published_date and (FROM_DATE <= published_date <= TO_DATE):
                            if any(keyword in title or keyword in description for keyword in KEYWORDS_TO_FIND):
                                print(f"  [+] Found Relevant: {title[:70]}... ({published_date.strftime('%Y-%m-%d')}) - {url}")
                                all_relevant_articles.append({
                                    'url': url,
                                    'title': title,
                                    'description': description,
                                    'published_date': published_date.strftime('%Y-%m-%d')
                                })
                except Exception as e:
                    print(f"Error processing an article element: {e}")
                    continue # Continue processing other elements

            # Update the last processed article count
            last_article_count = current_article_count

            # Check the date of the last processed article for early stopping
            # It's safer to check the date of the last article *processed*
            if all_relevant_articles:
                 # Get the date of the most recently added relevant article
                 last_relevant_date_str = all_relevant_articles[-1]['published_date']
                 last_relevant_date = datetime.strptime(last_relevant_date_str, '%Y-%m-%d') # Parse back to datetime for comparison
                 if last_relevant_date < FROM_DATE:
                     print(f"\nReached article published before target date range ({FROM_DATE.strftime('%Y-%m-%d')}). Stopping scroll for this section.")
                     break
            # Also check the date of the *last element found* regardless of relevance or parsing success
            if article_elements:
                try:
                    last_element = article_elements[-1]
                    dateline_element = last_element.find_element(By.CSS_SELECTOR, DATELINE_SELECTOR)
                    last_element_date_str = dateline_element.text
                    last_element_published_date = parse_samakal_date(last_element_date_str)
                    if last_element_published_date and last_element_published_date < FROM_DATE:
                         print(f"\nReached page end displaying articles published before target date range ({FROM_DATE.strftime('%Y-%m-%d')}). Stopping scroll for this section.")
                         break
                except NoSuchElementException:
                    # If the last element doesn't have a dateline, we can't use its date for stopping
                    pass # Continue scrolling


            print(f"Simulating human scroll... Currently found {current_article_count} total articles.")

            # Simulate multiple key presses for smoother scrolling
            for _ in range(random.randint(4, 8)): # Randomize key presses per scroll action
                key = Keys.PAGE_DOWN if random.random() > 0.3 else Keys.ARROW_DOWN # More chance of PAGE_DOWN
                actions.move_to_element(body).send_keys(key).perform()
                time.sleep(random.uniform(0.2, 0.6)) # Randomize pause between key presses

            # Occasionally move the mouse slightly or click to appear more human
            if random.random() > 0.8:
                try:
                    # Move mouse to a random point on screen or a specific element
                    actions.move_by_offset(random.randint(-50, 50), random.randint(-50, 50)).perform()
                    # Optional: Click on a random non-article element if found (e.g., footer link, header)
                    # Example: actions.move_to_element(driver.find_element(By.CSS_SELECTOR("footer a"))).click().perform()
                except Exception: pass # Ignore errors if these actions fail

            # Wait for a short period for content to potentially load after scrolling
            time.sleep(random.uniform(1.5, 3.0)) # Increased general wait time

            # Check if new articles were loaded
            new_article_elements_after_scroll = driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)
            new_article_count_after_scroll = len(new_article_elements_after_scroll)

            if new_article_count_after_scroll == current_article_count:
                 no_new_articles_count += 1
                 print(f"No new articles loaded after scroll. Consecutive attempts: {no_new_articles_count}/{MAX_NO_NEW_ARTICLES}")
            else:
                 no_new_articles_count = 0 # Reset counter if new articles were found
                 print(f"Loaded {new_article_count_after_scroll - current_article_count} new articles.")

            if no_new_articles_count >= MAX_NO_NEW_ARTICLES:
                print("\nStopped scrolling: Reached maximum consecutive attempts with no new articles.")
                break

            scroll_attempts += 1
            if scroll_attempts >= MAX_SCROLLS:
                print("\nStopped scrolling: Reached maximum scroll limit.")
                break

    driver.quit()
    print("\n--- All sections scraped successfully. ---")

    # 5. Final Saving Logic
    if not all_relevant_articles:
        print("\n❌ No relevant articles found matching the date range and keywords.")
    else:
        df = pd.DataFrame(all_relevant_articles)
        # Ensure date column is datetime for sorting
        df['published_date'] = pd.to_datetime(df['published_date'])
        # Sort by date (most recent first) and drop duplicates based on URL
        df = df.sort_values(by='published_date', ascending=False).drop_duplicates(subset=['url'])

        try:
            df.to_csv(CSV_OUTPUT_FILENAME, index=False, encoding='utf-8-sig')
            print(f"\n✅ Success! {len(df)} unique relevant articles saved to '{CSV_OUTPUT_FILENAME}'")
            print("--- Head of the saved data ---")
            print(df.head())
            print("-----------------------------")
        except Exception as e:
            print(f"FATAL: Could not save data to CSV file. Error: {e}")


if __name__ == "__main__":
    main()