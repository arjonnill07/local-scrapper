"""
Final professional web scraper for samakal.com.
This script uses Human Profile Emulation and the correct Infinite Scroll logic
to match the website's new design as of July 2025.

** INSTRUCTIONS **
1. CLOSE ALL CHROME WINDOWS BEFORE RUNNING.
2. The CHROME_PROFILE_PATH should be correctly set from the previous step.
"""

# 1. Imports
import time
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
import os
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException

# 2. Configuration
CHROME_PROFILE_PATH = r"C:\Users\ASUS\AppData\Local\Google\Chrome\User Data\Profile 5"

SECTIONS_TO_SCRAPE = [
    "https://samakal.com/crime",
    "https://samakal.com/bangladesh",
    "https://samakal.com/whole-country" # Assuming this is a valid section
]
KEYWORDS_TO_FIND = ["মব", "গণপিটুনি", "পিটিয়ে হত্যা", "হামলা", "বিক্ষুব্ধ জনতা"]
FROM_DATE = datetime(2024, 8, 5)
TO_DATE = datetime(2025, 6, 30)
CSV_OUTPUT_FILENAME = "samakal_mob_violence_articles.csv"

# --- ✅ NEW CSS Selectors for the new layout ---
ARTICLE_BLOCK_SELECTOR = "div.list-item-wrapper" # The new selector for list items
DATELINE_SELECTOR = "span.dateline" # This selector remains the same

# 3. Helper Function to Parse Bengali Dates
BENGALI_TO_ENG_MAP = { 'জানুয়ারি': 'January', 'ফেব্রুয়ারি': 'February', 'মার্চ': 'March', 'এপ্রিল': 'April', 'মে': 'May', 'জুন': 'June', 'জুলাই': 'July', 'আগস্ট': 'August', 'সেপ্টেম্বর': 'September', 'অক্টোবর': 'October', 'নভেম্বর': 'November', 'ডিসেম্বর': 'December', '০': '0', '১': '1', '২': '2', '৩': '3', '৪': '4', '৫': '5', '৬': '6', '৭': '7', '৮': '8', '৯': '9' }
def parse_samakal_date(date_str):
    if not date_str: return None
    try:
        clean_str = date_str.replace('প্রকাশিত:', '').strip()
        for bn, en in BENGALI_TO_ENG_MAP.items():
            clean_str = clean_str.replace(bn, en)
        return datetime.strptime(clean_str, '%d %B %Y')
    except (ValueError, TypeError): return None

# 4. Main Scraping Logic
def main():
    if not os.path.exists(CHROME_PROFILE_PATH):
        raise FileNotFoundError("FATAL: Chrome profile path is not valid. Please update the CHROME_PROFILE_PATH variable.")

    print("--- Starting Scraping using Human Profile Emulation & Infinite Scroll ---")

    options = uc.ChromeOptions()
    options.add_argument(f"--user-data-dir={CHROME_PROFILE_PATH}")
    
    driver = uc.Chrome(options=options, use_subprocess=True)
    wait = WebDriverWait(driver, 20)
    section_html_list = []

    for section_url in SECTIONS_TO_SCRAPE:
        print(f"\n--- Starting section: {section_url} ---")
        driver.get(section_url)
        try:
            print("Waiting for initial articles to load...")
            wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)))
            print("✅ Initial articles loaded!")
        except TimeoutException:
            print(f"❌ FATAL: The page did not load articles. Check that all Chrome windows were closed.")
            continue

        # Improved infinite scroll: check minimum date among all articles after each scroll
        last_height = driver.execute_script("return document.body.scrollHeight")
        scroll_count = 0
        MAX_SCROLLS = 100  # Safety limit to avoid infinite loop
        while True:
            try:
                article_elements = driver.find_elements(By.CSS_SELECTOR, ARTICLE_BLOCK_SELECTOR)
                min_date = None
                for elem in article_elements:
                    try:
                        date = parse_samakal_date(elem.find_element(By.CSS_SELECTOR, DATELINE_SELECTOR).text)
                        if date and (min_date is None or date < min_date):
                            min_date = date
                    except Exception:
                        continue
                print(f"Oldest article date on page: {min_date.strftime('%Y-%m-%d') if min_date else 'N/A'}")
                if min_date and min_date < FROM_DATE:
                    print("Reached target date. Stopping scroll for this section.")
                    break

                # Scroll down to the bottom of the page
                print("Scrolling down to load more articles...")
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(3)
                new_height = driver.execute_script("return document.body.scrollHeight")
                if new_height == last_height:
                    print("End of content for this section (page height did not change).")
                    break
                last_height = new_height
                scroll_count += 1
                if scroll_count >= MAX_SCROLLS:
                    print("Reached maximum scroll limit. Stopping to avoid infinite loop.")
                    break
            except Exception as e:
                print(f"An unexpected error occurred during scroll: {e}")
                break

        print(f"Finished loading all articles for {section_url}.")
        section_html_list.append(driver.page_source)

    driver.quit()
    print("\n--- All sections loaded successfully. Browser has been closed. ---")
    
    # 5. Parsing and Saving Logic (uses the new ARTICLE_BLOCK_SELECTOR)
    print("\n--- Parsing HTML and filtering articles ---")
    all_articles = []
    for html_content in section_html_list:
        soup = BeautifulSoup(html_content, 'html.parser')
        article_blocks = soup.select(ARTICLE_BLOCK_SELECTOR)
        for block in article_blocks:
            try:
                title_element = block.select_one("h3.title a")
                date_element = block.select_one(DATELINE_SELECTOR)
                if not title_element or not date_element: continue
                title = title_element.get_text(strip=True)
                # Normalize title for keyword matching
                normalized_title = title.replace(" ", "").lower()
                url = title_element['href']
                published_date = parse_samakal_date(date_element.get_text(strip=True))
                if published_date and (FROM_DATE <= published_date <= TO_DATE):
                    if any(keyword.replace(" ", "").lower() in normalized_title for keyword in KEYWORDS_TO_FIND):
                        all_articles.append({'url': url, 'title': title, 'published_date': published_date.strftime('%Y-%m-%d')})
            except Exception:
                continue

    if not all_articles:
        print("\n❌ No relevant articles found matching the criteria.")
    else:
        df = pd.DataFrame(all_articles).drop_duplicates(subset=['url'])
        df.to_csv(CSV_OUTPUT_FILENAME, index=False, encoding='utf-8-sig')
        print(f"\n✅ Success! {len(df)} unique articles saved to '{CSV_OUTPUT_FILENAME}'")
        print("\n--- First 5 rows of the collected data ---")
        print(df.head())

if __name__ == "__main__":
    main()