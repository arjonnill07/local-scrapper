import os
import time
import random
import re
import requests
import pandas as pd
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import dateparser
from langdetect import detect
import argparse
import logging
from tqdm import tqdm
from functools import wraps

# ✅ Setup Selenium with local Chrome
chrome_options = Options()
chrome_options.add_argument("--headless=new")  # Optional: remove for visible browsing
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("user-agent=Mozilla/5.0")

# ✅ Set the path to your downloaded ChromeDriver
CHROMEDRIVER_PATH = "chromedriver-win64/chromedriver.exe"  # Updated to point to the executable
driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=chrome_options)

# 🔧 Config
KEYWORDS = ["মব", "গণপিটুনি", "পিটিয়ে হত্যা", "হামলা", "বিক্ষুব্ধ জনতা"]
FROM_DATE = datetime(2024, 8, 5)
TO_DATE = datetime(2025, 8, 30)

SECTIONS = [
    "https://samakal.com/crime/",
    "https://samakal.com/national/",
    "https://samakal.com/bangladesh/"
]

# User-Agent rotation
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
]

def set_random_user_agent():
    ua = random.choice(USER_AGENTS)
    chrome_options.add_argument(f'user-agent={ua}')

def retry(ExceptionToCheck, tries=3, delay=2, backoff=2):
    def deco_retry(f):
        @wraps(f)
        def f_retry(*args, **kwargs):
            mtries, mdelay = tries, delay
            while mtries > 1:
                try:
                    return f(*args, **kwargs)
                except ExceptionToCheck as e:
                    logging.warning(f"Retrying after error: {e}")
                    time.sleep(mdelay)
                    mtries -= 1
                    mdelay *= backoff
            return f(*args, **kwargs)
        return f_retry
    return deco_retry

@retry(Exception, tries=3)
def extract_article(url):
    try:
        driver.get(url)
        time.sleep(2)
        soup = BeautifulSoup(driver.page_source, "html.parser")
        # Updated selectors based on real article structure
        title_el = soup.select_one("h1")
        date_el = soup.select_one("div.date-time")
        content_els = soup.select("div#news-details p")
        if not title_el or not content_els:
            with open("failed_article.html", "w", encoding="utf-8") as f:
                f.write(driver.page_source)
            return None
        content = " ".join(p.get_text(strip=True) for p in content_els)
        parsed_date = dateparser.parse(date_el.get_text(strip=True)) if date_el else None
        if parsed_date and FROM_DATE <= parsed_date <= TO_DATE:
            if detect(content) == "bn":
                return {
                    "url": url,
                    "title": title_el.get_text(strip=True),
                    "published_date": parsed_date.strftime("%Y-%m-%d"),
                    "description": content[:300] + "...",
                    "source": "samakal.com"
                }
    except Exception as e:
        logging.error(f"❌ Error parsing article {url}: {e}")
    return None

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Samakal Article Scraper")
    parser.add_argument('--from-date', type=str, default="2024-08-05", help="Start date (YYYY-MM-DD)")
    parser.add_argument('--to-date', type=str, default="2025-08-30", help="End date (YYYY-MM-DD)")
    parser.add_argument('--output', type=str, default="samakal_section_scraper_output.csv", help="Output CSV file")
    args = parser.parse_args()

    FROM_DATE = datetime.strptime(args.from_date, "%Y-%m-%d")
    TO_DATE = datetime.strptime(args.to_date, "%Y-%m-%d")

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
    results = []
    seen_urls = set()
    driver = None
    try:
        for section_url in SECTIONS:
            for page in tqdm(range(1, 6), desc=f"Section: {section_url}"):
                paged_url = section_url + f"?page={page}"
                try:
                    if driver:
                        driver.quit()
                    chrome_options = Options()
                    chrome_options.add_argument("--headless=new")
                    chrome_options.add_argument("--no-sandbox")
                    chrome_options.add_argument("--disable-dev-shm-usage")
                    ua = random.choice(USER_AGENTS)
                    chrome_options.add_argument(f'user-agent={ua}')
                    driver = webdriver.Chrome(service=Service(CHROMEDRIVER_PATH), options=chrome_options)
                    driver.get(paged_url)
                    # Dynamic page scrolling to load all content
                    scroll_pause_time = 1.0
                    last_height = driver.execute_script("return document.body.scrollHeight")
                    for _ in range(10):  # Scroll 5 times (adjust as needed)
                        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                        time.sleep(scroll_pause_time)
                        new_height = driver.execute_script("return document.body.scrollHeight")
                        if new_height == last_height:
                            break
                        last_height = new_height
                    time.sleep(random.uniform(2, 4))
                    soup = BeautifulSoup(driver.page_source, "html.parser")
                    links = soup.find_all("a", href=True)
                    for a in links:
                        href = a["href"]
                        # Only follow article links
                        if "/article/" in href:
                            if href.startswith("/"):
                                href = "https://samakal.com" + href
                            if href not in seen_urls and "samakal.com" in href:
                                seen_urls.add(href)
                                article = extract_article(href)
                                if article:
                                    # Debug: print or log the title and description
                                    logging.info(f"[DEBUG] Article: {article['title'][:80]} | {article['description'][:80]}")
                                    # Check for keywords in title or content
                                    if any(kw in article["title"] or kw in article["description"] for kw in KEYWORDS):
                                        article["keyword"] = next((kw for kw in KEYWORDS if kw in article["title"] or kw in article["description"]), "matched")
                                        results.append(article)
                except Exception as e:
                    logging.error(f"⚠️ Failed to fetch section {paged_url}: {e}")
    finally:
        if driver:
            driver.quit()
    df = pd.DataFrame(results)
    if not df.empty:
        df = df.drop_duplicates(subset=["url"])
        df = df[["title", "url", "published_date", "description", "source", "keyword"]]
        df.to_csv(args.output, index=False)
        logging.info(f"✅ Saved {len(df)} articles to '{args.output}'")
    else:
        logging.warning("⚠️ No articles found matching the criteria. No CSV file was created.")
