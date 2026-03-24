# s3://rearc-quest-20260323-876784288665-us-east-1-an/quest-part-1/bls_pr_data/

import os
import re
import time
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, unquote

BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
OUTPUT_DIR = "bls_pr_data"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/120.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

session = requests.Session()
session.headers.update(HEADERS)

visited = set()


def is_valid_url(url):
    return url.startswith(BASE_URL)


def safe_filename(url):
    parsed = urlparse(url)
    filename = os.path.basename(parsed.path)
    filename = unquote(filename)
    filename = re.sub(r'[<>:"/\\|?*]', "_", filename)
    return filename if filename else "index.html"


def download_file(url, path):
    for attempt in range(3):
        try:
            with session.get(url, stream=True, timeout=30) as r:
                r.raise_for_status()
                with open(path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        if chunk:
                            f.write(chunk)
            return True
        except Exception as e:
            print(f"Retry {attempt+1} failed: {url} ({e})")
            time.sleep(2)
    return False


def crawl(url, local_dir):
    if url in visited:
        return
    visited.add(url)

    print(f"Scanning: {url}")
    os.makedirs(local_dir, exist_ok=True)

    try:
        response = session.get(url, timeout=30)
        response.raise_for_status()
    except Exception as e:
        print(f"FAILED to access {url}: {e}")
        return

    soup = BeautifulSoup(response.text, "html.parser")

    for link in soup.find_all("a"):
        href = link.get("href")

        if not href or href == "../":
            continue

        full_url = urljoin(url, href)

        if not is_valid_url(full_url):
            continue

        if href.endswith("/"):
            sub_dir = os.path.join(local_dir, href.strip("/"))
            crawl(full_url, sub_dir)
        else:
            filename = safe_filename(full_url)
            file_path = os.path.join(local_dir, filename)

            if os.path.exists(file_path):
                print(f"Skipping: {file_path}")
                continue

            print(f"Downloading: {full_url}")
            if download_file(full_url, file_path):
                time.sleep(0.5)  # slower = less blocking


if __name__ == "__main__":
    crawl(BASE_URL, OUTPUT_DIR)
    print("Done.")