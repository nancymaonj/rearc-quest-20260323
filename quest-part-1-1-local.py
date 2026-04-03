# Part 1: AWS S3 & Sourcing Datasets
# Republish this open dataset in Amazon S3 and share with us a link.
# You may run into 403 Forbidden errors as you test accessing this data. There is a way to comply with the BLS data access policies and re-gain access to fetch this data programatically - we have included some hints as to how to do this at the bottom of this README in the Q/A section.
# Script this process so the files in the S3 bucket are kept in sync with the source when data on the website is updated, added, or deleted.
# Don't rely on hard coded names - the script should be able to handle added or removed files.
# Ensure the script doesn't upload the same file more than once.

# write a python  function to fetch files from link: https://download.bls.gov/pub/time.series/pr/
# and save the files to a local directory "bls_downloads-local-20260402"
# avoid access forbiddnen issue

####################################################################### aws
# import urllib.request
# import re
# import os
# import time

# def fetch_bls_files():
#     # 1. Configuration
#     source_url = "https://download.bls.gov/pub/time.series/pr/"
#     local_dir = "bls_downloads-local-20260402"
    
#     # Critical: BLS will block you without a realistic User-Agent.
#     # It is recommended to include an email so they can contact you if needed.
#     headers = {
#         'User-Agent': 'DataCollector/1.0 (contact: yourname@example.com) Mozilla/5.0',
#         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
#         'Connection': 'keep-alive'
#     }

#     # Create local directory
#     if not os.path.exists(local_dir):
#         os.makedirs(local_dir)
#         print(f"Created directory: {local_dir}")

#     try:
#         # 2. Get the index page to find file names
#         print(f"Connecting to {source_url}...")
#         req = urllib.request.Request(source_url, headers=headers)
        
#         with urllib.request.urlopen(req) as response:
#             html_content = response.read().decode('utf-8')

#         # 3. Parse the file names
#         # We look for links that stay within the /pr/ directory
#         links = re.findall(r'href="/pub/time\.series/pr/([^"\s>]+)"', html_content)
#         # Filter: Remove duplicates and ignore sub-directories (links ending in /)
#         file_list = sorted(list(set([f for f in links if not f.endswith('/')])))

#         if not file_list:
#             print("No files found to download.")
#             return

#         print(f"Found {len(file_list)} files. Starting download...")

#         # 4. Download each file
#         for file_name in file_list:
#             file_url = source_url + file_name
#             target_path = os.path.join(local_dir, file_name)
            
#             print(f"Downloading: {file_name}...", end=" ", flush=True)
            
#             try:
#                 file_req = urllib.request.Request(file_url, headers=headers)
#                 with urllib.request.urlopen(file_req) as file_response:
#                     with open(target_path, 'wb') as local_file:
#                         local_file.write(file_response.read())
#                 print("Done.")
                
#                 # Ethical Scraping: Small delay to avoid triggering rate limits
#                 time.sleep(0.5) 
                
#             except Exception as file_err:
#                 print(f"Failed! Error: {file_err}")

#         print(f"\nAll downloads complete. Files saved to: {os.path.abspath(local_dir)}")

#     except urllib.error.HTTPError as e:
#         print(f"Access Denied (403). Try updating the User-Agent or wait 15 minutes.")
#     except Exception as e:
#         print(f"An unexpected error occurred: {e}")

# if __name__ == "__main__":
#     fetch_bls_files()
#######################################################################

import os
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup

BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
LOCAL_DIR = "bls_downloads-20260403"

INVALID_CHARS = '<>:"\\|?*'

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"
}

def is_valid_href(href):
    if not href or href in ("../",):
        return False
    if href.startswith("?"):
        return False
    if href.startswith("http"):  # skip external links
        return False
    return True

def clean_name(name):
    return "".join(c for c in name if c not in INVALID_CHARS)

def is_within_base(url):
    return url.startswith(BASE_URL)

def download_file(url, local_path):
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with requests.get(url, headers=HEADERS, stream=True) as r:
        r.raise_for_status()
        with open(local_path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

def crawl_directory(url, local_dir):
    local_dir = os.path.join(os.getcwd(), local_dir)
    print(f"Crawling: {url}, saving to: {local_dir}")
    response = requests.get(url, headers=HEADERS)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    for link in soup.find_all("a"):
        href = link.get("href")

        if not is_valid_href(href):
            continue

        full_url = urljoin(url, href)

        # 🔒 Stay strictly inside /pr/
        if not is_within_base(full_url):
            continue

        clean_href = clean_name(href)
        file_only = clean_href.split('/')[-1]
        # print(f"hello {local_dir}, {file_only}")

        local_path = os.path.join(local_dir, file_only)
        print(f"Found link: {href} -> {full_url} -> {local_path}")

        if href.endswith("/"):
            crawl_directory(full_url, local_path)
        else:
            print(f"Downloading: {full_url} -> {local_path}")
            download_file(full_url, local_path)

if __name__ == "__main__":
    crawl_directory(BASE_URL, LOCAL_DIR)