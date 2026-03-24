# a lambda function to process so the files in the S3 bucket (arn:aws:s3:::rearc-quest-20260323-876784288665-us-east-1-an) 
# are kept in sync with the source (https://download.bls.gov/pub/time.series/pr/),  when data on the website is updated, added, or deleted.
# the script should be able to handle added or removed files, 
# Ensure the script doesn't upload the same file more than once.
# only for files in the pr directory, not the entire bls.gov website.

import boto3
import hashlib
import time
import re
from html.parser import HTMLParser
from urllib.parse import urljoin, urlparse, unquote
from urllib.request import Request, build_opener, HTTPCookieProcessor
from http.cookiejar import CookieJar

BASE_URL = "https://download.bls.gov/pub/time.series/pr/"
BUCKET = "rearc-quest-20260323-876784288665-us-east-1-an"
PREFIX = "quest-part-1/"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Referer": "https://download.bls.gov/",
    "Connection": "keep-alive"
}

s3 = boto3.client("s3")

# --- HTTP opener with cookies (helps reduce 403s) ---
cookie_jar = CookieJar()
opener = build_opener(HTTPCookieProcessor(cookie_jar))

visited = set()
source_urls = set()


# --- HTML parser for directory listings ---
class LinkParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []

    def handle_starttag(self, tag, attrs):
        if tag == "a":
            for k, v in attrs:
                if k == "href":
                    self.links.append(v)


# --- helpers ---
def is_valid_url(url: str) -> bool:
    # 🔒 stay strictly inside /pr/
    return url.startswith(BASE_URL)


def s3_key_from_url(url: str) -> str:
    parsed = urlparse(url)
    rel = parsed.path.replace("/pub/time.series/pr/", "")
    rel = unquote(rel)
    rel = re.sub(r'[<>:"\\|?*]', "_", rel)
    return PREFIX + rel


def http_get(url: str):
    req = Request(url, headers=HEADERS)
    with opener.open(req, timeout=30) as r:
        return r.read(), r.headers


# --- crawl source tree ---
def crawl(url: str):
    if url in visited:
        return
    visited.add(url)

    print(f"Scanning: {url}")

    try:
        html, _ = http_get(url)
    except Exception as e:
        print(f"FAILED: {url} ({e})")
        return

    parser = LinkParser()
    parser.feed(html.decode("utf-8", errors="ignore"))

    for href in parser.links:
        if not href or href == "../":
            continue

        full = urljoin(url, href)

        if not is_valid_url(full):
            continue

        if href.endswith("/"):
            crawl(full)
        else:
            source_urls.add(full)


# --- list S3 state ---
def list_s3_keys():
    keys = set()
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            keys.add(obj["Key"])

    return keys


# --- hashing ---
def md5_bytes(data: bytes) -> str:
    return hashlib.md5(data).hexdigest()


# --- upload only if changed ---
def upload_if_needed(url: str):
    key = s3_key_from_url(url)

    try:
        content, _ = http_get(url)
    except Exception as e:
        print(f"Download failed: {url} ({e})")
        return

    new_hash = md5_bytes(content)

    # check existing object
    try:
        head = s3.head_object(Bucket=BUCKET, Key=key)
        etag = head["ETag"].strip('"')

        if etag == new_hash:
            print(f"Unchanged: {key}")
            return
    except s3.exceptions.NoSuchKey:
        pass
    except Exception:
        pass

    print(f"Uploading: {key}")
    s3.put_object(Bucket=BUCKET, Key=key, Body=content)


# --- delete removed files ---
def delete_removed(source_keys, s3_keys):
    to_delete = s3_keys - source_keys

    for key in to_delete:
        print(f"Deleting: {key}")
        s3.delete_object(Bucket=BUCKET, Key=key)


# --- Lambda entrypoint ---
def lambda_handler(event, context):
    # warm-up request (reduces 403 likelihood)
    try:
        http_get("https://download.bls.gov/")
        time.sleep(1)
    except:
        pass

    # 1. crawl source
    crawl(BASE_URL)

    # 2. compute source key set
    source_keys = {s3_key_from_url(u) for u in source_urls}

    # 3. list current S3 objects
    s3_keys = list_s3_keys()

    # 4. sync: upload new/changed
    for url in source_urls:
        upload_if_needed(url)
        time.sleep(0.3)  # be polite

    # 5. sync: delete removed
    delete_removed(source_keys, s3_keys)

    return {
        "statusCode": 200,
        "body": f"Synced {len(source_keys)} files"
    }