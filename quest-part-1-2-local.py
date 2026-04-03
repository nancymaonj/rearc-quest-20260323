# Script this process so the files in the S3 bucket are kept in sync with the source when data on the website is updated, added, or deleted.
# Don't rely on hard coded names - the script should be able to handle added or removed files.
# Ensure the script doesn't upload the same file more than once.

# write a python lambda function to check if files in the s3 bucket (rearc-quest-20260402-us-east-1-an-876784288665-us-east-1-an)
# are in sync with the files from link: https://download.bls.gov/pub/time.series/pr/

# write a python function to check if files in the directory (bls_downloads-20260403)
# are in sync with the files from link: https://download.bls.gov/pub/time.series/pr/
# run in windows
# and if not in sync, print the files that are missing locally or have size mismatch with the remote files.
# then sych the files that are missing or have size mismatch by downloading them from the remote source.


#######################################################################
# import urllib.request
# import re
# import os

# def check_bls_sync():
#     # 1. Configuration
#     source_url = "https://download.bls.gov/pub/time.series/pr/"
#     local_dir = "bls_downloads-20260403"
    
#     # Headers to avoid 403 Forbidden
#     headers = {
#         'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
#         'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
#     }

#     if not os.path.exists(local_dir):
#         print(f"Error: Local directory '{local_dir}' not found.")
#         return

#     try:
#         # 2. Fetch the directory index
#         req = urllib.request.Request(source_url, headers=headers)
#         with urllib.request.urlopen(req) as response:
#             html = response.read().decode('utf-8')

#         # 3. Extract filenames using Case-Insensitive Regex
#         # Based on your HTML: <A HREF="/pub/time.series/pr/pr.class">
#         pattern = r'HREF="/pub/time\.series/pr/([^"]+)"'
#         found_files = re.findall(pattern, html, re.IGNORECASE)
        
#         # Deduplicate and ensure they are actual data files (containing 'pr.')
#         remote_filenames = sorted(list(set([f.split('/')[-1] for f in found_files if 'pr.' in f])))

#         print(f"Checking sync for {len(remote_filenames)} remote files...\n")

#         results = {"in_sync": [], "missing": [], "mismatch": []}

#         for fname in remote_filenames:
#             # Construct Windows-safe path
#             local_path = os.path.normpath(os.path.join(local_dir, fname))
#             file_url = source_url + fname

#             # 4. Get Remote Metadata (HEAD request)
#             head_req = urllib.request.Request(file_url, headers=headers, method='HEAD')
            
#             try:
#                 with urllib.request.urlopen(head_req) as head_res:
#                     remote_size = int(head_res.getheader('Content-Length'))
                
#                 if not os.path.exists(local_path):
#                     results["missing"].append(fname)
#                 else:
#                     local_size = os.path.getsize(local_path)
#                     # Compare sizes
#                     if local_size == remote_size:
#                         results["in_sync"].append(fname)
#                     else:
#                         results["mismatch"].append(f"{fname} (Local: {local_size} vs Remote: {remote_size})")
            
#             except Exception as e:
#                 print(f"Could not verify {fname}: {e}")

#         # 5. Summary Report
#         print("--- Windows Sync Report ---")
#         print(f"✅ Identical:      {len(results['in_sync'])}")
#         print(f"❌ Missing Locally: {len(results['missing'])}")
#         print(f"⚠️  Size Mismatch:   {len(results['mismatch'])}")

#         if results["missing"]:
#             print(f"\nMissing Files:\n - " + "\n - ".join(results["missing"]))
#         if results["mismatch"]:
#             print(f"\nMismatched Files (Likely updated on server):\n - " + "\n - ".join(results["mismatch"]))

#     except Exception as e:
#         print(f"Sync check failed: {e}")

# if __name__ == "__main__":
#     check_bls_sync()

#######################################################################    
import urllib.request
import re
import os
import time

def sync_bls_data():
    # 1. Configuration
    source_url = "https://download.bls.gov/pub/time.series/pr/"
    local_dir = "bls_downloads-20260403"
    
    # Headers to bypass 403 Forbidden
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8'
    }

    # Ensure local directory exists
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)
        print(f"Created directory: {local_dir}")

    try:
        # 2. Get the remote file list
        print(f"Connecting to {source_url}...")
        req = urllib.request.Request(source_url, headers=headers)
        with urllib.request.urlopen(req) as response:
            html = response.read().decode('utf-8')

        # Regex handles <A HREF="..."> and extracts the filename
        # Using re.IGNORECASE because BLS uses uppercase HREF
        pattern = r'HREF="/pub/time\.series/pr/([^"]+)"'
        remote_filenames = re.findall(pattern, html, re.IGNORECASE)
        
        # Filter for actual data files and deduplicate
        remote_files = sorted(list(set([f.split('/')[-1] for f in remote_filenames if 'pr.' in f])))

        to_download = []

        # 3. Audit Phase: Check for Mismatches
        print(f"Auditing {len(remote_files)} files...")
        for fname in remote_files:
            local_path = os.path.normpath(os.path.join(local_dir, fname))
            file_url = source_url + fname
            
            # HEAD request to get size only
            try:
                head_req = urllib.request.Request(file_url, headers=headers, method='HEAD')
                with urllib.request.urlopen(head_req) as head_res:
                    remote_size = int(head_res.getheader('Content-Length'))
                
                if not os.path.exists(local_path):
                    print(f" [MISSING] {fname}")
                    to_download.append(fname)
                else:
                    local_size = os.path.getsize(local_path)
                    if local_size != remote_size:
                        print(f" [MISMATCH] {fname} (Local: {local_size}b, Remote: {remote_size}b)")
                        to_download.append(fname)
            except Exception as e:
                print(f" [ERROR] Could not check {fname}: {e}")

        # 4. Action Phase: Sync missing/mismatched files
        if not to_download:
            print("\n✅ Everything is in sync. No downloads needed.")
            return

        print(f"\nStarting sync for {len(to_download)} files...")
        for fname in to_download:
            local_path = os.path.normpath(os.path.join(local_dir, fname))
            file_url = source_url + fname
            
            print(f" Downloading: {fname}...", end=" ", flush=True)
            try:
                get_req = urllib.request.Request(file_url, headers=headers)
                with urllib.request.urlopen(get_req) as get_res:
                    with open(local_path, 'wb') as f:
                        f.write(get_res.read())
                print("Done.")
                # Ethical delay to avoid rate limiting
                time.sleep(0.3)
            except Exception as e:
                print(f"Failed! {e}")

        print("\n✨ Sync complete.")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    sync_bls_data()