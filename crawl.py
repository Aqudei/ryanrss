import openpyxl
import pandas as pd
import os
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import json
import csv
import traceback as tb
import concurrent.futures
from urllib.parse import urlunparse, urlparse
load_dotenv()


INPUT_FILE = os.environ.get("INPUT_FILE")
HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/89.0.4389.82 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Referer': 'https://www.google.com/',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
}


def test_feed_url(base_url):
    """
    docstring
    """
    url_parts = list(urlparse(base_url))
    url_parts[2] = 'feed/'

    url = urlunparse(url_parts)

    try:
        response = requests.get(url, headers=HEADERS, timeout=20)
        if response.status_code == 200:
            return url, response.status_code
        else:
            return '', response.status_code
    except Exception as e:
        return

def crawl_feeds(url):
    """
    docstring
    """
    a_tags = set()

    response = requests.get(url, timeout=20, verify=False, headers=HEADERS)
    print(f"Response Status code: {response.status_code}")
    if response.status_code == 200:
        print("Parsing html...")
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('link')
        print("enumerating links...")
        for link in links:
            if 'rss' in link.get('type', ''):
                a_tags.add(link.get('href', ''))
            if '.feeds.' in link.get('href', ''):
                a_tags.add(link.get('href', ''))

        if len(a_tags) > 0:

            return {
                "feeds": a_tags,
                "url": url,
                "errors": "",
                "status": response.status_code
            }

    custom_feed_url = test_feed_url(url)
    if custom_feed_url:
        custom_url, custom_status = custom_feed_url
        return {
            "feeds": [custom_url],
            "url": url,
            "errors": '',
            "status": custom_status
        }

    return {
        "feeds": None,
        "url": url,
        "errors": f"No RSS link found",
        "status": response.status_code
    }


def to_absolute_feed_url(domain, url):
    """
    docstring
    """
    parts = list(urlparse(url))
    if parts[0]:
        return url


if __name__ == "__main__":
    print(f"Reading input file: {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)

    # We can use a with statement to ensure threads are cleaned up promptly
    with open("./output.csv", 'wt', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(('Domain', 'Publication Name',
                        'RSS', 'Remarks', 'Status Code'))

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            # Start the load operations and mark each future with its URL
            future_to_url = {executor.submit(
                crawl_feeds, row.Domain): row for row in df.itertuples()}
            for idx, future in enumerate(concurrent.futures.as_completed(future_to_url)):
                panda_row = future_to_url[future]
                print(f"Done processing [{idx+1}] {panda_row}")
                try:
                    data = future.result()
                    if data['feeds']:
                        for f in data['feeds']:
                            absolute_url = to_absolute_feed_url(
                                panda_row.Domain, f)
                            writer.writerow(
                                (panda_row.Domain, panda_row._2, absolute_url, "OK", data['status']))
                    else:
                        writer.writerow(
                            (panda_row.Domain, panda_row._2, '-', data['errors'], data['status']))
                except Exception as exc:
                    writer.writerow(
                        (panda_row.Domain, panda_row._2, '-', str(exc), 'Failure'))
                else:
                    print('%r page is %d bytes' % (panda_row, len(data)))
