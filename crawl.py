import openpyxl
import pandas as pd
import os
import requests
from dotenv import load_dotenv
from lxml import etree
from bs4 import BeautifulSoup
import json
import csv
import traceback as tb
load_dotenv()


INPUT_FILE = os.environ.get("INPUT_FILE")


def crawl_feeds(url):
    """
    docstring
    """
    a_tags = set()
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/109.0'}

    response = requests.get(url, timeout=20, verify=False, headers=headers)
    print(f"Response Status code: {response.status_code}")
    if response.status_code == 200:
        print("Parsing html...")
        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')
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
                "errors": ""
            }

    return {
        "feeds": None,
        "url": url,
        "errors": f"No RSS link found | Status code: {response.status_code}"
    }


if __name__ == "__main__":
    print(f"Reading input file: {INPUT_FILE}")
    df = pd.read_excel(INPUT_FILE)

    with open("./output.csv", 'wt', newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(('Domain', 'Publication Name', 'RSS', 'Remarks'))
        for idx, row in enumerate(df.itertuples()):
            try:
                print(f"Processing [{idx+1}]\t{row.Domain}")
                urls = crawl_feeds(row.Domain)
                if urls['feeds']:
                    for f in urls['feeds']:
                        writer.writerow((row.Domain, row._2, f, "OK"))
                else:
                    writer.writerow((row.Domain, row._2, '', urls['errors']))
            except Exception as e:
                writer.writerow((row.Domain, row._2, '', tb.format_exc()))
