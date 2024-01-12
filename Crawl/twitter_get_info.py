import asyncio
# from tqdm import tqdm
from playwright.sync_api import sync_playwright
# from p_tqdm import p_map
import pandas as pd
import json
import time


def scrape_profile(name: str) -> dict:
    """
    Scrape a X.com profile details e.g.: https://x.com/Scrapfly_dev
    """

    _xhr_calls = []
    url = f"https://x.com/{name}"

    def intercept_response(response):
        """capture all background requests and save them"""
        # we can extract details from background requests
        if response.request.resource_type == "xhr":
            _xhr_calls.append(response)
        return response

    with sync_playwright() as pw:        
        browser = pw.chromium.launch(headless=True)
        context = browser.new_context(viewport={"width": 1920, "height": 1080})
        page = context.new_page()

        # enable background request intercepting:
        page.on("response", intercept_response)
        
        # go to url and wait for the page to load
        page.goto(url)
        page.wait_for_selector("[data-testid='primaryColumn']")

        # find all tweet background requests:
        tweet_calls = [f for f in _xhr_calls if "UserBy" in f.url]
        for xhr in tweet_calls:
            data = xhr.json()
            return data['data']['user']['result']



if __name__ == "__main__":
    # batch_size = 10
    input_path = "questn_final.jsonl"
    output_path = "wallet_twitter_account.jsonl"
    data = pd.read_json(input_path, orient="records", lines=True, encoding="utf-8")
    usernames = data["twitter_username"].drop_duplicates().tolist()
    for i in usernames:
        try:
            twitter_profile = scrape_profile(i)
            twitter_profile['twitter_username'] = i
        except:
            twitter_profile = {}
        with open(output_path, "a", encoding="utf-8") as f:
            json.dump(twitter_profile, f, ensure_ascii=False)
            f.write("\n")
    
