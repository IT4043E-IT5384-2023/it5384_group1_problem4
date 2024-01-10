import requests
from p_tqdm import p_map
import time
from tqdm import tqdm
import pandas as pd
import json
from unidecode import unidecode


def preprocessing(text):
    alphabet = ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o',
                'p','q','r','s','t','u','v','w','x','y','z','.','-','0','1','2','3','4','5','6','7','8','9']
    return "".join([i for i in text if unidecode(i).lower() in alphabet])

def get_info_per_page(url, params, fields):
    data = requests.get(url, params=params).json()
    return [[i[field] for field in fields] for i in data["pageProps"]['data']]

if __name__ == "__main__":
    num_pages = 10354
    # num_pages = 5
    url = f"https://www.walletlabels.xyz/_next/data/39v5jPlcEJfB9dZpk9A4d/socials.json"
    address_url = "https://ensdata.net/"
    a = 0
    counter = 0

    for page in tqdm(range(1, num_pages+1), total=num_pages):    
        result = get_info_per_page(url, {'page':page, "per_page": 10}, ['id','handle','ens'])
        for user in tqdm(result[a:], total=len(result[a:])):
            try:
                with open("data_wallet_labels_final.jsonl", "a") as f:
                    json.dump({'user_id': user[0], 'user_address':requests.get(address_url+preprocessing(user[2])).json()['address'],'twitter_username': user[1]}, f, ensure_ascii=False)
                    f.write("\n")
                counter += 1
            except:
                with open("error_data_wallet_labels_final.jsonl", "a") as f:
                    f.write(str(user) + "\n")
                print(counter)
                continue

