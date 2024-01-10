import requests
from p_tqdm import p_map
import time
from tqdm import tqdm
import pandas as pd
import json

def get_info_per_page(url, params, fields):
    data = requests.get(url, params=params).json()
    return [ [i[field] for field in fields] for i in data['result']['data']]

def crawl_quester():
    url_com = "https://api.questn.com/consumer/community/recommended_list/"
    url_quest = "https://api.questn.com/consumer/explore/entity_list/"
    url_quester = "https://api.questn.com/consumer/quest/user_participants/"
    com = requests.get(url_com)
    com = com.json()
    num_pages_com = com['result']['num_pages']
    counter = 0
    a = 3
    for page_com in tqdm(range(a, num_pages_com+1), total=num_pages_com-a+1 ):
        community_name = get_info_per_page(url_com, {'page':page_com}, ['url'])
        for name in community_name:
            try:
                quest = requests.get(url_quest, params={"community_url": name[0]} ).json()
                num_pages_quest = quest['result']['num_pages']
                for page_quest in range(1, num_pages_quest+1):
                    quest_ids = get_info_per_page(url_quest, {'page':page_quest, "community_url": name}, ['id'])
                    for quest_id in quest_ids:
                        try:
                            data = requests.get(url_quester, params={"quest_id": quest_id[0]} ).json()
                            num_pages_quester = data['result']['num_pages']
                            for page in range(1, num_pages_quester+1):
                                result = get_info_per_page(url_quester, {'page':page, "quest_id": quest_id}, ['user_id', 'user_address', 'twitter_username'])
                                for user in result:
                                    try:
                                        with open("data_questn_final.jsonl", "a") as f:
                                            json.dump({'user_id': user[0], 'user_address':user[1],'twitter_username': user[2]}, f, ensure_ascii=False)
                                            f.write("\n")
                                        counter += 1
                                    except:
                                        with open("error_data_questn_final.jsonl", "a") as f:
                                            f.write(str(user) + "\n")
                                        print(counter)
                                        continue
                        except:
                            with open("error_data_questn_final.jsonl", "a") as f:
                                f.write(str(user) + "\n")
            except:
                with open("error_data_questn_final.jsonl", "a") as f:
                    f.write(str(user) + "\n")

if __name__ == "__main__":
    crawl_quester()
