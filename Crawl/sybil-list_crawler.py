import pandas as pd

if __name__ == "__main__":
    data = pd.read_json("data_sybil-list.json")
    user_info = pd.DataFrame(range(len(data.columns)), columns=['user_id'])
    user_info['user_address'] = data.columns
    user_info['twitter_username'] = [eval(str(data[i].iloc[0]))['handle']  if str(data[i].iloc[0]) != "nan" else "" for i in data.columns]
    full_data = user_info.to_json("data_sybil-list_final.jsonl", orient='records', lines=True, force_ascii=False)
