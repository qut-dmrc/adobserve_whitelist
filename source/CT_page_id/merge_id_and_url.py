import pandas as pd
import numpy as np
import json

dataset = "whitelist_test"
source_file_with_page_urls = "Ad monitoring targets"
url_col = "Page URL"
with open(f'results/{dataset}.json','r') as f:
    accounts = json.load(f)

handle_to_id_map = dict()
handle_to_name_map = dict()
for account in accounts:
    handle_to_id_map[account['pageAlias']] = account['id']
    handle_to_name_map[account['pageAlias']] = account['name']

df = pd.read_csv(source_file_with_page_urls+".csv")
'''
1. following three lines should be commented out once ids are finalised
'''
df['id'] = df.apply(lambda x: int(handle_to_id_map[x[url_col].replace('https://www.facebook.com/','').replace('https://facebook.com/','').replace('/','')]) if x[url_col].replace('https://www.facebook.com/','').replace('https://facebook.com/','').replace('/','') in handle_to_id_map.keys() else None,axis=1)
df['name'] = df.apply(lambda x: handle_to_name_map[x[url_col].replace('https://www.facebook.com/','').replace('https://facebook.com/','').replace('/','')] if x[url_col].replace('https://www.facebook.com/','').replace('https://facebook.com/','').replace('/','') in handle_to_name_map.keys() else "",axis=1)
df[[url_col,'name','id']].to_csv("url_ids_1st_iteration.csv",index=False)

# 2. further gather ids for missed urls using facebook ad library

# 3. merge final df with ids with original source file, need to get handle to id map again.
url_to_id_map = dict()
df2 = pd.read_csv("url_ids_1st_iteration.csv")[[url_col,'id']]
for index,row in df2.iterrows():
    url_to_id_map[row[url_col]] = int(row['id']) if not np.isnan(row['id']) else row['id']
df['id'] = df.apply(lambda x: str(int(url_to_id_map[x[url_col]])) if not np.isnan(url_to_id_map[x[url_col]]) else "",axis=1)
df.to_csv(source_file_with_page_urls+'_final.csv',index=False)

## to copy and paste in the source file online [easier than uploading a new file, prevent truncated id]
ids = pd.read_csv(source_file_with_page_urls+'_final.csv')['id'].tolist()
with open("ids.txt",'w') as f:
    for _id in ids:
        if np.isnan(_id):
            continue
        new_id = str(int(_id)) if not np.isnan(_id) else ""
        f.write(new_id+'\n')
