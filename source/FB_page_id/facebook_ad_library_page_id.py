import requests
import json
import os
import csv
import pandas as pd
def get_page_info(page):
    # replace with your session credentials
    cookies = {}

    headers = {}

    params = {}

    data = {}
    # end replace

    print("getting page "+page["name"])
    response = requests.post(
        'https://www.facebook.com/ads/library/async/search_typeahead/',
        params=params,
        cookies=cookies,
        headers=headers,
        data=data,
    )
    try:
        page_info = [json.loads(response.content.decode('UTF-8')[9:])['payload']['pageResults'][0]]
        print('ID', page_info[0]['id'])
        return page_info[0]
    except:
        print('Couldn\'t obtain ID')
        return None

def get_page_ids(pages,dataset):
    pages_info = []
    pages_wrong_match = []
    page_id_map = []
    for page in pages:
        page_info = get_page_info(page)
        page_row = [page['name']]
        if page_info is None:
            page_row.extend([""])
        else:
            page_row.extend([page_info['id']])
            pages_info.append(page_info)
        page_id_map.append(page_row)
    os.mkdir('results')
    page_info_name = f'results/{dataset}.json'
    with open(page_info_name,'w') as f:
        json.dump(pages_info,f)
    with open("pages.csv", 'w', encoding='utf-8', errors='ignore', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(page_id_map)
    
# brand name and url provided
df = pd.read_csv('Ad monitoring targets.csv')
pages = []
for index,row in df.iterrows():
    page = dict()
    page['name'] = row['Trading As'] if 'Trading As' in row else "" # page name
    page['handle'] = row['Page ID'].replace('https://www.facebook.com/','').replace('https://facebook.com/','').replace('/','') # get handle from URL
    pages.append(page)
get_page_ids(pages,"whitelist_test")
# cross-check with advertiser data downloaded from https://www.facebook.com/ads/library/report/?source=nav-header
