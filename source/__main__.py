## code
from configreader import Config
from api import FB_AD_API
from pusher import Pusher
from schema import PAGE_SCHEMA
from reconstructrawjson import downloadTablesFromBQ, reconstructRawJSON, downloadFileFroms3
## libraries
from datetime import datetime
from os import environ, remove, path, walk
from decouple import config
import pandas as pd
import boto3
import json
import simplejson
import time
import random
import csv
import subprocess

def get_page_ids(pages,api,dataset):
    pages_info = []
    pages_wrong_match = []
    page_id_map = []
    for page in pages:
        page_info = api.get_page_info(page)
        page_row = [page]
        if page_info is None:
            page_row.extend([""])
            continue
        elif page_info['name']==page:
            pages_info.append(page_info)
            page_row.append(page_info['id'])
        else:
            print("wrong match")
            page_row.append(page_info['name'])
        page_id_map.append(page_row)
    
    with open("pages.csv", 'w', encoding='utf-8', errors='ignore', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(page_id_map)
    page_info_name = f'results/page_info{dataset}.json'
    with open(page_info_name,'w') as f:
        json.dump(pages_info,f)
    with open('results/wrong_match.json','w') as f:
        json.dump(pages_wrong_match,f)
    api.pusher.push_to_gbq(page_info_name.split('/')[-1], PAGE_SCHEMA)
    # return [page['id'] for page in pages_info] # return this if the list of pages in config.py is not in ID form
    return pages

def push_json_to_s3(bucket_name="", src_folder="",dest_folder=""):
    print("pushing to s3 {}".format(bucket_name))
    s3 = boto3.client('s3', aws_access_key_id=config('AWS_ACCESS_KEY_ID'), aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'))
    folder = src_folder
    for root,dirs,files in walk(folder):
        for file in files:
            if file.endswith('.json'):
                with open(path.join(root,file),'r') as f:
                    data = json.load(f)
                    s3.put_object(Body=json.dumps(data),
                                    Bucket=bucket_name,
                                    Key=dest_folder+file,
                                    ContentType='application/json')
            else:
                s3.upload_file(path.join(root,file),bucket_name,dest_folder+file)
                # remove(path.join(root,file))

def combine_new_data_with_existing(tablenames, bucket_name):
    dataframes = []
    print("downloading CSVs from GBQ")
    for name in tablenames:
        if 'page' in name:
            dataframes.append(downloadTablesFromBQ(name))
        else:
            ## so that ads that were scraped from today backdates to 4 days ago are all included, reason being that the scraping may take more than one day. 
            # If scrapedDate is only set to today's date, ad data scraped yesterday or the day before will not be included
            dataframes.append(downloadAlcoholLinkage(name,"WHERE ScrapedDate BETWEEN '{}' AND '{}'".format((datetime.now() - timedelta(days=4)).strftime('%Y-%m-%d'),datetime.now().strftime('%Y-%m-%d'))))
    [df_page,df_ad,df_card,df_category] = dataframes 
    df_page = pd.merge(df_page,df_category, how='inner', on='id')
    # df_page = pd.read_csv("page_info.csv",low_memory=False)
    # df_ad = pd.read_csv("whitelist_test.csv",low_memory=False)
    # df_card = pd.read_csv("ad_snapshot_card.csv",low_memory=False)
    ad_map = {}
    # old raw json combined
    remoteDirectoryName = 'raw_json'
    dest_folder = "raw_json"
    print("downloading old combined raw json")
    downloadFileFroms3(bucket_name, remoteDirectoryName, bucket_name+".json", dest_folder)
    with open("raw_json/"+bucket_name+".json",'r') as f:
        print("adding old combined raw json")
        raw_json = json.load(f)
        for ad in raw_json:
            ad_archive_id = int(ad['adArchiveID'])
            ad_map[ad_archive_id] = ad
    # new raw json
    for _,_,files in walk('raw_json'):
        for file in files:
            if '20' in file:
                with open("raw_json/"+file,'r') as f:
                # with open("raw_json/alcohol-linkage_last.json",'r') as f:
                    print("adding new raw json")
                    new_ads = json.load(f)
                    for ad in new_ads:
                        ad_archive_id = int(ad['adArchiveID'])
                        if ad_archive_id in list(ad_map.keys()):
                            ad_map[ad_archive_id] = {**ad_map[ad_archive_id], **ad} # ads with the same ad_archive_id will replace the value of shared attrbiutes with the old one 
                        else:
                            ad_map[ad_archive_id] = ad

    for col in df_ad.columns:
        if 'datetime64' in str(df_ad[col].dtype):
            df_ad[col] = df_ad[col].dt.strftime("%Y-%m-%d %H:%M:%S+10")
    print("looping through existing dataframe")
    for index, row in df_ad.iterrows():
        ad_json = row.to_dict()
        ad_archive_id = int(ad_json['adArchiveID'])
        if ad_archive_id in list(ad_map.keys()):
            ad = {**ad_map[ad_archive_id],**reconstructRawJSON(row, df_page, df_card)} # combine page and card info with ad data into one json object
        else:
            ad = reconstructRawJSON(row, df_page, df_card)
        if 'timeframe' not in list(ad.keys()) or ad['timeframe']==None:
            ad['timeframe'] = []
        ad['timeframe'].append("({},{})".format(ad['startDate'],ad['endDate']))
        ad_map[ad_archive_id] = ad
    with open('raw_json/{}.json'.format(bucket_name), 'w') as f:
        f.write(simplejson.dumps(list(ad_map.values()), ignore_nan=True, default=str))

def fflate(dataset):
    print("Compressing data")
    p = subprocess.Popen(['node','fflate.js',dataset], stdout=subprocess.PIPE)
    p.wait()

def mainLoop(dataset, firstRun=False, paused=False):
    # config
    config = Config(dataset)
    pusher = Pusher(config)
    api = FB_AD_API(pusher,dataset)
    # get ids
    page_ids_collected = True
    if not page_ids_collected:
        ids = get_page_ids(config.pages, api, dataset)
        ids = []
        with open("page_idset.txt",'w') as f:
            for _id in ids:
                f.write(str(_id)+"\n")
    else:
        ids = config.pages
    # paused=True #for interrupted process
    raw_json = []
    if paused:
        with open(path.join('raw_json/',dataset+".json"),'r') as f:
            raw_json = json.load(f)
    for _id in ids:
        print('collecting id', _id)
        ads = api.get_ads(_id)
        if ads != None:
            raw_json.extend(ads)
        else:
            print("Response returned NoneType")
        with open("processed_page_ids.txt",'a+') as f:
            f.write(_id+"\n")
        # all raw json data
        with open(path.join('raw_json/',datetime.now().strftime("%Y-%m-%d"))+".json",'w') as f:
            json.dump(raw_json, f)
        if firstRun:
            with open(path.join('raw_json/',dataset+".json"),'w') as f:
                json.dump(raw_json, f)
        time.sleep(random.randint(5, 10))    
    # api.testdownloadMedia()


if __name__ == '__main__':
    tablenames = ['page_info', 'ad_main','ad_snapshot_card','page_category']
    dataset = "whitelist_test"
    mainLoop(dataset, False, False)
    # combine_new_data_with_existing(tablenames, dataset)
    push_json_to_s3(dataset,"raw_json/","raw_json/")
    # fflate(dataset)
    # push_json_to_s3("bucket_for_compressed_file","compressed","")