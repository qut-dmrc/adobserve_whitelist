from google.cloud import bigquery
from decouple import config
import pandas as pd
import json
import os
import math
import numpy
import simplejson
from datetime import datetime
import boto3

def downloadFileFroms3(bucketName, remoteDirectoryName, key, dest_folder):
    s3 = boto3.resource('s3', aws_access_key_id=config('AWS_ACCESS_KEY_ID'), aws_secret_access_key=config('AWS_SECRET_ACCESS_KEY'))
    bucket = s3.Bucket(bucketName)
    bucket.download_file(remoteDirectoryName+"/"+key, os.path.join(dest_folder,key))

def readFromBQ(credential_file, query_string):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_file
    bqclient = bigquery.Client()
    # Download query results.
    df = (
        bqclient.query(query_string)
        .result()
        .to_dataframe()
    )
    return df

def downloadTablesFromBQ(name,additionalCondition=""):
    page_query_string = """
        SELECT
        *
        FROM `project.dataset.%s` 
        %s
        """ % (name, additionalCondition)
    page_data = readFromBQ(config('BQ_CREDENTIAL_FILE'), page_query_string)
    page_data.to_csv(name+".csv", index=None)
    return page_data

def CSVtoJSON(meta_filepath, downsizeto=None):
    df = pd.read_csv(meta_filepath, dtype='str')
    header = df.columns.values
    if downsizeto:
        length = min(df.shape[0], downsizeto)
    else:
        length = df.shape[0]
    metadata = []
    for i in range(length): # number of rows shape:(row, column)
        row = df.iloc[i,1:].to_dict()
        metadata.append(row)
    with open(meta_filepath.split('.')[0]+'.json','w') as f:
        json.dump(metadata, f)

def objectTypeCheck(obj):
    for key in obj:
        if type(obj[key]).__module__ == 'numpy':
            if type(obj[key]) == numpy.bool_:
                obj[key] = bool(obj[key])
            if type(obj[key]) == numpy.float64:
                obj[key] = float(obj[key])
            if type(obj[key]) == numpy.int64:
                obj[key] = int(obj[key])
        if type(obj[key]) == 'pandas._libs.tslibs.timestamps.Timestamp' or type(obj[key]) == 'datetime.date':
            obj[key] = str(obj[key])

def reconstructRawJSON(df_ad, df_page, df_card):
    ad_json = df_ad.to_dict()
    objectTypeCheck(ad_json)
    ad_archive_id = ad_json['adArchiveID']
    scrapedDate = ad_json['ScrapedDate']
    page_id = ad_json['snapshot_page_id']
    page = df_page[(df_page['id']==page_id)]
    if page.shape[0] == 0:
        page_json = {}
    else:
        page_json = page.iloc[0,1:].to_dict()
        objectTypeCheck(page_json)
        del page_json['ScrapedDate']
    cards = df_card[df_card['adArchiveId'] == ad_archive_id]
    cards_json = []
    for i in range(cards.shape[0]): # number of rows shape:(row, column)
        row = cards.iloc[i,1:].to_dict()
        objectTypeCheck(row)
        cards_json.append(row)
    ad_json['page'] = page_json
    ad_json['snapshot_cards'] = cards_json
    return ad_json