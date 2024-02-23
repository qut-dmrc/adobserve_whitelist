import requests
import boto3
import os
from os import environ, remove, path
from time import time, sleep
from datetime import datetime
import sqlite3 as sql
import json
import csv
import concurrent.futures
from exceptions import *
from fields import alcohol_ads_fields, ad_snapshots_fields
from pusher import Pusher
from schema import AD_SCHEMA, SNAPSHOT_SCHEMA
from decouple import config

class API:
    def __init__(self, session=None):
        self.log_function = print
        self.retry_rate = 5
        self.num_retries = 5
        self.failed_last = False
        self.force_stop = False
        self.ignore_errors = False
        self.common_errors = (requests.exceptions.ConnectionError,
                              requests.exceptions.Timeout,
                              requests.exceptions.HTTPError)
        self.session = session

    def log_error(self, e):
        """
        Print errors. Stop travis-ci from leaking api keys
        :param e: The error
        :return: None
        """

        if not environ.get('CI'):
            self.log_function(e)
            if hasattr(e, 'response') and hasattr(e.response, 'text'):
                self.log_function(e.response.text)

    def _sleep(self, seconds):
        """
        Sleep between requests, but don't force asynchronous code to wait
        :param seconds: The number of seconds to sleep
        :return: None
        """
        for _ in range(int(seconds)):
            if not self.force_stop:
                sleep(1)

    @staticmethod
    def merge_params(parameters, new):
        if new:
            parameters = {**parameters, **new}
        return parameters

    def get(self, *args, **kwargs):

        """
        An interface for get requests that handles errors more gracefully to
        prevent data loss
        """

        try:
            req_func = self.session.get if self.session else requests.get
            req = req_func(*args, **kwargs)
            req.raise_for_status()
            self.failed_last = False
            return req

        except requests.exceptions.RequestException as e:
            self.log_error(e)
            for i in range(1, self.num_retries):
                sleep_time = self.retry_rate * i
                self.log_function("Retrying in %s seconds" % sleep_time)
                self._sleep(sleep_time)
                try:
                    req = requests.get(*args, **kwargs)
                    req.raise_for_status()
                    self.log_function("New request successful")
                    return req
                except requests.exceptions.RequestException:
                    self.log_function("New request failed")

            # Allows for the api to ignore one potentially bad request
            if not self.failed_last:
                self.failed_last = True
                raise ApiError(e)
            else:
                raise FatalApiError(e)
    def post(self, url, headers, params, data):
        try:
            req = requests.get(url, headers=headers, params=params, data=data)
            req.raise_for_status()
            self.failed_last = False
            return req.json()

        except requests.exceptions.RequestException as e:
            self.log_error(e)
            for i in range(1, self.num_retries):
                sleep_time = self.retry_rate * i
                self.log_function("Retrying in %s seconds" % sleep_time)
                self._sleep(sleep_time)
                try:
                    req =  req = requests.get(url, headers=headers, params=params, data=data)
                    req.raise_for_status()
                    self.log_function("New request successful")
                    return req
                except requests.exceptions.RequestException:
                    self.log_function("New request failed")

            # Allows for the api to ignore one potentially bad request
            if not self.failed_last:
                self.failed_last = True
                raise ApiError(e)
            else:
                raise FatalApiError(e)


class FB_AD_API(API):
    def __init__(self, pusher, dataset):
        super().__init__()
        self.dataset = dataset
        self.ads_filename = dataset+'.csv'
        self.snapshots_filename = 'ad_snapshot_card.csv'
        self.media_dir = 'media/'
        self.json_dir = 'raw_json/'
        self.pusher = pusher
        self.AWS_ACCESS_KEY_ID = config('AWS_ACCESS_KEY_ID')
        self.AWS_SECRET_ACCESS_KEY = config('AWS_SECRET_ACCESS_KEY')
        # self.conn = sql.connect('alcohol_ads.db')
        # self.sqlc = self.conn.cursor()
        # self.create_db_tables()
        if not os.path.isdir(self.media_dir):
            os.mkdir(self.media_dir)

    def create_db_tables(self):
        self.sqlc.execute("CREATE TABLE IF NOT EXISTS alcohol_ads("+
                        """
                        adid text,
                        adArchiveID text,
                        archiveTypes integer,
                        categories integer,
                        collationCount integer,
                        collationID integer,
                        currency text,
                        endDate text,
                        entityType text,
                        favInfo text,
                        gatedType text,
                        hiddenSafetyData text,
                        hideDataStatus text,
                        impressionsText text,
                        impressionsIndex text,
                        isActive text,
                        isProfilePage text,
                        reachEstimate integer,
                        snapshot_ad_creative_id integer,
                        snapshot_byline text,
                        snapshot_caption text,
                        snapshot_cta_text text,
                        snapshot_effective_auth_category text,
                        snapshot_event text,
                        snapshot_extra_images text,
                        snapshot_extra_links text,
                        snapshot_extra_texts text,
                        snapshot_extra_videos text,
                        snapshot_instagram_shopping_products text,
                        snapshot_display_format text,
                        snapshot_page_welcome_message text,
                        snapshot_creation_time text,
                        spend text,
                        startDate text,
                        publisherPlatform text,
                        menuItems text,
                        scrapedDate text
                    )""")

        self.sqlc.execute("CREATE TABLE IF NOT EXISTS ad_snapshot_cards("+
                    """ad_creative_id text,
                    body text,
                    caption text,
                    cta_text text,
                    cta_type text,
                    title text,
                    link_description text,
                    link_url text,
                    image_url_ori text,
                    image_url_resized text,
                    image_crops text,
                    video_hd_url text,
                    video_sd_url text,
                    video_preview_image_url text
                    )""")
        self.conn.commit()

    def get_page_info(self, page):
        # same as the function in FB_page_id
        return

    def get_ads(self, page_id):
        
        #commented out for aupoli
        
        self.ads_file = open(self.ads_filename, 'a', encoding='utf-8', errors='ignore', newline='')
        self.ad_writer = csv.writer(self.ads_file)
        self.snapshots_file = open(self.snapshots_filename, 'a', encoding='utf-8', errors='ignore', newline='')
        self.snapshot_writer = csv.writer(self.snapshots_file)
        self.ad_writer.writerow(alcohol_ads_fields)
        self.snapshot_writer.writerow(ad_snapshots_fields)
        
        
        # empty list for each new page_id
        self.ads = []
        self.snapshots = []
        self.raw_json = []
        self.current_page = page_id
        unprocessed_ads = []
        results, forward_cursor, collation_token = self.get_first_30_ads(page_id)
        if results == None:
            return None
        print('Result length',len(results))
        # print("Forward_cursor, collation_token", forward_cursor, collation_token)
        unprocessed_ads.extend(results)
        while forward_cursor:
            results, forward_cursor, collation_token = self.paginate_more_ads(page_id, forward_cursor, collation_token)
            print('Result length',len(results))
            unprocessed_ads.extend(results)
            break
        self.raw_json = [ad for ads in unprocessed_ads for ad in ads]
        if not os.path.isdir(self.json_dir):
            print("creating json folder")
            os.mkdir(self.json_dir) # create folder
        # with open(os.path.join(self.json_dir,datetime.now().strftime("%Y-%m-%d"))+".json",'w') as f:
        #     json.dump(self.raw_json, f)
        
        
        for ads in unprocessed_ads:
            # print(ad[0])
            # break
            for ad in ads:
                self.flatten(ad)
        self.writeDataToCSV()
        
        print('Total ads collected', len(unprocessed_ads))
        
        if(len(unprocessed_ads) > 0):
            
            # commented out for aupoli
            self.pusher.push_to_gbq(self.ads_filename, AD_SCHEMA)
            self.pusher.push_to_gbq(self.snapshots_filename, SNAPSHOT_SCHEMA)
            self.push_to_s3()
        else:
            remove(self.ads_filename)
            remove(self.snapshots_filename)
        
        return self.raw_json

    
    def get_first_30_ads(self, page_id):
        self.session_id = '#replace with your own'
        cookies = {#replace with your own
        }

        headers = {#replace with your own
        }

        data = {#replace with your own
        }


        params = (
            ('session_id', self.session_id),
            ('count', '30'),
            ('active_status', 'all'),
            ('ad_type', 'all'),
            ('countries/[0/]', 'AU'),
            ('view_all_page_id', page_id),
            ('media_type', 'all'),
            ('search_type', 'page'),
        )
        if self.dataset == 'poli':
            params = (
                ('session_id', self.session_id),
                ('count', '30'),
                ('active_status', 'all'),
                ('ad_type', 'political_and_issue_ads'),
                ('countries/[0/]', 'AU'),
                ('view_all_page_id', page_id),
                ('media_type', 'all'),
                ('sort_data/[direction/]','desc'),
                ('sort_data/[mode/]','relevancy_monthly_grouped'),
                ('search_type','page'),
            )

        response = requests.post('https://www.facebook.com/ads/library/async/search_ads/', headers=headers, params=params, cookies=cookies, data=data)
        
        response_data = json.loads(response.content.decode('UTF-8')[9:])['payload']
        if response_data == None or response_data == {}:
            print("No result data")
            print(response.content.decode('UTF-8'))
            return None, None, None
        results = response_data['results']
        forward_cursor = response_data['forwardCursor']
        collation_token = response_data['collationToken']

        return results, forward_cursor, collation_token
    
    def paginate_more_ads(self, page_id, forward_cursor, collation_token):
        cookies = {#replace with your own
        }

        headers = {#replace with your own
        }

        data = {#replace with your own
        }

        params = (
            ('forward_cursor', forward_cursor),
            ('session_id', self.session_id),
            ('collation_token', collation_token),
            ('count', '30'),
            ('active_status', 'all'),
            ('ad_type', 'all'),
            ('countries/[0/]', 'AU'),
            ('view_all_page_id', page_id),
            ('media_type', 'all'),
            ('search_type', 'page'),
            ('sort_data/[direction/]','desc'),
            ('sort_data/[mode/]','relevancy_monthly_grouped'),
            ('search_type','page'),
        )
        if self.dataset == 'aupoli2022':
            params = (
                ('forward_cursor', forward_cursor),
                ('session_id', self.session_id),
                ('collation_token', collation_token),
                ('count', '30'),
                ('active_status', 'all'),
                ('ad_type', 'political_and_issue_ads'),
                ('countries/[0/]', 'AU'),
                ('view_all_page_id', page_id),
                ('media_type', 'all'),
                ('search_type', 'page'),
            )

        response = requests.post('https://www.facebook.com/ads/library/async/search_ads/', headers=headers, params=params, cookies=cookies, data=data)
        response_data = json.loads(response.content.decode('UTF-8')[9:])['payload']
        results = response_data['results']
        forward_cursor = response_data['forwardCursor']
        collation_token = response_data['collationToken']

        return results, forward_cursor, collation_token

    def flatten(self, post):
        """
        Turn a nested dictionary into a flattened list

        :param post: a post corresponds to a row in the csv 
        :return: A flattened dictionary in list
        """
        ad_row = []
        ad_row.append(int(post['adid'])) if 'adid' in post and post['adid'] !=None else ad_row.append(-1)
        ad_row.append(post['adArchiveID']) if 'adArchiveID' in post else ad_row.append(-1)
        ad_row.append(post['archiveTypes'][0]) if 'archiveTypes' in post and len(post['archiveTypes'])>0 else ad_row.append(-1)
        ad_row.append(post['categories'][0]) if 'categories' in post and len(post['categories'])>0 else ad_row.append(-1)
        ad_row.append(int(post['collationCount'])) if 'collationCount' in post and post['collationCount'] != None else ad_row.append(-1)
        ad_row.append(int(post['collationID'])) if 'collationID' in post and post['collationID'] != None else ad_row.append(-1)
        ad_row.append(post['currency']) if 'currency' in post else ad_row.append("")
        ad_row.append(datetime.fromtimestamp(post['endDate']).strftime("%Y-%m-%d %H:%M:%S+10")) if 'endDate' in post and post['endDate'] is not None else ad_row.append("")
        ad_row.append(post['entityType']) if 'entityType' in post else ad_row.append("")
        ad_row.append(post['fevInfo']) if 'fevInfo' in post else ad_row.append("")
        ad_row.append(post['gatedType']) if 'gatedType' in post else ad_row.append("")
        ad_row.append(post['hiddenSafetyData']) if 'hiddenSafetyData' in post else ad_row.append("")
        ad_row.append(post['hideDataStatus']) if 'hideDataStatus' in post else ad_row.append("")
        ad_row.append(post['impressionsText']) if 'impressionsText' in post else ad_row.append("")
        ad_row.append(post['impressionsIndex']) if 'impressionsIndex' in post else ad_row.append("")
        ad_row.append(post['isActive']) if 'isActive' in post else ad_row.append("")
        ad_row.append(post['isProfilePage']) if 'isProfilePage' in post else ad_row.append("")
        ad_row.append(str(post['reachEstimate'])) if 'reachEstimate' in post else ad_row.append("")
        ad_row.append(post['snapshot']['ad_creative_id']) if 'snapshot' in post and 'ad_creative_id' in post['snapshot'] else ad_row.append(-1)
        ad_row.append(post['snapshot']['byline']) if 'snapshot' in post and 'byline' in post['snapshot'] else ad_row.append("")
        ad_row.append(post['snapshot']['caption']) if 'snapshot' in post and 'caption' in post['snapshot'] else ad_row.append("")
        ad_row.append(post['snapshot']['cta_text']) if 'snapshot' in post and 'cta_text' in post['snapshot'] else ad_row.append("")
        ad_row.append(post['snapshot']['effective_authorization_category']) if 'snapshot' in post and 'effective_authorization_category' in post['snapshot'] else ad_row.append("")
        ad_row.append(str(post['snapshot']['event'])) if 'snapshot' in post and 'event' in post['snapshot'] else ad_row.append("")
        ad_row.append(str(post['snapshot']['extra_images'])) if 'snapshot' in post and 'extra_images' in post['snapshot'] else ad_row.append("")
        ad_row.append(str(post['snapshot']['extra_links'])) if 'snapshot' in post and 'extra_links' in post['snapshot'] else ad_row.append("")
        ad_row.append(str(post['snapshot']['extra_texts'])) if 'snapshot' in post and 'extra_texts' in post['snapshot'] else ad_row.append("")
        ad_row.append(str(post['snapshot']['extra_videos'])) if 'snapshot' in post and 'extra_videos' in post['snapshot'] else ad_row.append("")
        ad_row.append(str(post['snapshot']['instagram_shopping_products'])) if 'snapshot' in post and 'instagram_shopping_products' in post['snapshot'] else ad_row.append("")
        ad_row.append(post['snapshot']['display_format']) if 'snapshot' in post and 'display_format' in post['snapshot'] else ad_row.append("")
        ad_row.append(str(post['snapshot']['page_welcome_message'])) if 'snapshot' in post and 'page_welcome_message' in post['snapshot'] else ad_row.append("")
        ad_row.append(datetime.fromtimestamp(post['snapshot']['creation_time']).strftime("%Y-%m-%d %H:%M:%S+10")) if 'snapshot' in post and 'creation_time' in post['snapshot'] else ad_row.append("")
        ad_row.append(post['snapshot']['page_id']) if 'snapshot' in post and 'page_id' in post['snapshot'] else ad_row.append(-1)
        ad_row.append(post['snapshot']['page_name']) if 'snapshot' in post and 'page_name' in post['snapshot'] else ad_row.append("")
        ad_row.append(str(post['spend'])) if 'spend' in post else ad_row.append("")
        ad_row.append(datetime.fromtimestamp(post['startDate']).strftime("%Y-%m-%d %H:%M:%S+10")) if 'startDate' in post and post['startDate'] is not None else ad_row.append("")
        ad_row.append(str(post['stateMediaRunLabel'])) if 'stateMediaRunLabel' in post else ad_row.append("")
        ad_row.append(str(post['publisherPlatform'])) if 'publisherPlatform' in post else ad_row.append("")
        ad_row.append(str(post['menuItems'])) if 'menuItems' in post else ad_row.append("")
        ad_row.append(datetime.now().strftime("%Y-%m-%d"))
        self.ads.append(ad_row)
        if 'snapshot' in post and 'cards' in post['snapshot']:
            for card in post['snapshot']['cards']:
                snapshot_row = []
                snapshot_row.append(post['snapshot']['ad_creative_id']) if 'snapshot' in post and 'ad_creative_id' in post['snapshot'] else snapshot_row.append(-1)
                snapshot_row.append(card['body']) if 'body' in card else snapshot_row.append("")
                snapshot_row.append(card['caption']) if 'caption' in card else snapshot_row.append("")
                snapshot_row.append(card['cta_text']) if 'cta_text' in card else snapshot_row.append("")
                snapshot_row.append(card['cta_type']) if 'cta_type' in card else snapshot_row.append("")
                snapshot_row.append(card['title']) if 'title' in card else snapshot_row.append("")
                snapshot_row.append(str(card['link_description'])) if 'link_description' in card else snapshot_row.append("")
                snapshot_row.append(card['link_url']) if 'link_url' in card else snapshot_row.append("")
                media_type = None
                if 'original_image_url' in card and card['original_image_url'] is not None:
                    snapshot_row.append(card['original_image_url'].split('?')[0].split('/')[-1]) 
                    if card['original_image_url']:
                        self.downloadImageFromURL(card['original_image_url'])
                        media_type = "image"
                else:
                    snapshot_row.append("")
                if 'video_hd_url' in card and card['video_hd_url'] is not None:
                    video_url = card['video_hd_url'] if card['video_hd_url'] != "" else card['video_sd_url']
                    snapshot_row.append(video_url.split('?')[0].split('/')[-1])
                    if video_url:
                        self.downloadImageFromURL(video_url)
                        media_type = "video"
                else:
                    snapshot_row.append("")
                snapshot_row.append(post['adArchiveID'])
                snapshot_row.append(datetime.now().strftime("%Y-%m-%d"))
                snapshot_row.append(media_type)
                self.snapshots.append(snapshot_row)
            if len(post['snapshot']['cards']) == 0:
                snapshot_row = []
                snapshot_row.append(post['snapshot']['ad_creative_id']) if 'snapshot' in post and 'ad_creative_id' in post['snapshot'] else snapshot_row.append(-1)
                snapshot_row.append(post['snapshot']['body']) if 'body' in post['snapshot'] else snapshot_row.append("")
                snapshot_row.append(post['snapshot']['caption']) if 'caption' in post['snapshot'] else snapshot_row.append("")
                snapshot_row.append(post['snapshot']['cta_text']) if 'cta_text' in post['snapshot'] else snapshot_row.append("")
                snapshot_row.append(post['snapshot']['cta_type']) if 'cta_type' in post['snapshot'] else snapshot_row.append("")
                snapshot_row.append(post['snapshot']['title']) if 'title' in post['snapshot'] else snapshot_row.append("")
                snapshot_row.append(str(post['snapshot']['link_description'])) if 'link_description' in post['snapshot'] else snapshot_row.append("")
                snapshot_row.append(post['snapshot']['link_url']) if 'link_url' in post['snapshot'] else snapshot_row.append("")
                media_type = None
                if 'images' in post['snapshot'] and len(post['snapshot']['images']) > 0:
                    snapshot_row.append(post['snapshot']['images'][0]['original_image_url'].split('?')[0].split('/')[-1]) if 'original_image_url' in post['snapshot']['images'][0] else snapshot_row.append("")
                    if 'original_image_url' in post['snapshot']['images'][0]:
                        self.downloadImageFromURL(post['snapshot']['images'][0]['original_image_url'])
                        media_type = "image"
                else:
                    snapshot_row.append("")
                if 'videos' in post['snapshot'] and len(post['snapshot']['videos']) > 0:
                    video_url = post['snapshot']['videos'][0]['video_hd_url'] if post['snapshot']['videos'][0]['video_hd_url'] != "" else post['snapshot']['videos'][0]['video_sd_url']
                    snapshot_row.append(video_url.split('?')[0].split('/')[-1])
                    if video_url:
                        self.downloadImageFromURL(video_url)
                        media_type = "video"
                else:
                    snapshot_row.append("")
                snapshot_row.append(post['adArchiveID'])
                snapshot_row.append(datetime.now().strftime("%Y-%m-%d"))
                snapshot_row.append(media_type)
                self.snapshots.append(snapshot_row)

    def writeDataToCSV(self):
        # with open(self.output_filename, 'a', encoding='utf-8', errors='ignore', newline='') as f:
        #     writer = csv.writer(f)
        #     writer.writerows(data)
        self.ad_writer.writerows(self.ads)
        self.ads_file.flush()
        self.ads_file.close()
        self.snapshot_writer.writerows(self.snapshots)
        self.snapshots_file.flush()
        self.snapshots_file.close()

    '''
    Images is a nunpy array of urls of which the images are stored
    '''
    def downloadImageFromURL(self, url):
        with concurrent.futures.ThreadPoolExecutor() as executor:
            executor.submit(self.load_url, self.media_dir, url)

    def load_url(self, img_folder, url):
        r = requests.get(url, allow_redirects=True)
        filename = img_folder+url.split('?')[0].split('/')[-1]
        print(filename)
        with open(filename, 'wb') as f:
            f.write(r.content)
    
    def push_to_s3(self):
        s3 = boto3.client('s3', aws_access_key_id=self.AWS_ACCESS_KEY_ID, aws_secret_access_key=self.AWS_SECRET_ACCESS_KEY)

        folders = [self.media_dir]
        for folder in folders:
            for root,dirs,files in os.walk(folder):
                for file in files:
                    # if datetime.now().strftime("%Y-%m-%d") not in file:
                    s3.upload_file(path.join(root,file),self.dataset,path.join(root,file))
                    remove(path.join(root,file))
        