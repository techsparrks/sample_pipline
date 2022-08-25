# -*- coding: utf-8 -*-
"""
Created on Wed Aug 24 13:17:54 2022

@author: mojtaba
"""


import requests
import json
import pandas as pd
import configparser
from gcloud import storage
# from oauth2client.service_account import ServiceAccountCredentials
import os


### load pipedrive credentials
config = configparser.ConfigParser()
root = os.getcwd()
config.read(str(root) + '/config.ini')
cfg = config['pipedrive_credentials']


### load gcloud storage credentials
os.environ['GCLOUD APP CREDENTIALS'] = "data-analytics-359712-1f4a4a01f7ba.json"




### deals
base_url = cfg['base_url']
url = base_url + '/deals'
api_token = cfg['api_token']

params = {
    "api_token":api_token,
    "limit" : 500
    }


headers = {
    'cache-control': "no-cache",
    'postman-token': "63a2a81f-2dd4-202a-64df-173d2891af52"
    }

response = requests.request("GET", url, headers=headers, params=params)


deals_obj = json.loads(response.text)





### write into table
deals_df = pd.DataFrame(columns=['deal_id','stage_id', 'deal_name', 'value', 'currency', 'status','pipeline_id', 
                                 'add_time', 'won_time', 'owner_name', 
                                 'creator_user_id', 'creator_name', 
                                 'user_id', 'user_name', 'user_email', 
                                 'org_name','org_people_count', 'org_address', 'org_cc_email',
                                 ])



for obj in deals_obj['data']:



   ## deal info 
   deal_id = obj['id']
   stage_id = obj['stage_id']
   deal_name = obj['title']
   value = obj['value']
   currency = obj['currency']
   status = obj['status']
   pipeline_id = obj['pipeline_id']
   add_time = obj['add_time']
   won_time = obj['won_time']
   owner_name = obj['owner_name']
   
   ## creator info
   creator_user_id = obj['creator_user_id']['id']
   creator_name = obj['creator_user_id']['name']
   
 
   ## user info
   if obj['user_id']:
       user_id = obj['user_id']['id']
       user_name = obj['user_id']['name']
       user_email = obj['user_id']['email']
   
   ## org info
   if obj['org_id']:
       org_name = obj['org_id']['name']
       org_people_count = obj['org_id']['people_count']
       org_address = obj['org_id']['address']
       org_cc_email = obj['org_id']['cc_email']   
   
   row = [deal_id, stage_id, deal_name, value, currency, status, pipeline_id, add_time, won_time, owner_name, 
          creator_user_id, creator_name, user_id, user_name, user_email,
          org_name, org_people_count, org_address, org_cc_email]
   
   deals_df.loc[len(deals_df)] = row


try:
    deals_df.to_csv('./deals_data.csv', index=1, encoding='utf-8')
    print("deals data generated!")
except Exception as e:    
    print(e)
    
    
    
    
    
    
    
    
### google cloud storage connection
storage_client = storage.Client()    
    


#create a bucket
# bucket_name = "test_bucket_sparrks"
# bucket = storage_client.bucket(bucket_name)
# print(bucket)
# bucket.location ='US'
# bucket =  storage_client.create_bucket(bucket_name)
    


## access the bucket and upload to the bucket
def upload_to_bucket(blob_name, file_path, bucket_name):
    try:
        bucket = storage_client.get_bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.upload_from_filename(file_path)
        return True
    except Exception as e:
        print(e)
        return False
    
    
    
    
file_path = os.path.join(root, "deals_data.csv")

upload_to_bucket('test_file.csv',file_path, 'sparrks-infra')




### access the bucket and download from the bucket
# def download_file_from_bucket(blob_name, file_path, bucket_name):
#     try:
#         bucket = storage_client.get_bucket(bucket_name)
#         blob = bucket.blob(blob_name)
        
#         with open(file_path, 'wb') as f:
#             storage_client.download_blob_to_file(blob, f)
            
#         return True
#     except Exception as e:
#         print(e)
#         return False
    
    
# download_file_from_bucket('test_file.csv', os.path.join(root, 'test-file.csv'), 'sparrks-infra')




# write into bigquery
from google.cloud import bigquery
client = bigquery.Client()
project = client.project


table_ref = client.dataset('crm_data').table('pipedrive_deals_data')


table = client.get_table(table_ref)

rows_to_insert = [(1, 66, u'Cara Care Rollout Deal', 11000, u'EUR', u'open', 1, '2021-05-17', 
                  '2021-05-17', 'Nicolas Stephan', 12186948, 'Yoanna', 12162441, 'Nicolas Stephan', 'nicolas.stephan@sparrks.de',
                   'HiDoc Technologies GmbH (Cara Care)', 2, "Torstraße 59, 10119 Berlin, Deutschland", 'sparrks@pipedrivemail.com')]


try:
    client.insert_rows(table, rows_to_insert)
    print('True')
except Exception as e:
    print(e)






