import json
import boto3
from requests_aws4auth import AWS4Auth
import logging
import sys
import os

sys.path.append('/opt/python/requests')
import requests

# Get logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# s3 configurations
s3 = boto3.resource('s3')
bucketName = "BUCKET_NAME"
s3Folder = "elasticsearch-sync/data_"

my_session = boto3.session.Session()
region = my_session.region_name 
service = "es"
credentials = my_session.get_credentials()
awsauth = AWS4Auth(credentials.access_key, credentials.secret_key, region, service, session_token=credentials.token)

headers = { "Content-Type": "application/json" }

# Get url from environment variables
host = "https://YOUR_ENDPOINT.ap-south-1.es.amazonaws.com"
searchUri = "/DEMO-TABLE/_search"
url = host + searchUri

def lambda_handler(event, context):
    total_count = 0
    next_partitionKey = None
    next_sortKey = None
    fileCount = 1
    column_names = "id,date\n"
    content = column_names
    while True:
        out_file = s3Folder + f"{fileCount}.csv"
        print("current file - ",out_file)
        print("search after - ",next_partitionKey,next_sortKey)
        payload = {
            "_source" : ["id","date"], #include fields that is required
            "size": 10000,
            "query": {
                "match_all": {}
            },
            "sort": [
                {
                    "id": "asc"
                },
                {
                    "date": "asc"
                }
            ],
            "search_after": [next_partitionKey,next_sortKey] #add keys whichever makes it unique. If it is dynamoDB table, then use the partition and sort key
        }

        response = requests.post(searchUri, auth=awsauth, data=json.dumps(payload), headers=headers)
        print(response.text)
        if json.loads(response.text)["data"] != None:
            data = eval(json.loads(response.text)["data"])
            next_partitionKey = data[len(data)-1]["id"]
            next_sortKey = data[len(data)-1]["date"]
            total_count += len(data)
            for item in data:
                content += f"{item['id']},{item['date']}\n"
            s3.Object(bucketName, out_file).put(Body = content)
            del content
            del data
            del response
            content = column_names
            fileCount += 1
        else:
            print("total_count ", total_count)
            break

lambda_handler("","") #remove this line if you are using in AWS lambda