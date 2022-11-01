#!/usr/bin/env python3

# make sure to install these packages before running:
# pip install pandas
# pip install sodapy

from sodapy import Socrata
import requests
from requests.auth import HTTPBasicAuth
import datetime
import json
import os
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--page_size', type = int)
parser.add_argument('--num_pages', type = int, default = -1)
parser.add_argument('--offset', type = int, default = 0)

args = parser.parse_args()

datefmt = '%m/%d/%Y'

DATASET_ID = os.environ['DATASET_ID']
APP_TOKEN = os.environ['APP_KEY']
ES_INDEX = os.environ['DATA_ID']
ES_HOST = os.environ['ES_HOST']
ES_USERNAME = os.environ['ES_USERNAME']
ES_PASSWORD = os.environ['ES_PASSWORD']

# Unauthenticated client only works with public data sets. Note 'None'
# in place of application token, and no username or password:
client = Socrata("data.cityofnewyork.us", APP_TOKEN, timeout = 30)

# res = client.get(DATASET_ID, select = 'COUNT(*)')
TOTAL_RECORD = 56926246
# TOTAL_RECORD = int(res[0]['COUNT'])
print(TOTAL_RECORD)

resp = requests.get(ES_HOST, auth=HTTPBasicAuth(ES_USERNAME, ES_PASSWORD))
print(resp.json())

# create elasticsearch index
try:
    resp = requests.put(
        # this is the URL to create payroll "index"
        #which is our elasticsearch database/table
        f"{ES_HOST}/{ES_INDEX}",
        auth = HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
        #these are the "columns" of this database/table
        json={
            "settings": {
                "number_of_shards": 1,
                "number_of_replicas": 1
            },
            "mappings": {
                "properties": {
                    "amount_due": {"type": "float"},
                    "fine_amount": {"type": "float"},
                    "interest_amount": {"type": "float"},
                    "issue_date": {"type": "date"},
                    "payment_amount": {"type": "float"},
                    "penalty_amount": {"type": "float"},
                    "reduction_amount": {"type": "float"},
                    "summons_number": {"type": "long"},                    
                }
            },
        })
    resp.raise_for_status()
except Exception as e:
    print(resp.json())
    print(e)
    print("Index already exists! Skipping")
    

#grab single row
    
PAGE_SIZE = args.page_size
NUM_PAGES = args.num_pages
PAGE_OFFSET = args.offset
total = 0

while NUM_PAGES != 0 and PAGE_OFFSET < TOTAL_RECORD:

    rows = client.get(DATASET_ID, limit=PAGE_SIZE, offset = PAGE_OFFSET)
    if len(rows) == 0:
        break

    NUM_PAGES -= 1
    PAGE_OFFSET += PAGE_SIZE

    tosend = []

    for row in rows:
        #convert
        try:
            row["amount_due"] = float(row["amount_due"])
            row["fine_amount"] = float(row["fine_amount"])
            row["interest_amount"] = float(row["interest_amount"])
            row["payment_amount"] = float(row["payment_amount"])
            row["penalty_amount"] = float(row["penalty_amount"])
            row["reduction_amount"] = float(row["reduction_amount"])
            row["summons_number"] = str(row["summons_number"])
            row["id"] = row['issue_date'] + row['violation_time'] + row['state'] + row['plate']
            row['issue_date'] = datetime.datetime.strptime(row['issue_date'], datefmt).strftime('%Y-%m-%d')

            tosend.append(row)
        except Exception as e:
            # print(e)
            pass

    try:
        # upload to elasticsearch by creating a doc
        resp = requests.post(
            # this is the URL to create a new payroll document
            # which is our "row" in elasticsearch database/table
            f"{ES_HOST}/{ES_INDEX}/_bulk",
            auth = HTTPBasicAuth(ES_USERNAME, ES_PASSWORD),
            headers = {"Content-Type" : "application/json"},
            data = "\n".join(
                    ["\n".join([json.dumps({"index" : {"_id" : str(x["id"])}}), json.dumps(x)]) for x in tosend] + [""]))
        # if it fails, stop the whole thing!
        #QUESTION: should we adopt this during our "main"
        #download as well...?f
        resp.raise_for_status()

        total += len(tosend)
        print("Sent %d/%d records" % (len(tosend), total))
        # print(resp.json())
    except Exception as e:
        print(e)
        pass

