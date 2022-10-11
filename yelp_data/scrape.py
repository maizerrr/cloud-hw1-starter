import json
import requests
import os
import boto3
from datetime import datetime
from tqdm import tqdm
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

API = json.load(open('yelp_api.json'))
UPDATE_DB = False
UPDATE_SEARCH = True

# AWS OpenSearch Endpoint
URL = "search-yelp-restaurants-prpnqlimkntokoukjsi5m6wkvi.us-west-2.es.amazonaws.com"


def scrape_data():
    restaurants = { "yelp-restaurants": [] }
    for term in ["asian","japanese","italian","american","indian","chinese","mexican","greek","latin","spanish"]:
        location = "new york"
        headers = {
            'Authorization': 'Bearer ' + API['API_key']
        }
        tqdm.write("Fetching data of {} restaurants...".format(term))
        for offset in tqdm(range(0, 1000, 50)):
            url = "https://api.yelp.com/v3/businesses/search?term={}&location={}&limit=50&offset={}".format(term+ " restaurants", location, offset)
            response = requests.request("GET", url, headers=headers).json()
            for business in response['businesses']:
                try:
                    item = { 
                        "id": { "S": business['id'] }, 
                        "name": { "S": business['name'] }, 
                        "address": { "S": "\n".join(business['location']['display_address']) }, 
                        "coordinates": { "S": json.dumps(business['coordinates']) }, 
                        "review_count": { "N": str(business['review_count']) }, 
                        "rating": { "N": str(business['rating']) }, 
                        "zip_code": { "S": business['location']['zip_code'] },
                        "cuisine": { "S": term },
                        "insertedAtTimestamp": { "S": str(datetime.now()) } 
                    }
                    restaurants["yelp-restaurants"].append({ "PutRequest": {"Item": item} })
                except KeyError:
                    continue
    file_name = "yelp-restaurants.json"
    with open(file_name, "w") as f:
        f.write(json.dumps(restaurants))
    tqdm.write("All data has been stored in {}".format(file_name))
    return restaurants


def upload_data(client, restaurants):
    for i in tqdm(range(0, len(restaurants['yelp-restaurants']), 25)):
        RequestItems = { "yelp-restaurants": restaurants['yelp-restaurants'][i:i+25] }
        response = client.batch_write_item(RequestItems = RequestItems)
    tqdm.write("All data uploaded to DynamoDB")
    return

def update_search(client, restaurants):
    index = 'restaurants'
    try:
        client.indices.delete(index=index)
    except:
        pass
    client.indices.create(index)
    action = {
        "index": {
            "_index": index
        }
    }
    for i in tqdm(range(0, len(restaurants['yelp-restaurants']), 100)):
        batch = restaurants['yelp-restaurants'][i:i+25]
        payload = ""
        for datum in batch:
            id = datum["PutRequest"]["Item"]["id"]["S"]
            cuisine = datum["PutRequest"]["Item"]["cuisine"]["S"]
            datum = { "id": id, "cuisine": cuisine }
            payload = payload + json.dumps(action) + "\n" + json.dumps(datum) + "\n"
        client.bulk(body=payload, index=index)
    tqdm.write("All data uploaded to OpenSearch")
    return


if __name__ == "__main__":
    # retrieve data from yelp api
    if os.path.exists("yelp-restaurants.json"):
        restaurants = json.load(open("yelp-restaurants.json"))
    else:
        restaurants = scrape_data()
    
    # upload data to dynamodb with batch writes
    if UPDATE_DB:
        client = boto3.client('dynamodb')
        upload_data(client, restaurants)

    # upload data to aws open-search
    if UPDATE_SEARCH:
        region = 'us-west-2'
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, region)
        client = OpenSearch(
            hosts = [{"host": URL, "port": 443}],
            http_auth = auth,
            use_ssl = True,
            verify_cets = True,
            connection_class = RequestsHttpConnection
        )
        update_search(client, restaurants)