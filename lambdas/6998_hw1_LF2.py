import boto3
import json
from time import sleep
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

URL = "search-yelp-restaurants-prpnqlimkntokoukjsi5m6wkvi.us-west-2.es.amazonaws.com"

def poll_msg():
    queue = boto3.resource('sqs').get_queue_by_name( QueueName='6998_hw1_SQS' )
    db = boto3.client('dynamodb')
    
    credentials = boto3.Session().get_credentials()
    es = OpenSearch(
            hosts = [{"host": URL, "port": 443}],
            http_auth = AWSV4SignerAuth(credentials, 'us-west-2'),
            use_ssl = True,
            verify_cets = True,
            connection_class = RequestsHttpConnection
        )
    
    while True:
        response = queue.receive_messages()
        if len(response) == 0:
            print("queue is empty rn")
            break
        to_remove = []
        for idx, message in enumerate(response):
            print(message.body)
            content = search_db(json.loads(message.body), db, es)
            send_msg(json.loads(message.body)['email'], content)
            to_remove.append( {'Id': str(idx), 'ReceiptHandle': message.receipt_handle} )
        if len(to_remove) > 0:
            queue.delete_messages( Entries=to_remove )


def send_msg(email, content):
    ses = boto3.client('ses')
    CHARSET = "UTF-8"
    
    res = ses.send_email(
        Source='qpalzmxdrfyved@gmail.com',
        Destination={
            'ToAddresses': [email],
        },
        Message={
            'Subject': {
                'Data': 'restaurant recommendation',
                'Charset': CHARSET
            },
            'Body': {
                'Text': {
                    'Data': content,
                    'Charset': CHARSET
                }
            }
        }
    )
    
    return res


def search_db(query, db, es):
    recommendations = []
    _query = {
        'size': 3,
        'query': {
            'query_string': {
                'default_field': 'cuisine',
                'query': query['cuisine']
            }
        }
    }
    ids = es.search(body=_query, index='restaurants')['hits']['hits'][0:3]
    
    print(ids)
    
    for id in ids:
        id = id["_source"]["id"]
        business = db.get_item(TableName='yelp-restaurants', Key={'id':{'S':id}})['Item']
        recommendations.append(business['name']['S'])
        recommendations.append(business['address']['S'].split('\n')[0])
    
    print(recommendations)
    
    content = """Hello! Here are my {} restaurant suggestions for {} people, for {} at {}: 
    1. {}, located at {}, 
    2. {}, located at {}, 
    3. {}, located at {}. 
    Enjoy your meal!""".format(query['cuisine'], query['people'], query['date'], query['time'], \
    recommendations[0], recommendations[1], recommendations[2], recommendations[3], recommendations[4], recommendations[5])
    
    return content


def lambda_handler(event, context):
    poll_msg()