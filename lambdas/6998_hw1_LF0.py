import json
import boto3

# Define the client to interact with Lex
client = boto3.client('lexv2-runtime')

def lambda_handler(event, context):
    sessionId = event['sessionId']
    last_user_msg = event['messages'][0]['unstructured']['text']
    bot_msg = "I'm still under development. Please come back later."
    
    print(last_user_msg)
    
    # initiate conversation with Lex
    response = client.recognize_text(botId='JDTVPS2M9Y',
                                     botAliasId='TSTALIASID',
                                     localeId='en_US',
                                     sessionId=sessionId,
                                     text=last_user_msg)
    
    # parse response from Lex
    _response = response.get("messages", [])
    if _response:
        bot_msg = _response[0]['content']
        print(response)
    
    return {
        'statusCode': 200,
        'body': {
            "messages": [
                {
                    "type": "unstructured",
                    "unstructured": {
                        "text": bot_msg
                    }
                }
            ]
        }
    }
