"""
Adapted from AWS sample code of the Lex Code Hook Interfance
"""
import math
import dateutil.parser
import datetime
import time
import os
import logging
import boto3
import json

logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

""" --- Helpers to build responses which match the structure of the necessary dialog actions --- """

def get_slots(intent_request):
    return intent_request['sessionState']['intent']['slots']
    
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': { 
                'type': 'ElicitSlot',
                'slotToElicit': slot_to_elicit
            },
            'intent': {
                'name': intent_name,
                'slots': slots
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message
        }]
    }

def close(session_attributes, fulfillment_state, message):
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': { 'type': 'Close' },
            'intent': { 'state': fulfillment_state }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message
        }]
    }

def delegate(session_attributes, intent_name, slots, message):
    return {
        'sessionState': {
            'sessionAttributes': session_attributes,
            'dialogAction': { 'type': 'Delegate' },
            'intent': {
                'name': intent_name,
                'slots': slots
            }
        },
        'messages': [{
            'contentType': 'PlainText',
            'content': message
        }]
    }
    
def isValidDate(date):
    try:
        dateutil.parser.parse(date)
        return True
    except ValueError:
        return False

""" --- Functions that control the bot's behavior --- """

def validate_slots(slots):
    is_valid = True
    violated_slot = None
    error_message = None
    
    # validating each slot
    if 'location' not in slots.keys() or slots['location'] is None:
        is_valid = False
        violated_slot = 'location'
        error_message = "What city are you looking to dine in?"
    elif 'cuisine' not in slots.keys() or slots['cuisine'] is None:
        is_valid = False
        violated_slot = 'cuisine'
        error_message = "Got it, {}. What cuisine would you like to try?".format(slots['location']['value']['interpretedValue'])
    elif 'people' not in slots.keys() or slots['people'] is None:
        is_valid = False
        violated_slot = 'people'
        error_message = "Ok, how many people are in your party?"
    elif int(slots['people']['value']['interpretedValue']) < 1 or int(slots['people']['value']['interpretedValue']) > 10:
        is_valid = False
        violated_slot = 'people'
        error_message = "Sorry, we cannot reserve a table for {} people. Please enter a valid number between 1 and 10.".format(slots['people']['value']['interpretedValue'])
    elif 'date' not in slots.keys() or slots['date'] is None:
        is_valid = False
        violated_slot = 'date'
        error_message = "A few more to go. What date?"
    elif not isValidDate(slots['date']['value']['interpretedValue']):
        is_valid = False
        violated_slot = 'date'
        error_message = "Sorry, I didn't get it. What date?"
    elif 'time' not in slots.keys() or slots['time'] is None:
        is_valid = False
        violated_slot = 'time'
        error_message = "What time?"
    elif 'email' not in slots.keys() or slots['email'] is None:
        is_valid = False
        violated_slot = 'email'
        error_message = "Great. Lastly, I need your email address so I can send you my findings."
        
    if is_valid:
        error_message = "You’re all set. Expect my suggestions shortly! Have a good day."
    
    return {
        'isValid': is_valid,
        'violatedSlot': violated_slot,
        'message': error_message
    }

def greeting_intent(intent_request):
    return {
        'sessionState': {
            'dialogAction': { 'type': 'ElicitIntent' }
        },
        'messages': [ {
            "contentType": "PlainText",
            "content": "How can I help you?"
        } ]
    }

def thank_you_intent(intent_request):
    return {
        'sessionState': {
            'dialogAction': { 'type': 'ElicitIntent' }
        },
        'messages': [ {
            "contentType": "PlainText",
            "content": "No Problem!"
        } ]
    }
    
def dining_suggestions_intent(intent_request):
    output_session_attributes = {}
    if intent_request['sessionState']['sessionAttributes'] is not None:
        output_session_attributes = intent_request['sessionState']['sessionAttributes']
    
    if intent_request['invocationSource'] == 'DialogCodeHook':
        slots = get_slots(intent_request)
        validate_res = validate_slots(slots)
        logger.debug(validate_res)
        if not validate_res['isValid']:
            slots[validate_res['violatedSlot']] = None
            return elicit_slot(output_session_attributes, intent_request['sessionState']['intent']['name'], \
                                slots, validate_res['violatedSlot'], validate_res['message'])
        return delegate(output_session_attributes, intent_request['sessionState']['intent']['name'], slots, validate_res['message'])
    
    res = sqs_msg(get_slots(intent_request))
    return close(output_session_attributes, 'Fulfilled', 'Thank you! Have a nice day.')

def sqs_msg(slots):
    sqs = boto3.resource('sqs')
    message = {
        'location': slots['location']['value']['interpretedValue'],
        'cuisine': slots['cuisine']['value']['interpretedValue'],
        'people': int(slots['people']['value']['interpretedValue']),
        'date': slots['date']['value']['interpretedValue'],
        'time': slots['time']['value']['interpretedValue'],
        'email': slots['email']['value']['interpretedValue']
    }
    queue = sqs.get_queue_by_name( QueueName='6998_hw1_SQS' )
    return queue.send_message(MessageBody=json.dumps(message))


""" --- Intents --- """

def dispatch(intent_request):
    logger.debug('dispatch sessionId={}, intentName={}'.format(intent_request['sessionId'], intent_request['sessionState']['intent']['name']))
    intent_name = intent_request['sessionState']['intent']['name']

    # Dispatch to your bot's intent handlers
    if intent_name == 'DiningSuggestionsIntent':
        return dining_suggestions_intent(intent_request)
    elif intent_name == 'GreetingIntent':
        return greeting_intent(intent_request)
    elif intent_name == 'ThankYouIntent':
        return thank_you_intent(intent_request)

    raise Exception('Intent with name ' + intent_name + ' not supported')


""" --- Main handler --- """

def lambda_handler(event, context):
    # By default, treat the user request as coming from the America/New_York time zone.
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    print(event)

    return dispatch(event)