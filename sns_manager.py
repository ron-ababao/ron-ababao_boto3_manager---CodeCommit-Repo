import json
import logging
import random
import boto3
from datetime import datetime
from datetime import timezone
from time import sleep
from decimal import *
import operator as op

logging.basicConfig(
level=logging.INFO,format='[%(asctime)s] %(levelname)s %(module)s %(lineno)d - %(message)s',)
log = logging.getLogger()

def create_sns_topic(topic_name):
    sns = boto3.client('sns')
    sns.create_topic(Name=topic_name)
    return True

def list_sns_topics(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    topics = sns.list_topics(**params)
    return topics.get('Topics', []), topics.get('NextToken', None)

def list_sns_subscriptions(next_token=None):
    sns = boto3.client('sns')
    params = {'NextToken': next_token} if next_token else {}
    subscriptions = sns.list_subscriptions(**params)
    return subscriptions.get('Subscriptions', []),subscriptions.get('NextToken', None)

def subscribe_sns_topic(topic_arn, mobile_number):
    sns = boto3.client('sns')
    params = {
    'TopicArn': topic_arn,
    'Protocol': 'sms',
    'Endpoint': mobile_number,
    }
    res = sns.subscribe(**params)
    print(res)
    return True

def send_sns_message(topic_arn, message):
    sns = boto3.client('sns')
    params = {'TopicArn': topic_arn,'Message': message}
    res = sns.publish(**params)
    print(res)
    return True

def unsubscribe_sns_topic(subscription_arn):
    sns = boto3.client('sns')
    params = {'SubscriptionArn': subscription_arn,}
    res = sns.unsubscribe(**params)
    print(res)
    return True

def delete_sns_topic(topic_arn):
    # This will delete the topic and all it's subscriptions.
    sns = boto3.client('sns')
    sns.delete_topic(TopicArn=topic_arn)
    return True

if __name__ == '__main__':
    
    creation=create_sns_topic('price-update-ron')
    log.info(creation)
    
    list_sns=list_sns_topics()
    log.info(list_sns)
    
    subs_topic=subscribe_sns_topic('arn:aws:sns:us-east-1:337008671328:price-update-ron','+639278882909')
    log.info(subs_topic)
    
    list_subs=list_sns_subscriptions()
    log.info(list_subs)
    
    send_message=send_sns_message('arn:aws:sns:us-east-1:337008671328:price-update-ron', 'Woo Hoodies are no 50% off!')
    log.info(send_message)
    
    unsub=unsubscribe_sns_topic('arn:aws:sns:us-east-1:337008671328:price-update-ron:2d43d82a-3247-457b-9d34-79d4cbd07a3b')
    log.info(unsub)

    del_tops=delete_sns_topic('arn:aws:sns:us-east-1:337008671328:price-update-ron')
    log.info(del_tops)