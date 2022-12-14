import tweepy
from tweepy.auth import OAuthHandler
from tweepy import Stream

import json
import logging 


from ctypes import *
CDLL(r'C:\Users\Billie\anaconda3\Lib\site-packages\confluent_kafka.libs\librdkafka-09f4f3ec.dll')

from confluent_kafka import Producer


# logging
logging.basicConfig(format='%(asctime)s | %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S',
                    filename='producer_twitter.log',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)


# Twitter API credentials
with open('credentials.json','r') as f:
    credential = json.load(f)

CONSUMER_KEY = credential['twitter_api_key']
CONSUMER_SECRET = credential['twitter_api_secret_key']
ACCESS_TOKEN = credential['twitter_access_token']
ACCESS_TOKEN_SECRET = credential['twitter_access_token_secret']


#################### KAFKA
p=Producer({'bootstrap.servers':'localhost:9092'})

topic_name = "twitter-data"
search_term = 'Data Engineer'

def receipt(err,msg):
    if err is not None:
        print('Error: {}'.format(err))
    else:
        message = 'Produced topic {} with value of {}\n'.format(msg.topic(), msg.value().decode('utf-8'))
        logger.info(message)
        print(message)

def twitterAuth():
    # create the authentication object
    authenticate = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    # set the access token and the access token secret
    authenticate.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    # create the API object
    api = tweepy.API(authenticate, wait_on_rate_limit=True)
    return api

class TweetListener(tweepy.Stream):

    def on_data(self, raw_data):
        logging.info(raw_data)
        p.produce(topic_name, value=raw_data, callback=receipt)
        return True

    def on_error(self, status_code):
        if status_code == 420:
            # returning False if on_data disconnects the stream
            return False

    def start_streaming_tweets(self, search_term):
        self.filter(track=search_term, stall_warnings=True, languages=["en"])


# class ListenerTS(Stream):

#     def on_status(self, status):
#         tweet = json.dumps({
#             'id': status.id, 
#             'name': status.user.name, 
#             'user_location':status.user.location, 
#             'text': status.text, 
#             'fav': status.favorite_count, 
#             'tweet_date': status.created_at.strftime("%Y-%m-%d %H:%M:%S"), 
#             'tweet_location': status.place.full_name if status.place else None
#         }, default=str)  

#         p.produce(topic_name, tweet.encode('utf-8'), callback=receipt)
#         return True
        
if __name__ == '__main__':
    twitter_stream = TweetListener(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    twitter_stream.start_streaming_tweets(search_term)
