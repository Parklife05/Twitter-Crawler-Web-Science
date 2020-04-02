import json
import sys
import time
from tweepy import Stream
from pymongo import MongoClient
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

# The MongoDB
client = MongoClient('localhost', 27017)
db = client.TwitterStream
db.tweets.create_index("id", unique=True, dropDups=True)
collection_tweets = db.all_tweets

keywords = ['Covid-19', 'Coronavirus', 'self-isolation', 'stay at home', '#flatenthecurve',
            'social distancing', 'pandemic', 'virus', '@RealDonalTrump', '#StayHomeSaveLives', '@GOVUK',
            '@10DowningStreet', 'Corona Virus', '@BorisJohnson ', 'coronavirus deaths', 'world health organisation',
            '#COVID_19', '#coronavirus', '@WHO', 'quarantine', '#Quarantine' 'donald', 'trump', 'boris', 'johnson',
            'corona', 'virus', 'test positive covid-19']
users = ['25073877', '3131144855', '17481977', '14499829', '14224719']

api_key = "wIf0jfle9QVnseAchp4mzi4QG"
api_secret = "FKT8wpWrfQHwhoIqFyK9Ch6PqCDu9GEOHiFEbpoiLwkZUU0W8X"
access_token = "1240779591603585024-YzxhWoxOiwcG7itfA4C7E1qiIug2FA"
access_token_secret = "7YWJqQYj5wzzaYEuf4z9qHvg3civohLrjMwYgaXJhILwF"

# Tweet Listener
class StdOutListener(StreamListener):

    def __init__(self, time_limit = 120):
        # time_limit is time in minutes
        self.duplicates = 0
        self.limit = (time_limit * 60)
        self.start_time = time.time()

    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            # Load the Tweet into the variable "tweet"
            tweet = json.loads(data)
            try:
                collection_tweets.insert_one(tweet)
                return True
            except:
                # This is a duplicate
                self.duplicates += 1
                print("Duplicates so far: %d" % self.duplicates)
        else:
            # Times Up
            return False

    # Prints the reason for an error to your console
    def on_error(self, status):
        print(status)


# Some Tweepy code that can be left alone. It pulls from variables at the top of the script
if __name__ == '__main__':
    listener = StdOutListener()
    try:
        auth = OAuthHandler(api_key, api_secret)
        auth.set_access_token(access_token, access_token_secret)
        print("AUTH SUCCESS")
    except:
        print("AUTH FAILED")
        sys.exit()

    stream = Stream(auth, listener)
    print("Starting Streaming for %s seconds" % listener.limit)
    stream.filter(follow=users, track=keywords, languages=["en"],is_async=True)
    print(listener.duplicates)
