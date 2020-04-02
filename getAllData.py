import datetime
import json
import sys
import time
from tweepy import Stream
from pymongo import MongoClient
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener

# The MongoDB connection info. This assumes your database name is TwitterStream, and your collection name is tweets.
client = MongoClient('localhost', 27017)
db = client.TwitterStream2
db.tweets.create_index("id", unique=True, dropDups=True)
collection = db.tweets

# Add the keywords you want to track.
keywords = ['COVID-19', 'coronavirus', 'the', 'i', 'to', 'a', 'and', 'is', 'in', 'it', 'you', 'of', 'for', 'on', 'my'
    , 'that', 'at', 'with', 'me', 'do', 'have', 'just', 'this', 'be', 'so', 'are', 'not', 'was', 'but', 'out',
            'up', 'what', 'now', 'new', 'from', 'your', 'like', 'good', 'no', 'get', 'all', 'about', 'we', 'if', 'time',
            'as', 'day', 'tinyUrl.com', 'today', 'there', 'love', 'more', 'work', 'too', 'got', 'he', 'back', 'think',
            'did', 'better', 'hope', 'world', 'life']

language = ['en']

api_key = "wIf0jfle9QVnseAchp4mzi4QG"
api_secret = "FKT8wpWrfQHwhoIqFyK9Ch6PqCDu9GEOHiFEbpoiLwkZUU0W8X"
access_token = "1240779591603585024-YzxhWoxOiwcG7itfA4C7E1qiIug2FA"
access_token_secret = "7YWJqQYj5wzzaYEuf4z9qHvg3civohLrjMwYgaXJhILwF"


# get Tweets from the stream and store only the stated fields to your database
class StdOutListener(StreamListener):

    def __init__(self, time_limit=30600):
        self.duplicates = 0
        self.limit = time_limit
        self.start_time = time.time()

    def on_data(self, data):
        if (time.time() - self.start_time) < self.limit:
            # Load the Tweet into the variable "t"
            t = json.loads(data)
            try:
                # Pull important data from the tweet to store in the database.
                tweet_id = t['id_str']  # The Tweet ID from Twitter in string format
                username = t['user']['screen_name']  # The username of the Tweet author
                followers = t['user']['followers_count']  # The number of followers the Tweet author has
                text = t['text']  # The entire body of the Tweet
                hashtags = t['entities']['hashtags']  # Any hashtags used in the Tweet
                dt = t['created_at']  # The timestamp of when the Tweet was created
                language = t['lang']  # The language of the Tweet

                # Convert the timestamp string given by Twitter
                created = datetime.datetime.strptime(dt, '%a %b %d %H:%M:%S +0000 %Y')

                # Load all of the extracted Tweet data into the variable "tweet" that will be stored into the database
                tweet = {'id': tweet_id, 'username': username, 'followers': followers, 'text': text,
                         'hashtags': hashtags,
                         'language': language, 'created': created}

                # Save the refined Tweet data to MongoDB
                collection.insert_one(tweet)

                print(username + ':' + ' ' + text)
                return True
            except:
                print(t)
                self.duplicates += 1
        else:
            return False;

    # Prints the reason for an error to your console
    def on_error(self, status):
        print(status)


# Some Tweepy code that can be left alone. It pulls from variables at the top of the script
if __name__ == '__main__':
    l = StdOutListener()
    try:
        auth = OAuthHandler(api_key, api_secret)
        auth.set_access_token(access_token, access_token_secret)
        print("AUTH SUCCESS")
    except:
        print("AUTH FAILED")
        sys.exit()

    stream = Stream(auth, l)
    stream.filter(track=keywords, languages=["en"])
    print(l.duplicates)
