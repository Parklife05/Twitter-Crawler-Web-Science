import pymongo
import csv

client = pymongo.MongoClient('localhost', 27017)
db = client.TwitterStream
tweets = db.all_tweets.find()

hashtags = []

if __name__ == "__main__":

    print("=== Getting Hashtags ===")
    for tweet in tweets:
        try:
            one_tweets_tags = ""
            for tag in tweet['entities']['hashtags']:
                if one_tweets_tags == "":
                    one_tweets_tags = tag['text']
                else:
                    one_tweets_tags = one_tweets_tags + " " + tag['text']
            if one_tweets_tags != "":
                hashtags.append(one_tweets_tags)
        except:
            pass

    print("=== Writing CSV File ===")

    with open('hashtags.csv', 'w', newline='', encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(['hashtags'])
        for entry in hashtags:
            writer.writerow([entry])

    print("=== Done ===")
