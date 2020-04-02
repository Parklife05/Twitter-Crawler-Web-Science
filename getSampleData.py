import csv
import pymongo

client = pymongo.MongoClient('localhost', 27017)
db = client.TwitterStream
all_tweets = db.all_tweets

sample = []
i = 0
while i <= 400:

    for tweet in all_tweets.find():
        sample.append(tweet)
        i += 1

with open('sample.csv', 'w', newline='', encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(['sample'])
    for entry in sample:
        writer.writerow([entry])
print(sample)