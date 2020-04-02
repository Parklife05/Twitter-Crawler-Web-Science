import csv
import re
from collections import Counter
import pymongo
from sklearn.cluster import KMeans
from sklearn.feature_extraction import DictVectorizer

client = pymongo.MongoClient('localhost', 27017)
db = client.TwitterStream
all_tweets = db.all_tweets

documents = []
username_data = []
hashtag_data = []

if __name__ == "__main__":
    for tweet in all_tweets.find():
        try:
            if tweet['truncated'] == True:
                documents.append({tweet['user']['screen_name']: tweet['extended_tweet']['full_text']})
            else:
                documents.append({tweet['user']['screen_name']: tweet['text']})
        except:
            pass

    vectorizer = DictVectorizer()
    X = vectorizer.fit_transform(documents)

    true_k = 10
    print("=== Preforming KMeans with %d clusters ===" % true_k)
    model = KMeans(n_clusters=true_k, init='random', max_iter=300, n_init=10)
    model.fit(X)


    order_centroids = model.cluster_centers_.argsort()[:, ::-1]
    terms = vectorizer.get_feature_names()


    for i in range(true_k):
        hashtag_data.append([])
        username_data.append([])
        print("\t=== Cluster %d, Size: %d ===" % (i, len(order_centroids[i])))
        for ind in order_centroids[i, :10]:
            tweet_string = terms[ind]
            tweet_string = "@" + tweet_string
            usernames = re.findall(r'(@\w+)', tweet_string)
            hashtags = re.findall(r'(#\w+)', terms[ind])
            for username in usernames:
                username_data[i].append(username)
            for hashtag in hashtags:
                hashtag_data[i].append(hashtag)

        print("=== Usernames ===")
        print(Counter(username_data[i]).most_common(5))
        if not hashtag_data[i] == []:
            print("=== Hashtags ===")
            print(Counter(hashtag_data[i]).most_common(5))
        else:
            print("no hashtags")
print(hashtag_data )
print(username_data)
print(documents)
