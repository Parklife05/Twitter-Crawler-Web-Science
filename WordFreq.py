import pymongo
import pandas as pd
import numpy as np
import csv

if __name__ == "__main__":

    words = dict()

    # Connect to mongo db and get tweets
    client = pymongo.MongoClient('localhost', 27017)
    db = client.TwitterStream
    tweets = db.all_tweets

    # Set up pandas Dataframe
    df = pd.DataFrame(list(tweets.find()))
    print(df.info())

    # Get word occurrences
    freq_words = df['text'].str.split(expand=True).stack().value_counts().rename_axis('word').reset_index(name='counts')

    # Remove words occurring less than 100 times
    freq_words = freq_words.drop(freq_words[freq_words['counts'] < 300].index)

    print(freq_words.info())

    # Write to csv file to look for topics
    with open('output.csv', 'w', newline='') as f:
        writer = csv.writer(f)
        for index, row in freq_words.iterrows():
            writer.writerow([row['word'], row['counts']])
