import pandas as pd
import numpy as np
from scipy import stats
from operator import itemgetter
import re
import json
import matplotlib.pyplot as plt
import seaborn as sns
import networkx as nx
import pymongo

from wordcloud import WordCloud

client = pymongo.MongoClient('localhost', 27017)
db = client.TwitterStream
tweets = db.all_tweets


# Get the information about user
def get_users(tweets_final):
    print("Getting users...")
    tweets_final["screen_name"] = tweets_df['user.screen_name']
    tweets_final["user_id"] = tweets_df["user.id"]
    tweets_final["followers_count"] = tweets_df["user.followers_count"]
    return tweets_final


# Get the user mentions
def get_usermentions(tweets_final):
    print("Getting User Mentions...")
    # Inside the tag 'entities' will find 'user mentions' and will get 'screen name' and 'id'
    if not tweets_df['entities.user_mentions'].empty:
        tweets_final["user_mentions_screen_name"] = tweets_df['entities.user_mentions'][0][0]['screen_name']
        tweets_final["user_mentions_id"] = tweets_df['entities.user_mentions'][0][0]["id_str"]
    return tweets_final


# Get retweets
def get_retweets(tweets_final):
    print("Getting Retweets...")
    # Inside the tag 'retweeted_status' will find 'user' and will get 'screen name' and 'id'
    tweets_final["retweeted_screen_name"] = tweets_df["retweeted_status.user.screen_name"]
    tweets_final["retweeted_id"] = tweets_df["retweeted_status.user.id_str"]
    return tweets_final


# Get quoted tweets
def get_quoted(tweets_final):
    print("Getting Quoted Tweets...")
    # Inside the tag 'retweeted_status' will find 'user' and will get 'screen name' and 'id'
    tweets_final["quoted_screen_name"] = tweets_df["quoted_status.user.screen_name"]
    tweets_final["quoted_id"] = tweets_df["quoted_status.user.id_str"]
    return tweets_final


# Get the interactions between the different users
def get_interactions(row):
    # From every row of the original dataframe
    # First we obtain the 'user_id' and 'screen_name'
    user = row["user_id"], row["screen_name"]
    # Be careful if there is no user id
    if user[0] is None:
        return (None, None), []

    # The interactions are going to be a set of tuples
    interactions = set()

    # Add Mentions
    interactions.add((row["user_mentions_id"], row["user_mentions_screen_name"]))
    # Add Retweets
    interactions.add((row["retweeted_id"], row["retweeted_screen_name"]))
    # Add replies
    interactions.add((row["in_reply_to_user_id"], row["in_reply_to_screen_name"]))
    # Add quotes
    interactions.add((row["quoted_id"], row["quoted_screen_name"]))

    # Discard if user id is in interactions
    interactions.discard((row["user_id"], row["screen_name"]))
    # Discard all not existing values
    interactions.discard((None, None))
    # Return user and interactions
    return user, interactions


# Get the information about replies
def get_reply(tweets_final):
    # Just copy the 'in_reply' columns to the new dataframe
    tweets_final["in_reply_to_screen_name"] = tweets_df["in_reply_to_screen_name"]
    tweets_final["in_reply_to_status_id"] = tweets_df["in_reply_to_status_id"]
    tweets_final["in_reply_to_user_id"] = tweets_df["in_reply_to_user_id"]
    return tweets_final


# Lastly fill the new dataframe with the important information
def fill_df(tweets_final):
    tweets_final = get_users(tweets_final)
    tweets_final = get_usermentions(tweets_final)
    tweets_final = get_retweets(tweets_final)
    tweets_final = get_reply(tweets_final)
    tweets_final = get_quoted(tweets_final)
    return tweets_final




if __name__ == "__main__":
    pd.set_option('display.float_format', lambda x: '%.f' % x)
    print("Getting Database And Flattening...")
    tweets_df = pd.json_normalize(tweets.find({}).limit(500), max_level=2)

    print(list(tweets_df.columns))
    # Create a second dataframe to put important information
    tweets_final = pd.DataFrame(
        columns=["created_at", "id", "in_reply_to_screen_name", "in_reply_to_status_id", "in_reply_to_user_id",
                 "retweeted_id", "retweeted_screen_name", "user_mentions_screen_name", "user_mentions_id",
                 "text", "user_id", "screen_name", "followers_count"])

    # Columns that are going to be the same
    equal_columns = ["created_at", "id", "text"]
    tweets_final[equal_columns] = tweets_df[equal_columns]

    tweets_final = fill_df(tweets_final)

    tweets_final = tweets_final.where((pd.notnull(tweets_final)), None)

    graph = nx.Graph()

    print("Getting Interactions....")

    for index, tweet in tweets_final.iterrows():
        user, interactions = get_interactions(tweet)
        user_id, user_name = user
        tweet_id = tweet["id"]
        # tweet_sent = tweet["sentiment"]
        for interaction in interactions:
            int_id, int_name = interaction

            graph.add_edge(user_id, int_id, tweet_id=tweet_id)

            graph.nodes()[user_id]["name"] = user_name
            graph.nodes()[int_id]["name"] = int_name

    print("Graph Data...")

    print(f"There are {graph.number_of_nodes()} nodes and {graph.number_of_edges()} edges present in the Graph")

    degrees = [val for (node, val) in graph.degree()]
    print(f"The maximum degree of the Graph is {np.max(degrees)}")
    print(f"The minimum degree of the Graph is {np.min(degrees)}")
    print(f"The average degree of the nodes in the Graph is {np.mean(degrees):.1f}")
    print(f"The most frequent degree of the nodes found in the Graph is {stats.mode(degrees)[0][0]}")

    if nx.is_connected(graph):
        print("The graph is connected")
    else:
        print("The graph is not connected")

    print(f"There are {nx.number_connected_components(graph)} connected components in the Graph")

    print("Drawing Graph...")
    pos = nx.spring_layout(graph, k=0.15)
    plt.figure()
    nx.draw(graph, pos=pos, edge_color="black", linewidths=0.05,
            node_size=4, alpha=0.6, with_labels=False)
    nx.draw_networkx_nodes(graph, pos=pos, node_size=5, node_color=range(graph.number_of_nodes()), cmap='Blues')
    plt.savefig('all_Interactions.png')
    plt.show()
