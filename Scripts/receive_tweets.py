import tweepy
import configparser
import pandas as pd 

def receive_tweets():
    print("Task 1 - Retrieving Tweets from Twitter API")
    # read config 
    #print("Task 1.1 - Reading Config File")
    config = configparser.ConfigParser()
    config.read('/home/paritosh/PycharmProjects/pysparkBDA/LAB_12/config.ini')
    

    api_key = config['twitter']['api_key']
    api_key_secret = config['twitter']['api_key_secret']

    access_token = config['twitter']['access_token']
    access_token_secret = config['twitter']['access_token_secret']
    
    # authentication 
    auth = tweepy.OAuthHandler(api_key,
        api_key_secret)
    auth.set_access_token(access_token,
        access_token_secret)

    api = tweepy.API(auth)

    # NFT hastags
    keywords = ['#Tech']

    for i in keywords:
        limit = 1000
        tweets = tweepy.Cursor(api.search_tweets,
        q = keywords, count = 50,tweet_mode='extended').items(limit)

    # create DataFrame
    columns = ['User', 'Tweet']
    data = []

    for tweet in tweets:
        data.append([tweet.user.screen_name, tweet.full_text])
        
   # print("Task 1.2 - Authentication Successful")
    #print("Task 1.3 - Tweets Retrieved")


    df = pd.DataFrame(data, columns=columns)

    df.to_csv("/home/paritosh/PycharmProjects/pysparkBDA/LAB_12/Data.csv")
    #print("Task 1.4 - Tweets Saved to CSV")