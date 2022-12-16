import pandas as pd


def read_tweets():
    print("Task 2 - Reading Tweets from CSV")
    df = pd.read_csv("/home/paritosh/PycharmProjects/pysparkBDA/LAB_12/Data.csv")
    #print("Task 2.1 - Tweets Read from CSV")
    return df

