import pandas as pd 
from Scripts.read_tweets import read_tweets
from Scripts.receive_tweets import receive_tweets
from pyspark.sql import SparkSession
import warnings
warnings.filterwarnings("ignore")

if __name__ == "__main__":
    # Create a Stream Listener instance.
    receive_tweets()
    # Read the tweets from the CSV file.
    df = read_tweets()
    # Print the first 5 rows of the DataFrame.

    # Perform and start streaming session using SPARK Data frame.
    # Create a SparkSession instance.
    print("Creating Spark Session by Paritosh Tripathi")
    spark = SparkSession \
        .builder \
        .appName("Twiiter Streaming") \
        .master("local[*]") \
        .getOrCreate()

    # Create a DataFrame from the read_tweets() function.
    df = spark.createDataFrame(df)
    # Print the schema of the DataFrame.
    df.printSchema()

    # Tweets preprocessing and finding trending #tags.
    # Create a temporary view of the DataFrame.
    print("Task 3 - Tweets Preprocessing and Finding Trending #tags")
    df.createOrReplaceTempView("tweets")
    # Select the tweets from the DataFrame.
    tweets = spark.sql("SELECT Tweet FROM tweets")
    # Print the first 5 rows of the DataFrame.
    tweets.show(5)

    # Preprocess the tweets.
    # Import the necessary functions.
    print("Preprocess the tweets")
    from pyspark.sql.functions import regexp_replace, col, lower
    # Remove the special characters from the tweets.
    #tweets = tweets.withColumn("Tweet", regexp_replace(col("Tweet"), "[^a-zA-Z\\s]", ""))
    # Convert the tweets to lowercase.
    tweets = tweets.withColumn("Tweet", lower(col("Tweet")))
    # Print the first 5 rows of the DataFrame.
    tweets.show(5)

    # Find the trending #tags.
    # Import the necessary functions.
    print("Find the trending #tags")
    from pyspark.sql.functions import split, explode
    # Split the tweets into words.
    words = tweets.select(explode(split("Tweet", " ")).alias("word"))
    # Select the words that start with #.
    hashtags = words.filter(words.word.startswith("#"))
    # Print the all the words
    hashtags.show()


