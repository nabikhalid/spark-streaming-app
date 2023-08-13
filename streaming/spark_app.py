from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.dstream import DStream
import re
import json
import pandas as pd

# Initialize Spark
conf = SparkConf().setMaster("local[2]").setAppName("GitHubRepoStream")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 60)

# Set the checkpoint directory for stateful transformations
ssc.checkpoint("checkpoint")

# Connect to the data source
stream = ssc.socketTextStream("data-source", 9999)

# Function to print results in a pandas DataFrame
def print_results(time, rdd, header):
    if not rdd.isEmpty():
        print("\n-------------------------------------------")
        print(f"Time: {time}")
        print("-------------------------------------------")
        print(header)
        df = pd.DataFrame(rdd.collect(), columns=["Language", "Value"])
        print(df.to_string(index=False))

# Functions to update state for stateful transformations
def update_language_counts(new_values, running_count):
    return sum(new_values) + (running_count or 0)

def update_star_counts(new_values, running_count):
    if running_count is None:
        running_count = (0, 0)
    new_stars, new_count = sum(v[0] for v in new_values), sum(v[1] for v in new_values)
    return running_count[0] + new_stars, running_count[1] + new_count

def update_word_counts(new_values, running_count):
    return sum(new_values) + (running_count or 0)

# Task 1: Compute the total number of collected repositories for each language since the start of the streaming application
language_counts = stream.map(json.loads).map(lambda x: (x["language"], 1)).updateStateByKey(update_language_counts)
# print("Total number of repositories by language (since the start):")
# language_counts.pprint()
language_counts.foreachRDD(lambda time, rdd: print_results(time, rdd, "Total number of repositories by language (since the start):"))

# Task 2: Compute the number of collected repositories with changes pushed during the last 60 seconds
def process(rdd):
    if not rdd.isEmpty():
        repos = rdd.map(json.loads)
        recent_pushed_repos = repos.filter(lambda x: "pushed_at" in x and x["pushed_at"] is not None)
        recent_pushed_count = recent_pushed_repos.count()
        print("Number of repositories with changes pushed during the last 60 seconds: ", recent_pushed_count)

stream.foreachRDD(process)

# Task 3: Compute the average number of stars of all collected repositories for each language since the start of the streaming application
stars_sum_count = stream.map(json.loads).map(lambda x: (x["language"], (x["stargazers_count"], 1))).updateStateByKey(update_star_counts)
stars_avg = stars_sum_count.map(lambda x: (x[0], x[1][0] / x[1][1]))
# print("Average stars by language (since the start):")
# stars_avg.pprint()
stars_avg.foreachRDD(lambda time, rdd: print_results(time, rdd, "Average stars by language (since the start):"))

# Task 4: Find the top 10 most frequent words in the description of all collected repositories for each language since the start of the streaming application
def tokenize_description(repo):
    description = repo["description"]
    if description is not None:
        words = re.sub('[^a-zA-Z ]', '', description).lower().split()
        return [(repo["language"], word) for word in words]
    return []

def print_top_words(time, rdd):
    if not rdd.isEmpty():
        print("\n-------------------------------------------")
        print(f"Time: {time}")
        print("-------------------------------------------")
        print("Top 10 most frequent words by language (since the start):")
        data = []
        for lang, words in rdd.collect():
            for word, count in words:
                data.append([lang, word[1], count])
        df = pd.DataFrame(data, columns=["Language", "Word", "Count"])
        print(df.to_string(index=False))

word_counts = stream.map(json.loads).flatMap(tokenize_description).map(lambda x: (x, 1)).updateStateByKey(update_word_counts)
top_10_words = word_counts.transform(lambda rdd: rdd.groupBy(lambda x: x[0][0]).mapValues(lambda values: sorted(values, key=lambda x: x[1], reverse=True)[:10]))
# print("Top 10 most frequent words by language (since the start):")
# top_10_words.pprint()
top_10_words.foreachRDD(print_top_words)

# Start the Spark Streaming application
ssc.start()
ssc.awaitTermination()
