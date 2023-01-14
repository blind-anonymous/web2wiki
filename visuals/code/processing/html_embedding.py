import enum
import pandas as pd 
import os

import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
import pyarrow.parquet as pq
import pyarrow as pa


import sys
sys.setrecursionlimit(10000)

import re
import time
from bs4 import BeautifulSoup
from typing import Counter

import sys
sys.path.append("/scratch/venia/web2wiki/")


from settings import WIKI_PAGES_DIR

os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7"
os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2"
spark = SparkSession.builder.getOrCreate()

import glob 
files = glob.glob(WIKI_PAGES_DIR + "/*")
# regex_search = "([\:\/a-zA-Z\.]+wikipedia\.org[\/a-zA-ZäöüÄÖÜßþóúí\_\(\),\,\-\#\&\$\@\!0-9\.\%\–\'\:]+)"
regex_search = r"(en.wikipedia\.org[\s\/a-zA-ZäöüÄÖÜßþóúí\_\?(\),\,\-\#\&\$\@\!0-9\.\%\–\'\:\!]+)"
bad_links = [
    "/dlabdata1/piccardi/WikipediaPagesExtractor/WikiPages/CC-MAIN-20210307044328-20210307074328-00556.warc.gz.parquet"
]
files = [k for k in files if k not in bad_links]




@F.udf
def extract_embeddings(x: str): 
    soup = BeautifulSoup(x,features="lxml")
    all_links = soup.find_all('a', {"href": re.compile(regex_search)})
    embeddings = []
    for link in all_links:
        embedding = extract_embedding(link)
        embeddings.append(embedding)
    return embeddings

@F.udf
def extract_wiki(x: str): 
    soup = BeautifulSoup(x,features="lxml")
    all_links = soup.find_all('a', {"href": re.compile(regex_search)})
    links = []
    for link in all_links:
        links.append(link.get("href"))
    return links

def extract_embedding(link):
    try:
        counter = Counter()
        while link.parent:
            link = link.parent
            counter[link.name] += 1 / len(link.find_all())
        return str(counter)
    except:
        return None


def run(data):
    data = data.withColumn("wiki_links", extract_wiki(F.col("content")))
    data = data.withColumn("embedding", extract_embeddings(F.col("content")))
    data = data.select("url","wiki_links","embedding")
    return data

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

if __name__ == "__main__":
    print("==================")
    files_partitioned = chunks(files, 400)
    for i, file_paths in enumerate(files_partitioned):
        if i >= 1:
            print("==================")
            print(f"We are on partition {i}")
            t = time.time()
            data = spark.read.load(file_paths)
            selected = run(data)
            selected.write.parquet(f"/scratch/venia/web2wiki/data/wikilinks_extracted_soup/html_embedding{i}.parquet", mode = "overwrite")
            print("It took {:.2f} seconds to write one chunk.".format(time.time() - t))
