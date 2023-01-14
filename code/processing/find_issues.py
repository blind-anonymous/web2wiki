import glob
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
import pyarrow.parquet as pq
import pyarrow as pa

import os

import sys
sys.setrecursionlimit(10000)

import re
import time
from bs4 import BeautifulSoup

from settings import WIKI_PAGES_DIR

os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7"
os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2"
spark = SparkSession.builder.getOrCreate()


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

files = glob.glob(WIKI_PAGES_DIR + "/*")
# regex_search = "([\:\/a-zA-Z\.]+wikipedia\.org[\/a-zA-ZäöüÄÖÜßþóúí\_\(\),\,\-\#\&\$\@\!0-9\.\%\–\'\:]+)"
regex_search = r"([\:\/a-zA-Z\.]+wikipedia\.org[\s\/a-zA-ZäöüÄÖÜßþóúí\_\?(\),\,\-\#\&\$\@\!0-9\.\%\–\'\:\!]+)"
files = [k for k in files if k != "/dlabdata1/piccardi/WikipediaPagesExtractor/WikiPages/CC-MAIN-20210307044328-20210307074328-00556.warc.gz.parquet"]

@F.udf
def extract_soup(x):
    soup = BeautifulSoup(x,features="html.parser")
    all_links = soup.find_all('a', {"href": re.compile(regex_search)})
    k = [k.get("href") for k in all_links]
    return k


def run():
    files_partitioned = chunks(files, 400)
    for i, file_paths in enumerate(files_partitioned):
        if i == 14 or i == 18:
            for file in file_paths:
                print(i)
                try:
                    data = spark.read.load(file)
                    data = data.withColumn("wiki_links", extract_soup(F.col("content")))
                    selected = data.select("url", "wiki_links")
                    selected.toPandas()
                except:
                    with open("data/bad_files.txt", "a") as f:
                        f.write("\n" + file)

if __name__ == "__main__":
    run()