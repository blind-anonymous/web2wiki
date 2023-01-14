print("====================")
import pandas as pd 
import glob

import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
import pyarrow.parquet as pq
import pyarrow as pa


from settings import DATA_DIR, EMBEDDING_DIR

os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7"
os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2"
spark = SparkSession.builder.getOrCreate()

print("====================")

import sys
sys.path.append("/scratch/venia/web2wiki/")
from settings import DATA_DIR

files = glob.glob(DATA_DIR + "page_views/2021-02/pageviews-*")
   

def process_df(df):
    df = df.filter(~F.col("wikidb").rlike(r"[a-z]*.m"))
    return df 

def process(files):
    schema = "wikidb STRING, title STRING, views INT, download INT"
    df = spark.read.option("delimiter", " ").option("header", "false").csv(files, schema = schema)
    grouped = df.groupBy(["wikidb", "title"]).agg(F.sum("views").alias("views"))
    grouped.write.parquet(DATA_DIR + "page_views/2021-02/all_views.parquet")
    
if __name__ == "__main__":
    print("Processing files")
    process(files)