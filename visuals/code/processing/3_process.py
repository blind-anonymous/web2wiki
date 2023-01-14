import enum
from http.client import responses
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
import glob
import numpy as np
import re

import sys
sys.path.append("/scratch/venia/web2wiki/code/helpers/")
sys.path.append("/scratch/venia/web2wiki/code/iterative_coding/")

sys.setrecursionlimit(10000)

import re
import time
from bs4 import BeautifulSoup

from settings import WIKI_PAGES_DIR
import helping_functions as hf

os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7/"
os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2/"
spark = SparkSession.builder.getOrCreate()

if __name__=="__main__":
    """This huge bank of code just processes what we did in 2_feature_extraction"""
    OUTPUT_DIR = ""
    
    files = glob.glob("/scratch/venia/web2wiki/data/web_content/iterative_coding_sample/tag_info_55_*")
    df = spark.read.load(files)
    df = df.filter(F.size('processed') > 0)
    df2 = df.withColumn("element",F.explode("processed"))
    df2 = df2.select("url","element")
    df2 = df2.withColumn("href", F.col("element.href"))
    df2 = df2.withColumn("header",  F.col("element.header"))
    df2 = df2.withColumn("tags",  F.col("element.tags"))
    df2 = df2.withColumn("classes", F.col("element.classes"))
    df2 = df2.withColumn("nbhd_text", F.col("element.nbhd_text"))
    df2=df2.withColumn('tag_footer', F.col('element.tags').getItem(0)).withColumn('tag_footer', F.col("tag_footer.tag_count"))
    df2=df2.withColumn('tag_header', F.col('element.tags').getItem(1)).withColumn('tag_header', F.col("tag_header.tag_count"))
    df2=df2.withColumn('tag_sup', F.col('element.tags').getItem(2)).withColumn('tag_sup', F.col("tag_sup.tag_count"))
    df2=df2.withColumn('class_footer', F.col('element.classes').getItem(0)).withColumn('class_footer', F.col("class_footer.class_count"))
    df2=df2.withColumn('class_header', F.col('element.classes').getItem(1)).withColumn('class_header', F.col("class_header.class_count"))
    df2=df2.withColumn('class_sidebar', F.col('element.classes').getItem(2)).withColumn('class_sidebar', F.col("class_sidebar.class_count"))
    df2=df2.withColumn('class_response', F.col('element.classes').getItem(3)).withColumn('class_response', F.col("class_response.class_count"))

    dff2 = df2.drop("element","tags","classes")
    
    dff2.write.parquet(OUTPUT_DIR)

