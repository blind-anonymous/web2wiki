import pandas as pd
from scipy.stats import entropy
import tldextract
import urllib
import re 
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql import Row
import pyarrow.parquet as pq
import pyarrow as pa
import glob


import sys
import os

import matplotlib.pyplot as plt
sys.setrecursionlimit(10000)

import re
import time
from bs4 import BeautifulSoup
sys.path.append("/scratch/venia/web2wiki/code/helpers/")


os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7"
# os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2"
spark = SparkSession.builder.getOrCreate()

@F.udf
def try_float(x):
    try:
        float(x)
        return True
    except:
        return False

@F.udf
def extract_subdomain(x):
    if len(tldextract.extract(x).subdomain) > 1:
        y = tldextract.extract(x).subdomain +"."+tldextract.extract(x).registered_domain
    else:
        y = tldextract.extract(x).registered_domain
    return y


def is_wiki(x):
    if ("wiki" in x) or ("pedia" in x):
        return 1
    else: 
        return 0 
    

pattern = re.compile(r"[12][90][89012][0123456789]/[01]\d")

def is_blog(x):
    if ("blog" in x) or (bool(pattern.search(x))): 
        return 1
    else:
        return 0

is_wiki_udf = F.udf(is_wiki, IntegerType())
is_blog_udf = F.udf(is_blog, IntegerType())

def header(x):
    if ("reference" in x.lower()) or ("bibliography" in x.lower()) or ("sources" in x.lower()):
        return 1 
    else:
        return 0 
header_udf = F.udf(header, IntegerType())
    
def attribution(x):
    if "File:" in str(x) or "Image:" in str(x):
        return 1 
    else:
        return 0 
    
attribution_udf = F.udf(attribution, IntegerType())

def normalise_title(title):
    """ Replace _ with space, remove anchor, capitalize """
    title = title.split("/")[-1]
    title = title.split("#")[0]
    title = urllib.parse.unquote(title)
    title = title.strip()
    if len(title) > 0:
        title = title[0].upper() + title[1:]
    n_title = title.replace("_", " ")
    # if '#' in n_title:
    #     n_title = n_title.split('#')[0]
    return n_title

if __name__=="__main__":
    print("Checkpoint: 1")
    df = spark.read.load("/scratch/venia/web2wiki/data/test/sanitized_wiki.parquet")
    df = df.drop_duplicates(subset = ["url","href"])

    df = df.withColumn("is_wiki", is_wiki_udf("url"))
    df = df.withColumn("is_blog", is_blog_udf("url"))
    df = df.withColumn("header_reference", header_udf("header"))
    df = df.withColumn("attribution", attribution_udf("href"))
    df.select("url","href","tag_footer","tag_header","tag_sup", "class_footer","class_header","class_sidebar","class_response","is_wiki","is_blog","header_reference","attribution").write.parquet("/scratch/venia/web2wiki/data/test/santized-er_wiki.parquet",mode="overwrite")

    
    # order_0 = ["tag_footer", "tag_header","class_footer","class_header","class_sidebar"]
    
    # for col in order_0:
    #     df = df[df[col].map(try_float)]
    # print("Checkpoint: 3")

    # evidence = ["tag_sup","is_reference"]
    # for col in evidence:
    #     df = df[df[col].map(try_float)]
    # order_2 = ["class_response"]
    # for col in order_2:
    #     df = df[df[col].map(try_float)]
    # df["0th_order"] = df[order_0].astype(float).sum(axis = 1)
    # df["evidence"] = df[evidence].astype(float).sum(axis = 1)
    # df["attribution"] = df["href"].apply(lambda x: "attribution" if "File:" in str(x) or "Image:" in str(x) else 0)

    # df["2nd_order"] = df[order_2].astype(float).sum(axis = 1)
    # print("Checkpoint: 4")


    # df["order"] = df.apply(lambda x: 2 if x["2nd_order"]>0 else 0 if x["0th_order"]>0 else 1, axis=1)
    
    # df["first_class"] = df.apply(lambda x: None if x["order"] != 1 else "attribution" if x["attribution"] != 0 else "evidence" if x["evidence"] != 0 else "ws",axis =1)


    # # df = df.dropna(subset="href")
    # # df = df[df["wiki_links"].apply(lambda x: True if "http" in str(x) else False)]
        
    # df["domain"] = df["url"].astype(str).apply(lambda x: extract_subdomain(x))
    # df["domain_count"] = df.groupby("domain")["url"].transform("count")

    # print("Writing file")
    # df.to_csv("/scratch/venia/web2wiki/data/test/santized-er_wiki.csv",index=False)