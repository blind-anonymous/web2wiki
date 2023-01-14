from extraction_helpers import * 
import enum
from http.client import responses
import pandas as pd 
import os
import pyspark.sql.functions as F
from pyspark.sql.types import *
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
import glob
import sys
sys.path.append("/scratch/venia/web2wiki/code/helpers/")
sys.path.append("/scratch/venia/web2wiki/code/iterative_coding/")

import re
import time

from settings import WIKI_PAGES_DIR
import helping_functions as hf

os.environ['SPARK_HOME'] = "/home/veselovs/spark-3.2.1-bin-hadoop2.7/"
os.environ['JAVA_HOME'] = "/home/veselovs/jdk-13.0.2/"
spark = SparkSession.builder.getOrCreate()

if __name__ == "__main__":
    iteration = "55"
    files = glob.glob(WIKI_PAGES_DIR+"/*")
    print("==================")
#     df = spark.read.load("/dlabdata1/dlab_common_datasets/web2wiki/FullHTMLJan2021.parquet")
    files_partitioned = chunks(files, 64)
    for i, file_paths in enumerate(files_partitioned):
        try:
            if i > 31:
                pd.Series(files).to_csv(f"/scratch/venia/web2wiki/data/iterated_coding/temp_files/files_{iteration}_{i}.csv")

            #     print(file_paths)
                print("==================")
                print(f"We are on partition {i}")
                t = time.time()
                df = spark.read.load(file_paths)
                df = df.withColumn("processed", process_all_udf("content"))
                df = df.drop(F.col("content"))

                df.write.parquet(f"/scratch/venia/web2wiki/data/web_content/iterative_coding_sample/tag_info_{iteration}_{i}.parquet", mode = "overwrite")
                print("It took {:.2f} seconds to write one chunk.".format(time.time() - t))
                df.unpersist()
                del df
        except: 
            print(f"Iteration {i} broke down, skipping to next iteration.")
