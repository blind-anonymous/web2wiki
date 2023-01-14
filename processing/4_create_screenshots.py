from tkinter import FIRST
import pandas as pd
import numpy as np
import os 
import sys

from screenshotting import *

def create_input(df):
    url_links = df["url"]
    wiki_links = df["href"]
    names = df.index
    return url_links, wiki_links,names

def take_screenshots(df, val, savedir, file_name):
    sc = Screenshotter(save_dir = savedir +"screenshots/")
    df.to_csv(savedir + f"{file_name}.csv")
    existing_screenshots = os.listdir(savedir + "screenshots/")
    existing_screenshots = [k.split(".")[0] for k in existing_screenshots]
    if val == True:
        df2 = df[~df.index.astype(str).isin(existing_screenshots)]
        max_index = df2["ind"].max()
        df = df[df["ind"]>max_index]
    url_links, wiki_links, names = create_input(df)
    sc.iterate_over(url_links,wiki_links,names)
    
def check_existance(df,dir):
    val = False
    if not os.path.exists(dir):
        sample = pd.read_csv(dir)
        sample["ind"] = sample.index
        sample.index=sample["Unnamed: 0"]
        sample.drop("Unnamed: 0",axis=1,inplace=True)
        val = True
    else:
        sample = df.sample(1000)
    return val, sample


if __name__=="__main__":
    SAMPLE = 200
    FIRST_CLASS_TYPES = ["evidence", "attribution", "ws"]
    
    first_class = pd.read_csv("/scratch/venia/web2wiki/data/validation/first_order.csv")
    all_orders = pd.read_csv("/scratch/venia/web2wiki/data/validation/across_orders.csv")
    
    
    # sample already generated datasets 
    val1, sample_full = check_existance(all_orders, "/scratch/venia/web2wiki/data/precision_recall_scoring/all_orders/all_orders.csv")
    val2, sample_first_full = check_existance(first_class, "/scratch/venia/web2wiki/data/precision_recall_scoring/first_order/first_order.csv")

    
    take_screenshots(sample_full, val1, "/scratch/venia/web2wiki/data/precision_recall_scoring/all_orders/", "all_orders")
    take_screenshots(sample_first_full,val2, "/scratch/venia/web2wiki/data/precision_recall_scoring/first_order/", "first_order")

    # # data = data[data["link_type"] == 1]
    # orders = {"zero": 0, "one": 1, "two": 2}
    
    # for key, item in orders.items():
    #     print(f"Processing order {key}")
        
    #     # process first order subtaxonomy separately 
    #     if key == "one":

    #         for first_class_type in FIRST_CLASS_TYPES:
    #             print(f"Processing {first_class_type} subtaxonomy")
    #             sc = Screenshotter(save_dir = f"/scratch/venia/web2wiki/data/precision_recall_scoring/first_order/{first_class_type}/screenshots/")
    #             subclass = first_class[first_class["first_class"] == first_class_type].sample(SAMPLE)
    #             subclass.to_csv(f"/scratch/venia/web2wiki/data/precision_recall_scoring/first_order/{first_class_type}/{first_class_type}.csv")
    #             url_links, wiki_links,names = create_input(subclass)
    #             sc.iterate_over(url_links,wiki_links,names)

    #     temp = all_orders[all_orders["order"] == item].sample(SAMPLE)
    #     temp.to_csv(f"/scratch/venia/web2wiki/data/precision_recall_scoring/all_orders/{key}.csv")
    #     sc = Screenshotter(save_dir = f"/scratch/venia/web2wiki/data/precision_recall_scoring/all_orders/{key}/screenshots/")
    #     url_links, wiki_links,names = create_input(temp)
    #     sc.iterate_over(url_links,wiki_links,names)

            
            

    

    
    