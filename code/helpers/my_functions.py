import glob
import os 

import pandas as pd
import numpy as np
import os 
import matplotlib.pyplot as plt

import tldextract
import urllib

import sys
import re

import sys
sys.path.append("/scratch/venia/web2wiki/")


def extract_domain(x):
    y = tldextract.extract(x).registered_domain
    return y

def extract_subdomain(x):
    if len(tldextract.extract(x).subdomain) > 1:
        y = tldextract.extract(x).subdomain +"."+tldextract.extract(x).registered_domain
    else:
        y = tldextract.extract(x).registered_domain
    return y


def extract_suffix(x):
    y = tldextract.extract(x).suffix
    return y



def wiki_link_processing(x):
    y = x[1:-1]
    return y


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