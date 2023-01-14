import requests

from bs4 import BeautifulSoup
import re
import bz2
import sys

page_view_link = "https://dumps.wikimedia.org/other/pageviews/"

def create_page_view_links(year,month):
    # month needs to be in the form %%

    resp_text = requests.get(f"{page_view_link}/{year}/{year}-{month}").text
    p = BeautifulSoup(resp_text, "lxml")

    links = p.find_all("a", {"href": re.compile(r"pageviews+")})
    links = [f"{page_view_link}/{year}/{year}-{month}/{k.get('href')}" for k in links]
    return links

def write_links(out_file,year,month):
    links = create_page_view_links(year,month)
    with open(out_file, "wt") as f:
        for link in links:
            f.write(link +"\n")

if __name__ == "__main__":
    out_file = sys.argv[1]
    year  = sys.argv[2]
    month = sys.argv[3]
    write_links(out_file, year, month)

