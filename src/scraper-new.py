#********************************************************************
# WEBPAGE SCRAPING MODULE
#
# Part 1: NLP Token Level Tools
# Part 2: Generic Web Scraping Tools
# Part 3: Perfumes Specific Scraping
#
#********************************************************************


#--------------------------------------------------------------------
# Imports 
#--------------------------------------------------------------------

# Standard Python
import os
import sys
import re
import itertools

# Data Frames
import pandas as pd

# Web tools
import requests
from bs4 import BeautifulSoup, SoupStrainer
from urllib.parse import urlparse,quote, unquote
from urllib.request import Request, urlopen


# Prpoject Imports
from src.utils import make_data_pathname
from src.utils import clean_sentence

#********************************************************************
# Part 1: Generic Web Scraping Tools
#********************************************************************

REQUEST_HEADERS = {'User-Agent': 'Chrome/83.0.4103.97',
                   'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
                   'Accept-Encoding': 'none',
                   'Accept-Language': 'en-US,en;q=0.8',
                   'Connection': 'keep-alive'}

# Returns a response object

def get_url_response (url, params={}):
    try:
        return requests.get(url, headers=REQUEST_HEADERS, params=params)
    except Exception:
        return None

#--------------------------------------------------------------------

def get_url_data (url, params={}):
    response = get_url_response(url, params)
    if response==None:
        return None
    else:
        return response.content

#--------------------------------------------------------------------

# Extracts the content of the response of a request.

# def get_url_data (url, params={}):
#     #url = url + '/#sortField=oi&sortAsc=false&venues=3&page=1&cleared=1&group=1'
#     url = url + '/#sortField=oi&sortAsc=false&venues=3&cleared=1&group=1'
#     headers = {'User-Agent': 'Chrome/83.0.4103.97',
#                'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
#                'Accept-Encoding': 'none',
#                'Accept-Language': 'en-US,en;q=0.8',
#                'Connection': 'keep-alive'}
#     request = Request(url, headers=headers)
#     return urlopen(request, timeout=20).read()

#--------------------------------------------------------------------

def link_contains_stop_word (link, stop_words):
    result = False
    for x in stop_words:
        if x in link['href']:
            result = True
    return result

#--------------------------------------------------------------------
# URL Predicates
#--------------------------------------------------------------------

def full_url_p (url):
    return ('http://' in url) or ('https://' in url)

#--------------------------------------------------------------------

def make_full_url (url, full_url):
    urlcomps = urlparse(full_url)
    return urlcomps.scheme + '://' + urlcomps.netloc + url

#--------------------------------------------------------------------'

def same_domain_p (url1, url2):
    comps1 = urlparse(url1)
    comps2 = urlparse(url2)
    return comps1.netloc==comps2.netloc

#--------------------------------------------------------------------'

def make_domain_url (url):
    comps = urlparse(url)
    return comps.scheme + '://' + comps.netloc + '/'

#--------------------------------------------------------------------

def internal_link_p (url, site_url):
    return same_domain_p (url, site_url)

#--------------------------------------------------------------------
# URL Extraction.
#--------------------------------------------------------------------

def extract_urls (url, filter='', stop_words=[]):
    response = get_url_response(url)
    urls = []
    if response is not None:
        soup = BeautifulSoup(response.content, 'lxml')
        for link in soup.find_all('a', href=True):
            if filter in link['href'] and not link_contains_stop_word (link, stop_words):
                urls.append([(unquote(link['href'])), link.get_text()])
        #urls = list(set(urls))
    return urls

#--------------------------------------------------------------------

def extract_full_urls(url, root_url, filter='', stop_words=[]):
    urls = extract_urls(url, filter, stop_words)
    full_urls = []
    for u in urls:
        if not full_url_p(u):
            full_url = make_full_url(u, root_url)
        else:
            full_url = u
        full_urls.append(full_url)
    return full_urls
 
#--------------------------------------------------------------------

def extract_internal_urls(url, root_url, filter='', stop_words=[]):
    urls = extract_full_urls(url, root_url, filter, stop_words)
    result = []
    for u in urls:
        if internal_link_p(u, root_url) == True:
            result.append(u)
    return(result)

#--------------------------------------------------------------------

def extract_text(url):
    content = get_url_data(url)
    text = []
    if content is not None:
        soup = BeautifulSoup(content, 'lxml')
        for x in soup.find_all('p'):
            text.append(x.get_text())
        text = [unquote(x)  for x in text if len(x) > 2]
    return text

#--------------------------------------------------------------------

def extract_clean_text (url):
    text = extract_text(url)
    clean = [clean_sentence(x) for x in text]
    filtered = [x for x in clean if len(x.split(' ')) > 1]
    return filtered


#********************************************************************
# Part 2: Famous People
#********************************************************************

# Data is spread across 6 pages.

FAMOUS_PEOPLE_URLS = [
    'https://www.onthisday.com/famous-people.php?p=1',
    'https://www.onthisday.com/famous-people.php?p=2',
    'https://www.onthisday.com/famous-people.php?p=3',
    'https://www.onthisday.com/famous-people.php?p=4',
    'https://www.onthisday.com/famous-people.php?p=5',
    'https://www.onthisday.com/famous-people.php?p=6']

#--------------------------------------------------------------------

def get_fp_html(url):
    return get_url_data(url)

#--------------------------------------------------------------------

def get_fp_page(url):
    page = int(url[-1])
    html = get_url_data(url, {'p' : page})
    soup = BeautifulSoup(html)
    entries = soup.findAll("li", {"class": "famous-people__item"})
    results = [x.text for x in entries]
    return results

#--------------------------------------------------------------------

def get_fp_list(urls=FAMOUS_PEOPLE_URLS):
    all_results = []
    for url in urls:
        results = get_fp_page(url)
        all_results += results
    df = pd.DataFrame(all_results, columns=['name'])
    return df

#--------------------------------------------------------------------

FAMOUS_PEOPLE_FILE = make_data_pathname('famous_people_onthisday.csv')

def save_fp (df, file='famous_people_onthisday.csv'):
    df.to_csv(file, index=False)
    return True

#--------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------
