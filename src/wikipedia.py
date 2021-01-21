#********************************************************************************
# Wikipedia Scraping Tools
#********************************************************************************

# Python imports
import os
from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd

# Project imports
from src.utils import make_data_pathname
from src.utils import tokenize_text
from src.scraper import get_url_response
from src.scraper import get_url_data

#--------------------------------------------------------------------
# Basic Tools
#--------------------------------------------------------------------

def get_wikipedia_url(topic):
    return 'https://en.wikipedia.org/wiki/' + topic


#--------------------------------------------------------------------

def ensure_response(topic, response=None):
    if response is None:
        topic_url = get_wikipedia_url(topic)
        response = get_url_response(topic_url)
    return response


#--------------------------------------------------------------------
# Get Wikipedia First Paragraph 
#--------------------------------------------------------------------

# Look for all <p>'s then select first one containing <b>.

def get_wikipedia_first_paragraph (topic, response=None):
    response = ensure_response(topic, response)
    soup = BeautifulSoup(response.content, 'lxml')
    paragraphs = soup.find_all('p')
    index = 0
    for i in range(10):
        if paragraphs[i].find_all('b') != []:
            break
        else:
            index += 1
    if paragraphs != [] and len(paragraphs) >= index:
        return paragraphs[index].text
    else:
        return None

#--------------------------------------------------------------------
# Get Wikipedia All Paragraphs
#--------------------------------------------------------------------

# Look for all <p>'s then select first one containing <b>.

def get_wikipedia_all_paragraphs (topic, response=None):
    response = ensure_response(topic, response)
    soup = BeautifulSoup(response.content, 'lxml')
    paragraphs = soup.find_all('p')
    results = [p for p in paragraphs if len(p.text) > 25]
    if results != []:
        return [r.text for r in results]
    else:
        return None

#--------------------------------------------------------------------
# Scan Wikipedia Topic
#--------------------------------------------------------------------

# Retuens lists of lists of tokens.

def scan_wikipedia_topic(topic, response=None):
    response = ensure_response(topic, response)
    paragraphs = get_wikipedia_all_paragraphs (topic, response)
    if paragraphs is not None:
        results = []
        for p in paragraphs:
            tokens = tokenize_text(p)
            tokens = filter(lambda x: len(x) > 1, tokens)
            tokens = [x for x in tokens if not any(y.isdigit() for y in x)]
            results.append(list(tokens))
        return results
    return None
   
#--------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------

