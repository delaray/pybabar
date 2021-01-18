#********************************************************************************
# Wikipedia Scraping Tools
#********************************************************************************

# Python imports
import os
from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd

# Project imports
from src.utils import make_data_pathname
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
# End of File
#--------------------------------------------------------------------

