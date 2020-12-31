#********************************************************************
# MERRIAM WEBSTER SCRAPING Module
#********************************************************************

# Python Imports
import requests
from bs4 import BeautifulSoup, SoupStrainer


# Project Imports
from src.scraper import get_url_data

#--------------------------------------------------------------------
# Merriam Webster Scraper
#--------------------------------------------------------------------

#--------------------------------------------------------------------
# Word Part Of Speech
#--------------------------------------------------------------------

# Returns the part of speech from the Merriam Webster responnse page.

def extract_pos (response):
    pos = []
    soup = BeautifulSoup(response.content, 'lxml')
    results =  soup.find_all('span')
    print ('Span length: ' + str(len(results)))
    for link in results:
        if link.get('class')==['fl']:
            pos.append(link)
    pos = pos[0].contents
    return pos[0]

#--------------------------------------------------------------------

def get_word_pos(word):
    word_url = 'https://www.merriam-webster.com/dictionary/' + word
    response = get_url_response(word_url)
    pos = extract_pos(response)
    return pos

#--------------------------------------------------------------------
# Word Definition
#--------------------------------------------------------------------

word_def_class = ['dt', '']

def extract_definition(html):
    pos = []
    classes = []
    soup = BeautifulSoup(html)
    results =  soup.find_all('meta', {'name' : "description"})
    if len(results) > 0:
        x = results[0].attrs['content']
        x = x.split(':')
        x = x[0].split('-')
        return x[1].strip()
    else:
        return None

#--------------------------------------------------------------------

# Example URL:

MW1 = 'https://www.merriam-webster.com/dictionary/facetious'

def get_word_definition(word):
    word_url = 'https://www.merriam-webster.com/dictionary/' + word
    html = get_url_data(word_url)
    if html is not None:
        definition = extract_definition(html)
        return definition
    else:
        return None

#--------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------
