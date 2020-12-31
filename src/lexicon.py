#********************************************************************
# MERRIAM WEBSTER SCRAPING Module
#********************************************************************

# Python Imports
import requests
from bs4 import BeautifulSoup, SoupStrainer


# Project Imports
from src.scraper import get_url_response

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

def extract_definitionx(response):
    pos = []
    classes = []
    soup = BeautifulSoup(response.content, 'lxml')
    results =  soup.find_all('span')
    print ('Span length: ' + str(len(results)))
    for link in results:
        # if not link.get('class')==None:
        #     classes.append(link.get('class'))
        if link.get('class')==word_def_class:
            pos.append(link)
    #defs = classes[0].contents
    return pos # defs

#--------------------------------------------------------------------

def get_word_definition(word):
    word_url = 'https://www.merriam-webster.com/dictionary/' + word
    response = get_url_response(word_url)
    definition = extract_definition(response)
    return definition

#--------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------
