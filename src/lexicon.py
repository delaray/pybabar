#********************************************************************
# LEXICONS MODULE
#
# Part 1: Merriam Webster Scraping
# Part 2: Parts of Speech Lexicon
#
#********************************************************************

# Python Imports
import requests
from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd


# Project Imports
from src.utils import make_data_pathname
from src.scraper import get_url_data
from src.database import find_undefined_words
from src.database import update_word_definition

#********************************************************************
# Part 1: MERRIAM WEBSTER SCRAPING
#********************************************************************

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
        if len(x) > 1:
            return x[1].strip()
        else:
            print ("Definition: " + x[0])
            return x[0]
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

#********************************************************************
# Part 2: Parts of Speech Lexicon
#********************************************************************

#--------------------------------------------------------------------
# Load Parts of Speech
#--------------------------------------------------------------------

POS_FILE = make_data_pathname('parts-of-speech.csv')

def load_parts_of_speech_lexicon(file=POS_FILE):
    columns = ['word', 'base', 'pos', 'type', 'all-pos']
    df = pd.read_csv(file, names=columns, encoding='latin-1')
    return df


#--------------------------------------------------------------------
# Load Unknown Words
#--------------------------------------------------------------------

UNKNOWN_WORDS_FILE = make_data_pathname('unknown-words.csv')

def load_unknown_words_lexicon(file=UNKNOWN_WORDS_FILE):
    df = pd.read_csv(file, names=['word', 'status'], encoding='latin-1')
    return df


#********************************************************************
# Part 3 : Dictionary Word Definitions
#********************************************************************

def update_word_definitions():
    rows = find_undefined_words()
    count = 0
    for row in rows:
        count += 1
        if count%100 == 0:
            print ("Updated word definitions: " + str(count))
        id = row[0]
        word = row[1]
        definition = get_word_definition(word)
        if definition is not None:
            update_word_definition(id, definition)
    return True

#--------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------
