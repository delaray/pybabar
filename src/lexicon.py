#********************************************************************
# LEXICONS MODULE
#
# Part 1: Merriam Webster Scraping
# Part 2: Parts of Speech & Unknwon Words Lexicons
# Part 3 : Updating Dictionary Word Definitions
#
#********************************************************************

# Python Imports
import requests
from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd


# Project Imports
from src.utils import make_data_pathname
from src.scraper import get_url_response
from src.scraper import get_url_data
from src.database import find_undefined_words
from src.database import update_word_definition

#********************************************************************
# Part 1: MERRIAM WEBSTER SCRAPING
#********************************************************************

# Example URL:

MW1 = 'https://www.merriam-webster.com/dictionary/facetious'

#--------------------------------------------------------------------
# Generic Utilities
#--------------------------------------------------------------------

def get_word_url(word):
    return 'https://www.merriam-webster.com/dictionary/' + word

#--------------------------------------------------------------------

def ensure_response(word, response=None):
    if response is None:
        word_url = get_word_url(word)
        response = get_url_response(word_url)
    return response

#--------------------------------------------------------------------
# Parts Of Speech
#--------------------------------------------------------------------

PARTS_OF_SPEECH = ['noun',
                   'verb',
                   'adjective',
                   'adverb',
                   'preposition',
                   'conjunction',
                   'article',
                   'pronoun']

#--------------------------------------------------------------------
# Base Word Part Of Speech
#--------------------------------------------------------------------

# Returns the part of speech from the Merriam Webster response page.

def get_base_word_pos (word, response):
    response = ensure_response(word, response)
    soup = BeautifulSoup(response.content, 'lxml')
    # Primary Part of Speeach
    results = soup.find_all('a', {'class' : "important-blue-link" })
    primary = results[0].text
    entries = soup.find_all('div', {'class' : 'row entry-header'})
    entries = [x.find_all('a', {'class' : "important-blue-link"}) for x in entries]
    entries = [x[0].text for x in entries]
    return primary, list(set(entries))

#--------------------------------------------------------------------
# Base Word
#--------------------------------------------------------------------

# Returns the base word of <word>. In some cases it will be the
# save, but for derived adjectives, adverbs or nouns it could
# be different. It will also return the part of speech of the
# base word as well the of speech of <word>.

def get_base_word(word, response=None):
    response = ensure_response(word, response)
    soup = BeautifulSoup(response.content, 'lxml')
    results =  soup.find_all('h1', {'class' : "hword"})
    if results != []:
        base_word = results[0].text
        primary_pos, base_pos = get_base_word_pos(word, response)
        return base_word, primary_pos, base_pos
    else:
        return None, None, None

#--------------------------------------------------------------------

def get_word_pos (word, response=None):
    response = ensure_response(word, response)
    soup = BeautifulSoup(response.content, 'lxml')
    base_word, primary_pos, base_pos = get_base_word(word, response)
    pos = None
    if base_word is not None and word.lower() != base_word.lower():
        print('\nWord: ' + word, ' Base Word: ' + base_word)
        results2 = soup.find_all('span')
        for i in range(len(results2)):
            entry = results2[i]
            sclass = entry.attrs.get('class',[])
            word2 = entry.text.lower()
            if word2==word.lower() and results2[i+1].text in PARTS_OF_SPEECH:
                pos = results2[i+1].text
    if pos is None:
        pos = primary_pos
    return {'word' : {word : pos},
            'base-word' : {base_word : base_pos}}

#--------------------------------------------------------------------
# Get Other Words
#--------------------------------------------------------------------

# Returns entries in the "Other Words from" section with their pos.

def get_other_words(word, response=None):
    response = ensure_response(word, response)
    soup = BeautifulSoup(response.content, 'lxml')
    results = {}
    spans = soup.find_all('span')
    for i in range(len(spans)):
        entry = spans[i]
        sclass = entry.attrs.get('class',[])
        word2 = entry.text.lower()
        if sclass!=[] and sclass[0]=='ure':
            pos = spans[i+1].text
            if pos in PARTS_OF_SPEECH:
                results.update({word2 : pos })
    return {'other-words' : results}
    
#--------------------------------------------------------------------
# Word Definition
#--------------------------------------------------------------------

# Returns the first definition of word.

def extract_definition(html):
    pos = []
    classes = []
    soup = BeautifulSoup(html)
    results =  soup.find_all('meta', {'name' : "description"})
    if len(results) > 0:
        x = results[0].attrs['content']
        x = x.split(':')
        # x = x[0].split('-')
        if len(x) > 0:
            return x[0].strip()
        else:
            print ("Definition: " + x)
            return x
    else:
        return None

#--------------------------------------------------------------------


def get_word_definition(word, response=None):
    response = ensure_response(word, response)
    html = response.content
    if html is not None:
        definition = extract_definition(html)
        return definition
    else:
        return None

#--------------------------------------------------------------------
# Get Word Properties
#--------------------------------------------------------------------

def get_word_properties (word, response=None):
    response = ensure_response(word, response)
    words = get_word_pos (word, response)
    other_words = get_other_words (word, response)
    words.update(other_words)
    definition = get_word_definition(word, response)
    words.update({'definition' : definition})
    return words

#--------------------------------------------------------------------
# Add word to lexicon
#--------------------------------------------------------------------

# Return a list of rows of the form:

# [<word> <pos> <base> <definition>]

def add_word_to_lexicon(word):
    properties = get_word_properties(word)
    word_pos = properties['word'][word]
    base_word = list(properties['base-word'].keys())[0]
    base_pos = list(properties['base-word'].values())[0]
    definition =  properties['definition']
    word_entry = [word, word_pos, base_word, definition]
    base_entry = [base_word, base_pos, base_word, definition]
    return [word_entry, base_entry]

    
#--------------------------------------------------------------------
# Database Operations
#--------------------------------------------------------------------

# This scrapes Merriam Webster deinitions.

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
            definition = definition.replace("'", "")
            definition = definition.replace('"', '')
            update_word_definition(id, definition)
    return True


#********************************************************************
# Part 2: Parts of Speech & Unknwon Words Lexicons
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

#--------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------
