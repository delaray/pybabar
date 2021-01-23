#********************************************************************
# LEXICONS MODULE
#
# Part 1: Merriam Webster Scraping
# Part 2: Parts of Speech & Unknwon Words Lexicons
# Part 3: Updating Dictionary Word Definitions
# Part 4: Finding new words.
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
from src.database import find_topic
from src.database import find_topic_out_neighbors
from src.database import find_edges
from src.database import find_dictionary_word
from src.database import find_undefined_words
from src.database import update_word_definition
from src.database import add_dictionary_word
from src.wikipedia import scan_wikipedia_topic

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
                   'plural noun',
                   'noun (1)',
                   'noun (2)',
                   'verb',
                   'auxiliary verb',
                   'adjective',
                   'adverb',
                   'preposition',
                   'conjunction',
                   'definite article',
                   'indefinite article',
                   'pronoun',
                   'combining form',
                   'abbreviation',
                   'abbreviation (1)',
                   'abbreviation (2)',
                   'prefix',
                   'suffix']

#--------------------------------------------------------------------
# Base Word Part Of Speech
#--------------------------------------------------------------------

# Returns the part of speech from the Merriam Webster response page.

def get_base_word_pos (word, response=None):
    response = ensure_response(word, response)
    soup = BeautifulSoup(response.content, 'lxml')
    # Primary Part of Speeach
    results = soup.find_all('a', {'class' : "important-blue-link" })
    if results != []:
        primary = results[0].text
        entries = soup.find_all('div', {'class' : 'row entry-header'})
        entries = [x.find_all('a', {'class' : "important-blue-link"}) for x in entries]
        if entries != []:
            entries = [x[0].text for x in entries if len(x) > 0]
            return primary, list(set(entries))
        else:
            return primary, []
    else:
        return None, None

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
    if base_word is not None:
        if word.lower() != base_word.lower():
            # print('Word: ' + word, ' Base Word: ' + base_word)
            results2 = soup.find_all('span')
            for i in range(len(results2)):
                entry = results2[i]
                sclass = entry.attrs.get('class',[])
                word2 = entry.text.lower()
                if word2==word.lower() and results2[i+1].text in PARTS_OF_SPEECH:
                    pos = results2[i+1].text
        if pos is None:
            pos = primary_pos
        if pos is not None:
            return {'word' : {word : pos},
                    'base-word' : {base_word : base_pos}}
        else:
            return None
    else:
        return None

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
    soup = BeautifulSoup(html, 'lxml')
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
        definition = definition.replace("'", "")
        definition = definition.replace("\`", "")
        return definition
    else:
        return None

#--------------------------------------------------------------------
# Get Word Properties
#--------------------------------------------------------------------

def get_word_properties (word, response=None):
    response = ensure_response(word, response)
    words = get_word_pos (word, response)
    if words is not None:
        other_words = get_other_words (word, response)
        words.update(other_words)
        definition = get_word_definition(word, response)
        words.update({'definition' : definition})
        return words
    else:
        return None

#--------------------------------------------------------------------
# Add word to lexicon
#--------------------------------------------------------------------

# Adds word and all derived words to the database.
# Returns True or False.

# [<word> <pos> <base> <definition>]

def add_word_to_lexicon(word):
    properties = get_word_properties(word)
    if properties is not None:
        word_pos = properties['word'][word]
        base_word = list(properties['base-word'].keys())[0]
        base_pos = list(properties['base-word'].values())[0]
        definition =  properties['definition']
        word_entry = [word, word_pos, base_word, definition]
        base_entry = [base_word, base_pos, base_word, definition]
        other_words = properties['other-words']
        other_entries = list(map (lambda k: [k, other_words[k], base_word, definition],
                                  other_words.keys()))
        entries = [word_entry, base_entry] + other_entries
        for entry in entries:
            add_dictionary_word(entry)
        return True
    else:
        return False

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
# Part 3: Finding new words.
#********************************************************************

# Retrieves the paragraphs of a wikipedia topic and scans them into
# adding those words and their derived words into the dictionary.

def find_new_words_from_topic(topic, count=0):
    token_lists = scan_wikipedia_topic(topic)
    if token_lists is not None:
        unknown_words = []
        for tokens in token_lists:
            for token in tokens:
                if find_dictionary_word(token) is None and "'" not in token:
                    # print ('Word: ' + str(token))
                    try:
                        if token not in unknown_words:
                            success = add_word_to_lexicon(token)
                            if success==True:
                                count +=1
                            else:
                                unknown_words.append(token)
                    except Exception as err:
                        print ("Error warning: " + str(err))
        return unknown_words, count
    else:
        return None, count

#--------------------------------------------------------------------
# Find New Words from Topics
#--------------------------------------------------------------------

def find_new_words_from_topics(topics, count=0):
    unknown = []
    for topic in topics:
        if find_topic(topic) is not None:
            uw, count=find_new_words_from_topic(topic, count)
            if uw is not None:
                unknown.append(uw)
                if count%100==0:
                    print ('New words: ' + str(count))
    return unknown, count

#--------------------------------------------------------------------
# Find New Words
#--------------------------------------------------------------------

TOPICS = ['Medecine',
          'Technology',
          'News',
          'Entertainments',
          'Business',
          'Economics',
          'Geography',
          'History',
          'Science',
          'Literature',
          'Art']

#--------------------------------------------------------------------

def find_new_words(topics=TOPICS):
    unknown = []
    count = 0
    for topic in topics:
        if find_topic(topic) is not None:
            candidates = find_topic_out_neighbors(topic)
            candidates = [topic] + candidates
            uw, count= find_new_words_from_topics(candidates, count)
            unknown += uw
    return uw, count
                

#********************************************************************
# Part 4: Parts of Speech & Unknwon Words Lexicons
#********************************************************************

# Bootstapping the dictionary from Common Lisp Data Files from 2012.

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
