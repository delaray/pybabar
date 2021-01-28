#********************************************************************************
# Wikipedia Scraping Tools
#
# Part 1: Wikipedia Paragraphs
# Part 2: Wikipedia Sentences
#
#********************************************************************************

# Python imports
import os
import re
import pandas as pd
from functools import reduce
from bs4 import BeautifulSoup, SoupStrainer

# Project imports
from src.utils import make_data_pathname
from src.utils import tokenize_text
from src.scraper import get_url_response
from src.scraper import get_url_data
from src.database import find_topic_out_neighbors

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

#********************************************************************************
# Part 1: Wikipedia Paragraphs
#********************************************************************************

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

#********************************************************************************
# Part 2: Wikipedia Sentences
#********************************************************************************

# Return a list of sentences of the first wikipedia paragraph. Remove
# references, newlines & empty sentences.

def get_topic_sentences(topic):
    paragraph = get_wikipedia_first_paragraph(topic)
    sentences = paragraph.split('.')
    result = map(lambda x: re.sub('\[\d+\]', '', x), sentences)
    result = list(map(lambda x: x.replace('\n', ''), result))
    if '' in result:
        result.remove('')
    return result

#--------------------------------------------------------------------

def get_topics_sentences(topics):
    return reduce(lambda x, y: x + y, map(get_topic_sentences, topics))

#--------------------------------------------------------------------

def get_neighbors_sentences(topic):
    topics = [topic] + find_topic_out_neighbors(topic)
    return get_topics_sentences(topics)

#--------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------

