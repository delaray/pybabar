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
from src.utils import concat_dfs
from src.scraper import get_url_response
from src.scraper import get_url_data
from src.database import find_topic_out_neighbors
from src.database import find_potential_subtopics

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
        if len(paragraphs) > i:
            if paragraphs[i].find_all('b') != []:
                break
            else:
                index += 1
        else:
            return None
    if paragraphs != [] and len(paragraphs) >= index:
        return paragraphs[index].text
    else:
        return None

#--------------------------------------------------------------------
# Get Wikipedia All Paragraphs
#--------------------------------------------------------------------

# Look for all <p>'s then select first one containing <b>.

def get_wikipedia_paragraphs (topic, response=None):
    response = ensure_response(topic, response)
    if response is not None:
        soup = BeautifulSoup(response.content, 'lxml')
        paragraphs = soup.find_all('p')
        results = [p for p in paragraphs if len(p.text) > 25]
        if results != []:
            return [r.text for r in results]
        else:
            return None
    else:
        return None

#--------------------------------------------------------------------
# Scan Wikipedia Topic
#--------------------------------------------------------------------

# Retuens lists of lists of tokens.

def scan_wikipedia_topic(topic, response=None):
    response = ensure_response(topic, response)
    paragraphs = get_wikipedia_paragraphs (topic, response)
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
    # print ("Topic: " + topic)
    paragraphs = get_wikipedia_paragraphs(topic)
    if paragraphs is not None:
        results = []
        for paragraph in paragraphs:
            sentences = paragraph.split('.')
            result = map(lambda x: re.sub('\[\d+\]', '', x), sentences)
            result = list(map(lambda x: x.replace('\n', ''), result))
            if '' in result:
                result.remove('')
            results +=  result
        return results
    else:
        return []

#--------------------------------------------------------------------

def get_topics_sentences(topics):
    return reduce(lambda x, y: x + y, map(get_topic_sentences, topics))

#--------------------------------------------------------------------

def get_neighbors_sentences(topic):
    topics = [topic] + find_topic_out_neighbors(topic)
    return get_topics_sentences(topics)

#********************************************************************
# Part 3: Wikipedia Training Data 
#********************************************************************

def compute_topic_training_data (topic):
    data = []
    sentences = get_topic_sentences(topic)
    for sentence in sentences:
        data.append([topic, topic, sentence])
    neighbors = find_potential_subtopics(topic)
    print ('Neighbor count: ' + str(len(neighbors)))
    for neighbor in neighbors:
        neighbor = neighbor[1]
        # print ('Processing topic: ' + neighbor)
        sentences = get_topic_sentences(neighbor)
        for sentence in sentences:
            data.append([topic, neighbor, sentence])
    df = pd.DataFrame(data, columns = ['topic', 'subtopic', 'sentence'])
    print ('Total art related sentences: ' + str(df.shape[0]))
    return df


#--------------------------------------------------------------------
# Compute Topics Training Data
#--------------------------------------------------------------------

def compute_topics_training_data(topics):
    rdf = None
    dfs = []
    for topic in topics:
        print ('Processing topic: ' + topic)
        df = compute_topic_training_data(topic)
        dfs.append(df)
    rdf = concat_dfs(dfs)
    rdf.reset_index(inplace=True)
    return rdf

#--------------------------------------------------------------------
# Generate Topics Training Data
#--------------------------------------------------------------------

TRAINING_DATA_FILE = make_data_pathname('training_data.csv')

TOPICS = ["Art", "Science"]

def generate_topics_training_data(topics, file=TRAINING_DATA_FILE):
    df = compute_topics_training_data(topics)
    df.to_csv(file, index=False)
    return df

#--------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------

