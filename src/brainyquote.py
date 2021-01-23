#********************************************************************************
# Brainy Scraping Tools
#********************************************************************************


#--------------------------------------------------------------------
# Imports
#--------------------------------------------------------------------

# Python imports
import os
import time
from datetime import datetime
from bs4 import BeautifulSoup, SoupStrainer
import pandas as pd

#--------------------------------------------------------------------

# Project imports
from src.utils import make_data_pathname
from src.utils import tokenize_text
from src.scraper import get_url_response
from src.scraper import get_url_data
from src.database import add_topic_quotes_1


#--------------------------------------------------------------------

TOPICS_INDEX_URL = 'https://www.brainyquote.com/topics'

#--------------------------------------------------------------------
# Topic Lists
#--------------------------------------------------------------------

TOPICS1 = ['love', 'life', 'death', 'existence', 'logic']

TOPICS2 = ['nature', 'tree', 'trees', 'plant', 'plants',
           'flower', 'flowers', 'garden', 'gardens']

TOPICS3 = ['joy', 'happiness', 'bliss', 'serenity', 'peace']

TOPICS4 = ['anger', 'hatred', 'sorrow', 'solitude', 'loneliness']

TOPICS5 = ['art', 'painting', 'poetry', 'music', 'dancing',
           'singing', 'writing', 'acting', 'opera', 'ballet', 'concert']

TOPICS6 = ['philosophy', 'intelligence', 'consciousness', 'dream', 'dreams']

TOPICS_LISTS = [TOPICS1, TOPICS2, TOPICS3, TOPICS4, TOPICS5, TOPICS6]

#--------------------------------------------------------------------
# Basic Tools
#--------------------------------------------------------------------

def get_brainyquote_url(topic, page):
    base_page = 'https://www.brainyquote.com/topics/' + topic + '-quotes'
    if page==0:
        return base_page
    else:
        return base_page + "_" + str(page)

#--------------------------------------------------------------------

def ensure_response(topic, url=None, response=None):
    if response is None:
        if url is None:
            url = get_brainyquote_url(topic)
        response = get_url_response(url)
        if response.status_code==200:
            return response
        else:
            return None
    else:
        return response

 
#--------------------------------------------------------------------
# Brainy Quote Topics
#--------------------------------------------------------------------
   
def get_bq_topic_links():
    response = get_url_response(TOPICS_INDEX_URL)
    soup = BeautifulSoup(response.content, 'lxml')
    atags = soup.find_all('a', href=True)
    results = []
    for atag in atags:
        x = atag['href']
        if "/topic_index/" in x:
            results.append('https://www.brainyquote.com' + x)
    return results

#--------------------------------------------------------------------

# Topics to skip:

SKIP_TOPICS =['Wisdom', 'Funny', 'Friendship', 'Motivational',
              'Inspirational', 'Positive', 'Life', 'Love',
              'Wisdom', 'Funny', 'Friendship']

#--------------------------------------------------------------------

def contains_skip_topic (topic):
    status = False
    for x in SKIP_TOPICS:
        if x in topic:
            status = True
    return status

#--------------------------------------------------------------------

def get_bq_topics (url):
    response = get_url_response(url)
    soup = BeautifulSoup(response.content, 'lxml')
    atags = soup.find_all('a', href=True)
    results = []
    for atag in atags:
        x = atag['href']
        if "/topics/" in x:
            topic = atag.text.replace('\n', '')
            results.append(topic)
    return results[:-16]

#--------------------------------------------------------------------

def get_all_bq_topics():
    urls = get_bq_topic_links()
    all_topics = []
    for url in urls:
        topics = get_bq_topics(url)
        all_topics += topics
    return all_topics
    
#--------------------------------------------------------------------
# Scraping Quotes
#--------------------------------------------------------------------

# Returns a dictionary of quotes and authors keyed by quotes.

def get_authors_and_quotes (topic, limit=50):

    # Initialize
    page = 0
    results = {}
    url = get_brainyquote_url(topic, page)
    response = ensure_response(topic, url=url)

    # Iterate over all BrainyQuote pages.
    while response is not None and page < limit:
        print ('Processing page ' + str(page+1) + "...")

        soup = BeautifulSoup(response.content, 'lxml')
        divs = soup.find_all('div', {'class' : 'clearfix'})

        # Parse the div entries
        for entry in divs:
            atags = entry.find_all('a')
            if len(atags)==2:
                q = atags[0].text
                a = atags[1].text
                results.update({q : a})
            elif len(atags)==3:
                q = atags[1].text
                a = atags[2].text
                results.update({q : a})
        page += 1
        
        # Update url and response.
        url = get_brainyquote_url(topic, page)
        response = ensure_response(topic, url=url)
        
    return results

#--------------------------------------------------------------------

def get_topic_quotes(topic, response=None, limit=50):

    print ('Processing quotes for topic: ' + topic)

    # Scape the data
    results = get_authors_and_quotes(topic, limit=limit)
        
    # Now place results in a DF
    rows = []
    source = 'BrainyQuote'
    url = 'https://www.brainyquote.com'
    for key, value in results.items():
        row = [value, key, topic, source, url, datetime.now()]
        rows.append(row)
    cols = ['author', 'quote', 'topic', 'source', 'source_url', 'created_on']
    
    # Return the dataframe.
    return pd.DataFrame(rows, columns=cols)

#--------------------------------------------------------------------

def get_topics_quotes(topics, response=None, limit=50):
    # Scrape the specified topics
    dfs = []
    for topic in topics:
        df = get_topic_quotes(topic, limit=limit)
        time.sleep(5)
        dfs.append(df)
    rdf = pd.concat(dfs, axis=0)
    # Return concatenated dataframes.
    return rdf

#--------------------------------------------------------------------
# FIND NEW QUOTES
#--------------------------------------------------------------------

def populate_quotes_table (limit=20, max_topics=5):
    topics = get_all_bq_topics()
    count = 0
    while len(topics) > max_topics:
        next_topics = topics[:max_topics]
        df = get_topics_quotes(next_topics, limit=limit)
        add_topic_quotes_1(df)
        print ('Quotes processed: ' + str(df.shape[0]))
        topics = topics[100:]
        count += max_topics
        print ('Topics processed: ' + str(count))
        time.sleep(10)
    return True

#--------------------------------------------------------------------
# Soup select examples
#--------------------------------------------------------------------

# atags = soup.select('a[href*="/quotes/"]')
# atags = [x for x in atags if x.next_element != '\n']

#--------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------

