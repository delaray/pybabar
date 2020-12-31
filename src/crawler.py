#********************************************************************
# WEB CRAWLING MODULE
#
# Part 1: Crawling Wikipedia
# Part 2: Scrapy
# Part 3: Main Runtime
#
#********************************************************************


#--------------------------------------------------------------------
# Imports 
#--------------------------------------------------------------------

import sys

from scraper import get_related_wikipedia_topics
from postgres import create_wiki_db_graph_tables, count_wiki_vertices
from postgres import add_wiki_vertices, add_wiki_edges 


#********************************************************************
# Part 1: Crawling Wikipedia
#********************************************************************

DEFAULT_CRAWL_DEPTH=2

#-------------------------------------------------------------------------------
# PROCEESS TOPIC
#-------------------------------------------------------------------------------

# This proccess a single topic. It adds topic and its related topics to to the wiki_vertices table
# and creates edges between topic and all related topics.

def process_topic (topic):
    add_wiki_vertices([topic])
    new_topics = get_related_wikipedia_topics(topic)
    add_wiki_vertices(new_topics)
    add_wiki_edges(topic, new_topics)
    return new_topics
      

#-------------------------------------------------------------------------------
# CRAWL WIKIPEDIA
#-------------------------------------------------------------------------------

# Ths crawls wikipedia to the specified depth stating with root_topic.

def crawl_wiki(depth=2, root_topic='Art'):
    topics = [root_topic]
    topics_count = 0
    add_wiki_vertices(topics)
    for count in range(depth):
        next_topics =[]
        for topic in topics:
            new_topics = process_topic(topic)
            print ("Added " + str(count_wiki_vertices()) + " new topics.")
            next_topics = next_topics + new_topics
        topics = next_topics
    print ("Total topiccs added: " + str(count_wiki_vertices()))

#-------------------------------------------------------------------------------

def crawl_wikipedia (depth=DEFAULT_CRAWL_DEPTH, reset=False, topic='Art'):
    if reset==True:
        create_wiki_db_graph_tables()
    crawl_wiki(depth, topic)

#****************************************************************************
# Part 2: Scrapy
#****************************************************************************

# Scrapy Documentation: https://docs.scrapy.org/en/latest/topics/spiders.html

import scrapy as sp

class MySpider(scrapy.Spider):
    name = 'example.com'
    allowed_domains = ['example.com']
    start_urls = [
        'http://www.example.com/1.html',
        'http://www.example.com/2.html',
        'http://www.example.com/3.html',
    ]

    def parse(self, response):
        self.logger.info('A response from %s just arrived!', response.url)

#****************************************************************************
# Part 2: Main Runtime
#****************************************************************************

# This crawls Wikipedia starting with <topic> and stores the results in
# the wikidb database.

def main():
    args = sys.argv
    depth = int(args[1])
    reset = args[2]
    topic = args[3]
    print (depth)
    reset = True if reset=="True" else False
    print (reset)
    crawl_wikipedia(depth, reset, topic)
  
#-------------------------------------------------------------------------------

if __name__== "__main__":
  main()

#-------------------------------------------------------------------------------
# End of File
#-------------------------------------------------------------------------------
