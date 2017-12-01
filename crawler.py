import os as os

from scraper import get_related_wikipedia_topics
from wikidb import create_wiki_db_graph_tables, add_wiki_vertices, add_wiki_edges, count_wiki_vertices

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

def crawl_wiki(depth=2, root_topic='Elephant'):
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
# Runtime
#-------------------------------------------------------------------------------

create_wiki_db_graph_tables()
crawl_wiki(depth=2)

#-------------------------------------------------------------------------------
# End of File
#-------------------------------------------------------------------------------
