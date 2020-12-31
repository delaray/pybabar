#-----------------------------------------------------------------------------
# TOPICS API
#------------------------------------------------------------------------

import nltk

from src.database import find_wiki_in_neighbors 
from src.database import find_wiki_out_neighbors

#------------------------------------------------------------------------------
# Filter Topics
#------------------------------------------------------------------------------

# Removes references to subsections and parentheisized pages.

def filter_topics (source, topics):
    # list1 = [x for x in topics if ('#' + source.lower()) not in x.lower()]
    list1 = [x for x in topics if '#' not in x.lower()]
    list2 = [x for x in list1 if '_(' not in x]
    return list(set(list2))


#------------------------------------------------------------------------------
# In & Out Topics
#------------------------------------------------------------------------------

def find_in_topics (topic_name):
    return filter_topics(topic_name, find_wiki_in_neighbors(topic_name))

#------------------------------------------------------------------------------

def find_out_topics (topic_name):
    return filter_topics(topic_name, find_wiki_out_neighbors(topic_name))

#------------------------------------------------------------------------------

def find_related_topics (topic_name):
    topics = find_in_topics(topic_name) + find_out_topics(topic_name)
    return list(set(topics))

#------------------------------------------------------------------------------
# SUBTOPICS
#------------------------------------------------------------------------------

# Find _potential subtopics

def find_potential_subtopics (topic_name):
    result = []
    in_vertices =  set(find_wiki_in_neighbors(topic_name))
    out_vertices = set(find_wiki_out_neighbors(topic_name))
    vertices = list(in_vertices.union(out_vertices))
    for x in vertices:
        if topic_name.lower() in x.lower():
            result.append(x)
    return filter_topics (topic_name, result)

#------------------------------------------------------------------------------

def compute_wiki_subtopics(topic_name):
    topics = find_potential_subtopics(topic_name)
    subtopics = []
    for topic1 in topics:
        topic2 = topic1.replace('_', ' ')
        tokens = nltk.word_tokenize(topic2)
        if tokens[-1].lower() == topic_name.lower():
            subtopics.append(topic1)
    subtopics = [x for x in subtopics if x.lower() != topic_name.lower()]
    return subtopics

#------------------------------------------------------------------------------

# This computes the subtopics of topic_name and adds subtopic and supertopic
#  edges to the graph.

def add_wiki_subtopics(topic_name):
    subtopics = compute_wiki_subtopics (topic_name)
    for subtopic in subtopics:
        add_wiki_edge(subtopic, topic_name, edge_type='subtopic')
        add_wiki_edge(topic_name, subtopic, edge_type='supertopic')
    conn.commit()

#------------------------------------------------------------------------------

def subtopic_p (topic1, topic2):
    topic =  topic2.replace('_', ' ')
    tokens = nltk.word_tokenize(topic)
    subtopicp = True
    for t in tokens:
        if not t.lower() in topic1.lower():
            subtopicp = False
    return subtopicp

#------------------------------------------------------------------------------
# End Of File
#------------------------------------------------------------------------------
