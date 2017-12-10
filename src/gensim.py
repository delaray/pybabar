#***********************************************************************************
# Gensim Tutorial: https://radimrehurek.com/gensim/tut1.html
#***********************************************************************************

import logging
from pprint import pprint 
from gensim import corpora

logging.basicConfig(format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)

documents = ["Human machine interface for lab abc computer applications",
             "A survey of user opinion of computer system response time",
             "The EPS user interface management system",
             "System and human system engineering testing of EPS",
             "Relation of user perceived response time to error measurement",
             "The generation of random binary unordered trees",
             "The intersection graph of paths in trees",
             "Graph minors IV Widths of trees and well quasi ordering",
             "Graph minors A survey"]

#---------------------------------------------------------------------------------

# remove common words and tokenize
stoplist = set('for a of the and to in'.split())
texts = [[word for word in document.lower().split() if word not in stoplist]
          for document in documents]

# remove words that appear only once
from collections import defaultdict
frequency = defaultdict(int)
for text in texts:
    for token in text:
        frequency[token] += 1

texts = [[token for token in text if frequency[token] > 1]
         for text in texts]

#---------------------------------------------------------------------------------

dictionary = corpora.Dictionary(texts)
dictionary_file = data_file =  'c:\\Projects\\Python\\data\\sample.dict'

dictionary.save(dictionary_file)  # store the dictionary, for future reference

# print(dictionary)

#---------------------------------------------------------------------------------

new_doc = "Human computer interaction"
new_vec = dictionary.doc2bow(new_doc.lower().split())

# print(new_vec)  # the word "interaction" does not appear in the dictionary and is ignored
# [(0, 1), (1, 1)]

#---------------------------------------------------------------------------------
# Runtime Stuff
#---------------------------------------------------------------------------------
 
pprint(texts)

# [['human', 'interface', 'computer'],
#  ['survey', 'user', 'computer', 'system', 'response', 'time'],
#  ['eps', 'user', 'interface', 'system'],
#  ['system', 'human', 'system', 'eps'],
#  ['user', 'response', 'time'],
#  ['trees'],
#  ['graph', 'trees'],
#  ['graph', 'minors', 'trees'],
#  ['graph', 'minors', 'survey']]            

print(dictionary.token2id)

# {'minors': 11, 'graph': 10, 'system': 5, 'trees': 9, 'eps': 8, 'computer': 0,
# 'survey': 4, 'user': 7, 'human': 1, 'time': 6, 'interface': 2, 'response': 3}


#---------------------------------------------------------------------------------
# Runtime Stuff
#---------------------------------------------------------------------------------
