import os

import nltk
from postgres import find_potential_subtopics
    

#-----------------------------------------------------------------------------------------
# NLTK does bot provide a good default english parser
#-----------------------------------------------------------------------------------------


sample_text = "Come hear Uncle John's Band"

def test_parser(sent=sample_text):
    parser = Parser()
    print (parser.parse(sent))

def tokenize_topic_name (topic_name):
    return nltk.word_tokenize(topic_name)

def parse_topic_name (topic_name):
    return None


    
 	
# >>> tree = nltk.bracket_parse('(NP (Adj old) (NP (N men) (Conj and) (N women)))')
# >>> tree.draw()   

def test_chart():
    nltk.parse.chart.demo(2, print_times=False, trace=0, sent='I saw John with a dog', numparses=2)


#-----------------------------------------------------------------------------------------
# End of File
#-----------------------------------------------------------------------------------------
