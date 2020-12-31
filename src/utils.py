#***********************************************************************
# Miscellaneous Utility Functions
#***********************************************************************

# Standard Python imports
import os
import re
from functools import reduce
from itertools import chain, zip_longest

# Data science imports
import nltk
import numpy as np
import pandas as pd


#--------------------------------------------------------------------
# Project Directory and Pathnames
#--------------------------------------------------------------------

# The environment variable 'PYBAR_DIR' should contains the project
# root directory.

def get_pybar_dir():
    try:
        return os.environ['PYBAR_DIR']
    except KeyError:
       print ("Error: Unable to access environment variable PYBAR_DIR.")
       return None

#--------------------------------------------------------------------

PYBAR_DIR = get_pybar_dir()

#--------------------------------------------------------------------

# Directory for storing training data

def make_data_pathname (filename):
    return os.path.join(PYBAR_DIR, 'data', filename)

#--------------------------------------------------------------------

# Directory for storing result files.

def make_results_pathname (filename):
    return os.path.join(PYBAR_DIR, 'results', filename)

#--------------------------------------------------------------------

# Directory for storing models

def make_models_pathname (filename):
    return os.path.join(PYBAR_DIR, 'models', filename)

#------------------------------------------------------------------------------------

CLASSIFIER_SUFFIX = 'classifier'

def make_model_name (category_name):
    category_name = category_name.split(' ')[0]
    return category_name.lower() + '_' +  CLASSIFIER_SUFFIX

#------------------------------------------------------------------------------------
# Count Words
#------------------------------------------------------------------------------------

def count_words (sentences, min_count=2000):
    words = {}
    for sentence in sentences:
        tokens = tokenize_text(sentence, alphabetic_only=True)
        for token in tokens:
            if token not in STOP_WORDS and  len(token) > 3:
                if token in sentence:
                    words.update({token : words.get(token, 0) + 1})
    if len(words) > 0:
        df = pd.DataFrame.from_dict(words, orient='index')
        df.columns = ['count']
        words = list(zip(list(df.index), list(df['count'])))
        df = pd.DataFrame(words, columns=['word', 'count'])
        df = df.loc[df['count'] >= min_count]
        df = df.sort_values(['count'], ascending=False)
        return df
    else:
        return None

#------------------------------------------------------------------------------------
# Find First Word
#------------------------------------------------------------------------------------

# Return the word in words that occurs earliest on average in descriptions.

def find_first_word(words, descriptions):
    position_results = []
    for word in words:
        positions = list(map(lambda desc: desc.find(word), descriptions))
        position = int(reduce(lambda a, b: a + b, positions) / len(positions))
        position_results.append([word, position])
        position_results.sort(key=lambda x: x[1], reverse=True)
    return position_results[0][0]

#------------------------------------------------------------------------------------
# Select Best Word
#------------------------------------------------------------------------------------

# Return the most frequently ocuring word in descriptions. Ties are handled using
# find_first_word above.

# TODO: Handle sequences of words.

def select_best_word(descriptions, words_df):
    max_count = words_df['count'].max()
    max_words = list(words_df.loc[words_df['count']==max_count]['word'])
    if len(max_words)==1:
        return max_words[0]
    else:
        return find_first_word(max_words, descriptions)

#-------------------------------------------------------------------------------------------
# Remove stop words
#-------------------------------------------------------------------------------------------

STOP_WORDS = ['le', 'la', 'les', 'un', 'une', 'de', 'mm', 'cm', 'kg', 'en', 'dans',
              'avec', 'pour', 'sans', 'à', 'a', 'et', 'ou', 's', 'a', 'l', 'v']

def remove_stop_words(tokens):
    tokens = tokens.copy()
    for stop_word in STOP_WORDS:
        if stop_word in tokens:
            tokens.remove(stop_word)
    return tokens

#-------------------------------------------------------------------------------------------
# Aphabetic words
#-------------------------------------------------------------------------------------------

# Returns True if word only contains alpabetic characters.

def alphabetic_word_p(word):
    m =  re.match(r'[^\W\d]*$', word)
    if m == None:
        return False
    else:
        return True

#-------------------------------------------------------------------------------------------
# Tokenize Text
#-------------------------------------------------------------------------------------------

# Tokenize text and apply filters.
# TODO: Clarify logic below and use filter function from functional tools library.

def tokenize_text(text, alphabetic_only=False ):
    if type(text) != str:
        text = str(text)
    tokens = nltk.word_tokenize(text.lower())
    tokensFiltered = []
    for token in tokens:
        alpha_word_p = alphabetic_word_p(token)
        # stop_word_p = token in stopWords
        # condition_1 = remove_stopwords==False or not stop_word_p
        condition_1 = True
        condition_2 = alphabetic_only==False or alpha_word_p
        if condition_1 and condition_2:
            tokensFiltered.append(token)
    return tokensFiltered


#*******************************************************************************************
# Data Manipulation
#*******************************************************************************************

# Partitons data into training and test data by training_percent

def partition_data(data, training_percent):
    rows = data.shape[0]
    training_size = int(my_round(rows * training_percent / 100))
    test_size = int(my_round(rows * (100 - training_percent) / 100))
    training_data = data[:training_size]
    test_data = data[training_size:]
    return training_data, test_data

#--------------------------------------------------------------------

# Shuffles data using Pandas sample function.

def shuffle_data(df):
    return df.sample(frac=1).reset_index(drop=True)

#------------------------------------------------------------------------

# Mostly used for printing purposes.

def my_round (n, precision=2):
    factor = (10**precision)
    return  int(n * factor) / factor

#------------------------------------------------------------------------

def read_excel(file):
    df = pd.read_excel(file, engine='openpyxl')
    return df

#------------------------------------------------------------------------

def print_list (items):
    for x in items:
        print(str(x))
    return True

#-------------------------------------------------------------------------

def clean_df(df):
    # Replace NaNs
    df = df.fillna('')
    # REmove leading and trainling whitespace
    df = df.apply(lambda x: x.str.strip().str.capitalize() if x.dtype == "object" else x)
    # Eliminatye duplicated rows
    df = df.drop_duplicates()
    # Return clean df
    return df

#*************************************************************************
# Invented Data Generation
#*************************************************************************

#-------------------------------------------------------------------------
# PARTITION
#-------------------------------------------------------------------------

def partition (tokens, partitions=2):
    size = int(np.round(len(tokens)/partitions))
    token_sets = [tokens[i*size:(i+1)*size] for i in list(range(partitions))]
    return token_sets

#-------------------------------------------------------------------------------------------

def interleave_lists(l1, l2):
    c = list(chain.from_iterable(zip_longest(l1, l2)))
    return [x for x in c if x is not None]

#-------------------------------------------------------------------------------------------

# This combines the tokens in l1 with the tokens in l2 in one of three possible ways:
# (1) halving, (2) interleaving and (3) reversing.

def apply_combination_method(l1, l2, method):
    if method=='reversing':
        token_list = (l1 + l2)[::-1]
    elif method=='interleaving':
        token_list = interleave_lists(l1, l2)
    else:
        token_list = l1 + l2
    return token_list

#-------------------------------------------------------------------------------------------

# This considers all pairs of token lists from the two specified token lists
# and combines each pair in one of three possible ways in order to invent data.
# (1) halving, (2) interleaving and (3) reversing.

# We assume the token lists in  each in both have the same classification.

def combine_token_lists(token_lists1, token_lists2,  method='halving', limit=None):
    new_token_lists = []
    count = 0
    
    for l1 in token_lists2:
        for l2 in token_lists1:
            if (limit==None or count < limit) and l1 != [] and l2 != []:
                count += 1
                if count % 100000 == 0:
                    print ("Generated " + str(count) + " new rows of training data...")
                new_tokens = apply_combination_method(l1, l2, method)
                p =  ' '.join(new_tokens)
                new_token_lists.append(p)
            else:
                break
            
    return new_token_lists 
    
#-------------------------------------------------------------------------------------------

# This generates new sentences from the sentences in DF. Three text invention strategies
# are supported halving, interleaving and reversing.

# Assume df has 'decription' and 'class' columns

def invent_text(df, method='halving', limit=100000):
    classes = list(df['class'].unique())
    print ('Inventing training data for ' + str(len(classes)) + ' classes...')

    dfs = []

    # Handle each class seperately
    for c in classes:
        cdf = df.loc[df['class']==c]
        sentences = list(cdf['description'])
        if len(sentences) < limit:
            token_lists = [tokenize_text(s) for s in sentences]
            tokens_partitions = [partition(tokens) for tokens in token_lists]
    
            tlists1 = [p[0] for p in tokens_partitions]
            tlists2 = [p[1] for p in tokens_partitions]
    
            new_texts = combine_token_lists(tlists1, tlists2, method, limit)
            new_texts = new_texts[:limit]

            new_values = [[x, c] for x in new_texts]
  
            new_df = pd.DataFrame(new_values, columns=['description', 'class'])
            dfs.append(new_df)
        
    # Combine dataframes
    results_df = pd.concat(dfs, axis=0)
    print ('Total invented training data: ' + str(results_df.shape[0]))
    return results_df

#-------------------------------------------------------------------------------------------
# Clean Sentence
#-------------------------------------------------------------------------------------------

# Defined for convenience. Tokenizes then the reasembles.

def clean_sentence (s):
    tokens = tokenize_text(s)
    return ' '.join(tokens)

#--------------------------------------------------------------------
# End of File
#--------------------------------------------------------------------
