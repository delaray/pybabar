#**************************************************************************************
# WORD EMBEDDINGS
#**************************************************************************************
#
# Original paper: https://arxiv.org/pdf/1301.3781.pdf
#
#
# Referencs
#
# //towardsdatascience.com/word2vec-skip-gram-model-part-1-intuition-78614e4d6e0b
# //towardsdatascience.com/word2vec-skip-gram-model-part-2-implementation-in-tf-7efdf6f58a27
# https://github.com/Hironsan/awesome-embedding-models
# 
# Implementation Details
#
# This code uses the Gensim Python Library and their implemention of Word2Vec to build
# word embedding models.
#
#*************************************************************************************

# Disable gensim warning
import warnings
warnings.filterwarnings(action='ignore', category=UserWarning, module='gensim')

# Standar Python Libraries
import os
import json

# ML Libraries
import numpy as np
import gensim

# Project Libraries
import src.utils as ut

#-------------------------------------------------------------------------------------
# Embedding Parameters
#-------------------------------------------------------------------------------------

# Word vector dimensionality
VECTOR_SIZE = 100

# Minimum number of word occurences for inclusion in vocabulary
MIN_COUNT=5

# Word embedding window
WINDOW=5

#--------------------------------------------------------------------------------------
# Default Model Files
#--------------------------------------------------------------------------------------

# Gensim word embedding model files.

GENSIM_MODEL_FILE = 'gensim_embeddings.model'

WEIGHTS_FILE = 'gensim_weights.npz'
VOCAB_FILE = 'gensim_vocab.json'

#--------------------------------------------------------------------------------------
# Creating Word Embeddings
#--------------------------------------------------------------------------------------

# Returns a Gensim Word2Vec model and the list tokenized sentences.

# Sentences is a list of space delimited strings. Initially use a vector length of 100. 

def create_embeddings(sentences, size=VECTOR_SIZE, min_count=MIN_COUNT, window=WINDOW):
    
    tokenized_sentences = []
    
    for s in sentences:
        if type(s) == str:
            # Returns a list of sentences
            ts = ut.tokenize_text(s)
            tokenized_sentences.append(ts)
        
    model = gensim.models.Word2Vec(tokenized_sentences,
                                   size=size,
                                   window=window,
                                   min_count=min_count,
                                   workers=4)
    return model, tokenized_sentences

#--------------------------------------------------------------------------------------

def create_embeddings_from_df(df, col='description', min_count=MIN_COUNT):
    sentences = list(df[col])
    model, tokenized_sentences = create_embeddings(sentences, min_count=min_count)
    return model, tokenized_sentences
 
#--------------------------------------------------------------------------------------
# Save & Load Word Embeddings
#--------------------------------------------------------------------------------------

def save_word_embeddings(model, file=GENSIM_MODEL_FILE):
    pathname = ut.make_models_pathname(file)
    print("Saving gensim embeddings to " + pathname + " ...")
    model.save(pathname)
    return True

#---------------------------------------------------------------------------------------

def load_word_embeddings(file=GENSIM_MODEL_FILE):
    pathname = ut.make_models_pathname(file)
    print("Loading gensim embeddigns from" + pathname + " ...")
    return gensim.models.Word2Vec.load(pathname)

#---------------------------------------------------------------------------------------
# Comparing Words in Embeddings
#---------------------------------------------------------------------------------------

def compare_words (model, w1, w2, precision=2):
    try:
        return ut.round((model.wv.similarity(w1, w2) + 1) * 50, precision)
    except KeyError:
        return 0.0

#---------------------------------------------------------------------------------------

# Returns true if word is closer to w1 than to w2

def word_closer_to_p (word, model, w1, W2):
    p_score = compare_words(model, word, w1)
    n_score = compare_words(model, word, w2)
    return p_score >= n_score

#------------------------------------------------------------------------------------
# Word Vocabularies in Embeddings
#------------------------------------------------------------------------------------

def vocabulary (model):
    return model.wv.vocab    

#------------------------------------------------------------------------------------

def vocabulary_size(model):
    return len(vocabulary(model))

#------------------------------------------------------------------------------------

def vocabulary_words(model):
    vocab = vocabulary(model)
    return list(vocab.keys()) 

#------------------------------------------------------------------------------------

def word_in_vocabulary_p(word, model):
    return word in vocabulary_words (model)

#------------------------------------------------------------------------------------
# Saving Gensim Wights & Vocab Data for Use by Keras
#------------------------------------------------------------------------------------

# Adapted from: https://codekansas.github.io/blog/2016/gensim.html

def save_gensim_weights (name, gensim_model, wfile=WEIGHTS_FILE):
    name = name.lower()
    weights = gensim_model.wv.syn0
    wpath = ut.make_models_pathname(name + '_' + wfile)
    print ("Saving Gensim weights to:\n" + wpath)
    np.save(open(wpath, 'wb'), weights)

#------------------------------------------------------------------------------------
    
def save_gensim_vocab(name, gensim_model, vfile=VOCAB_FILE):
    name = name.lower()
    vocab = dict([(k, v.index) for k, v in gensim_model.wv.vocab.items()])
    vpath = ut.make_models_pathname(name + '_' + vfile)
    print ("Saving Gensim vocabulary to:\n" + vpath)
    print ("Vocabulary Size: " + str(len(vocab)))
    with open(vpath, 'w') as f:
        f.write(json.dumps(vocab))
 
#------------------------------------------------------------------------------------

# This saves the weights and vobulary of the gensim model so that they can be
# accessed by Keras.

def save_gensim_model (name, gensim_model, wfile=WEIGHTS_FILE, vfile=VOCAB_FILE):

    # Save the weights from gensim embeddings as numpy array.
    save_gensim_weights (name, gensim_model, wfile)

    # Save vocab from gensim model as json file.
    save_gensim_vocab(name, gensim_model, vfile)

#------------------------------------------------------------------------------------

# This checks to see if the gensim word embedding files exists.

def gensim_model_exists_p (name, wfile=WEIGHTS_FILE, vfile=VOCAB_FILE):
    name = name.lower()
    wpath = ut.make_models_pathname(name + '_' + wfile)
    vpath = ut.make_models_pathname(name + '_' + vfile)
    if os.path.isfile(wpath) and os.path.isfile(vpath):
        return True
    else:
        return False

#------------------------------------------------------------------------------------

# This verifies that the Gensim word embedding files exists, otherwise it generates
# them from the 'descripion' column in the data DF.

def ensure_gensim_model(category_name, data):
    model_name = ut.make_model_name (category_name)
    # Generate and save word embeddings
    if gensim_model_exists_p(model_name) == False:
        print ('Creating word embeddings from ' + str(data.shape[0]) + ' samples...')
        model, _ = create_embeddings_from_df(data)
        print ('Finished creating word embeddings.')
        save_gensim_model (model_name, model)
        return False
    else:
        return True

#************************************************************************************
# Part II: Keras Embeddings
#************************************************************************************

#------------------------------------------------------------------------------------
# Loading Gensim Generated Embdeddings Data for Use by Keras    
#------------------------------------------------------------------------------------

# Loads the weight matrix saved by save_gensim_weights
  
def load_keras_weights(name, wfile=WEIGHTS_FILE):
    name = name.lower()
    embeddings_path = ut.make_models_pathname(name + '_' + wfile)
    weights = np.load(open(embeddings_path, 'rb'))
    return weights

#------------------------------------------------------------------------------------

# Loads the vocabulary saved by save_gensim_vocab, returns two maps:
# word -> index and index -> word mappings

def load_keras_vocab(name, vfile=VOCAB_FILE):
    name = name.lower()
    vocab_path = ut.make_models_pathname(name + '_' + vfile)
    with open(vocab_path, 'r') as f:
        data = json.loads(f.read())
    word2idx = data
    idx2word = dict([(v, k) for k, v in data.items()])
    return word2idx, idx2word

#------------------------------------------------------------------------------------
# End of File
#------------------------------------------------------------------------------------