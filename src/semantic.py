import gensim

# Google pre_traineed model:
data_file =  'c:\\Projects\\Python\\data\\GoogleNews-vectors-negative300.bin'

# Load Google's pre-trained Word2Vec model.
def load_model(data_file=data_file):
    model = gensim.models.KeyedVectors.load_word2vec_format(data_file, binary=True)  
