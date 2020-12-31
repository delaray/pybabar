import pandas as pd
from src.utils import my_round
from src.utils import make_results_pathname
from src.embeddings import create_embeddings_from_df
from src.embeddings import vocabulary_words

#------------------------------------------------------------------------------------
# generate_category_emdeddings
#------------------------------------------------------------------------------------

# This returns a dataframe of product instances with only the description and raw_data
# category classes filtered by the number of category instances.

# def load_raw_category_instances (data_file='default', min_count=50):
#     df = load_raw_data(data_file)
#     df.columns = ['description', 'class']
#     # Remove classes with less than min_count members
#     df = df[df.groupby('class')['class'].transform('count').ge(min_count)]
#     return df
    
#------------------------------------------------------------------------------------
# Jaccard Index 
#------------------------------------------------------------------------------------

# Returns the Jaccard index (i.e. the percentage over of two lists)

def jaccard_index(l1, l2):
    sl1 = set(l1)
    sl2 = set(l2)
    intersect = sl1.intersection(sl2)
    union = sl1.union(sl2)
    if len(union) == 0:
        return 0.0
    else:
        return 100 * my_round(len(intersect)/len(union))

#------------------------------------------------------------------------------------
# Categories Classes & Counts
#------------------------------------------------------------------------------------

# Returns a list of the category classes in the dataframe.

def get_category_classes (df, column='class'):
    counts = df[column].value_counts()
    categories = list(counts.index)
    categories.sort()
    return categories

#------------------------------------------------------------------------------------

def count_category_classes (df, column='class'):
    counts = df[column].value_counts()
    categories = list(counts.index)
    rows =  list(zip(categories, list(counts)))
    df = pd.DataFrame(rows, columns=['category', 'count'])
    df= df.sort_values('category', axis=0, ascending=True)
    return df

#------------------------------------------------------------------------------------

CATEGORY_COUNTS_FILE = make_results_pathname('category-counts.xlsx')

def save_category_classes(df, file=CATEGORY_COUNTS_FILE):
    df = count_category_classes(df)
    df.to_excel(file, index=False)
    return True
    
#------------------------------------------------------------------------------------
# Category Vocabularies
#------------------------------------------------------------------------------------

# This returns a dictionary of categories and corresponding vocabulary words.

# Assumes 'description' and 'class' colums.

def generate_category_vocabularies(df, category=None, model=None):

    # Process all categories or asingle one if specidied
    if category is not None:
        categories = [category]
    else:
        categories = get_category_classes (df)

    # Create a dictionary of categories and their vocabularies.
    vocabularies = {}
    for category in categories:
        cdf = df.loc[df['class']==category]
        if model is None:
            print ('Creating embeddings for category: ' + category)
            model, _ = create_embeddings_from_df(cdf)
        vocabularies.update({category : vocabulary_words(model)})
        
    return vocabularies

#------------------------------------------------------------------------------------
# Vocabulary Mmatrix
#------------------------------------------------------------------------------------

def generate_vocabulary_matrix (df, vocabularies={}):
    if vocabularies == {}:
        vocabularies = generate_category_vocabularies(df)
    ckeys = list (vocabularies.keys())
    rows = []
    for category1 in ckeys:
        row = []
        for category2 in ckeys:
            row.append(jaccard_index(vocabularies[category1], vocabularies[category2]))
        rows.append(row)
    df = pd.DataFrame(rows, columns=ckeys, index=ckeys)
    return df

#------------------------------------------------------------------------------------
# End of File
#------------------------------------------------------------------------------------
