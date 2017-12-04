#------------------------------------------------------------------------
# CLUSTERING
#------------------------------------------------------------------------

import multiprocessing

DFAULT_THRESHOLD=0.25

#------------------------------------------------------------------------
# Jaccard Index
#------------------------------------------------------------------------

def jaccard_index (l1, l2):
    if len(l1)==0 or len(l2)==0:
        return 0
    else:
        s1 = set(l1)
        s2 = set(l2)
        nominator = len(s1.intersection(s2))
        denominator = len(s1.union(s2))
        return nominator * 1.0 / denominator

#------------------------------------------------------------------------

def get_jaccard_distance (e1, e2, matrix, indices):
    return matrix[indices[e1]][indices[e2]]

#------------------------------------------------------------------------
# Generate Distance Matrix 
#------------------------------------------------------------------------

def generate_distance_matrix (topics, comparator):
    matrix = []
    for topic1 in topics:
        row=[]
        for topic2 in topics:
            row.append(comparator(topic1, topic2))
        matrix.append(row)
    return matrix

#------------------------------------------------------------------------
# SR Clustering
#------------------------------------------------------------------------

def sr_clustering(l, threshold=DFAULT_THRESHOLD):
    clusters = []
    for elmt in l:
        None

def foo (a):
    def bar(b):
        return a + b
    return bar
        
#------------------------------------------------------------------------
# End of File
#------------------------------------------------------------------------
