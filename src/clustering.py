#------------------------------------------------------------------------
# CLUSTERING
#------------------------------------------------------------------------

DFAULT_THRESHOLD=0.25

def jaccard_index (l1, l2):
    s1 = set(l1)
    s2 = set(l2)
    nominator = len(s1.intersection(s2))
    denominator = len(s1.union(s2))
    return nominator * 1.0 / denominator

def sr_clustering(l, threshold=DFAULT_THRESHOLD):
    clusters = []
    for elmt in l:
        None

        
#------------------------------------------------------------------------
# End of File
#------------------------------------------------------------------------
