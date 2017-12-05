#------------------------------------------------------------------------
# CLUSTERING
#------------------------------------------------------------------------

from multiprocessing import Process, Manager, freeze_support
import pandas as pd
import postgres

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
# Generate Distance Matrix (serial version)
#------------------------------------------------------------------------

# TODO: Use an object to keep the matri and indices together

def generate_distance_matrix (topics1, topics2, conn=None):
    conn = postgres.ensure_connection(conn)
    # Populate the matrix
    matrix = []
    for topic1 in topics1:
        row=[]
        for topic2 in topics2:
            row.append(postgres.compare_topics(topic1, topic2, conn))
        matrix.append(row)
    # Create and return a DataFrame
    df = pd.DataFrame(matrix, index=topics1, columns=topics2)
    return df

# def verify_distance_matrix(m):
#     valid = True
#     for i in range(len(m)):
#         for j in range(len(m)):
#             if i==j and not m[i][j]==1:
#                 valid = False
#     return valid
    
#------------------------------------------------------------------------
# Generate Distance Matrix (parallel version)
#------------------------------------------------------------------------

def reassemble_matrix(quadrants):
    q1 = quadrants[0]
    q2 = quadrants[1]
    q3 = quadrants[2]
    q4 = quadrants[3]
    c1 = pd.concat([q1, q2], axis=0)
    c2 = pd.concat([q3, q4], axis=0)
    dm = pd.concat([c1, c2], axis=1)
    print(dm)
    return dm
 
#------------------------------------------------------------------------

# Process four quadrants of the matrix in parallel.

def pgenerate_distance_matrix (topics):

    # Partition topics
    midpoint = round(len(topics) / 2)
    l1 = topics[:midpoint]
    l2 = topics[midpoint:]

    # Use a shared variable to collect results fromm each process
    manager = Manager()
    return_dict = manager.dict()

    # Run four jobs, one for each quadrant of the matrix.
    jobs = [] 
    freeze_support()
    p1 = Process(target=postgres.pdm_worker, args=(l1, l1, 1, return_dict))
    jobs.append(p1)
    p1.start()
    p2 = Process(target=postgres.pdm_worker, args=(l1, l2, 2, return_dict))
    jobs.append(p2)
    p2.start()
    p3 = Process(target=postgres.pdm_worker, args=(l2, l1, 3, return_dict))
    jobs.append(p3)
    p3.start()
    p4 = Process(target=postgres.pdm_worker, args=(l2, l2, 4, return_dict))
    jobs.append(p4)
    p4.start()

    # Gather the results
    for proc in jobs:
        proc.join()

    # Reassemble the matrix...
    quadrants = return_dict.values()
    dm = reassemble_matrix (quadrants)
    
    # Return the distance matrix
    return dm

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