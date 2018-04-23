# Python multithreading for whatever reason seems to require that the workers be in
# a different module.

import postgres
import clustering

#------------------------------------------------------------------------------

def neighbor_worker (topic, tables, procnum, return_dict):
    results = postgres.find_wiki_in_neighbors (topic, tables)
    return_dict[procnum] = results

#------------------------------------------------------------------------------

# This used the dsitance matrix

def pdm_worker (l1, l2, procnum, return_dict):
    conn = ensure_connection()
    m = clustering.generate_distance_matrix (l1, l2, conn)
    return_dict[procnum] = m

#------------------------------------------------------------------------------
# End of File
#------------------------------------------------------------------------------
