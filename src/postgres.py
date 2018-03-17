import psycopg2
import nltk
import string
from multiprocessing import Process, Manager, freeze_support
import pandas as pd
import pprint
from functools import reduce


import clustering

#vertex_table_name = 'old_wiki_vertices'
vertex_table_name = 'wiki_vertices'

#edge_table_name = 'old_wiki_edges'
edge_table_name_prefix = 'wiki_edges_'

#------------------------------------------------------------------------------
# DB Connection
#------------------------------------------------------------------------------

def wikidb_connect():
    conn = psycopg2.connect("dbname='wikidb' user='postgres' password='postgres' host='localhost'")
    return conn

#------------------------------------------------------------------------------

def ensure_connection(conn=None):
    if conn == None:
        conn = wikidb_connect()
    return(conn)

#------------------------------------------------------------------------------
# Wiki DB Vertex Table Creationo
#------------------------------------------------------------------------------

create_vertices_str = "CREATE TABLE " + vertex_table_name + " (id serial NOT NULL, " + \
                      "name character varying, " + \
                      "weight integer DEFAULT 0, " + \
                      "CONSTRAINT vertex_id PRIMARY KEY (id));"

def create_vertices_table(conn):
    cur = conn.cursor()
    print ("Creating Wikipedia Vertices Table...")
    cur.execute("DROP TABLE IF EXISTS " + vertex_table_name + ";")
    cur.execute(create_vertices_str)

#------------------------------------------------------------------------------

def create_vertices_table_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON " + vertex_table_name + " ((lower(name)));")
    conn.commit()

#------------------------------------------------------------------------------
# Dumpings Tables as CSVs
#------------------------------------------------------------------------------
    
def save_vertex_table (pathname, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("copy (SELECT * FROM wiki_vertices) to " + "'" + pathname + "'  with csv")

#------------------------------------------------------------------------------

def save_edge_tables(directory, conn=None):
    None
    
#------------------------------------------------------------------------------
# Wiki DB Edge Tables Creation
#------------------------------------------------------------------------------

# Use both letter and digits given the number of digit based topic names.

def table_suffixes():
    return list(string.ascii_lowercase) + list(map(str, [0,1,2,3,4,5,6,7,8,9]))

    
def edge_table_name(letter):
    return edge_table_name_prefix + letter

def edge_table_names (suffixes=table_suffixes()):
    return [edge_table_name(x) for x in suffixes]

def source_name_letter (name):
    letter = name[0].lower()
    if letter in table_suffixes():
        return letter
    else:
        return 'z'
    
def create_edge_table_str (letter):
    return "CREATE TABLE " + \
        edge_table_name(letter) + \
        "(id serial NOT NULL, " + \
        "source integer NOT NULL," + \
        "target integer NOT NULL, " + \
        "type character varying, " + \
        "weight integer DEFAULT 0, " + \
        "CONSTRAINT edge_id_" + letter + " PRIMARY KEY (id));"

def create_edge_table(conn, letter):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS " + edge_table_name(letter) + ";")
    cur.execute(create_edge_table_str(letter))
    cur.execute("CREATE INDEX ON " + edge_table_name(letter) + " (source);")
    cur.execute("CREATE INDEX ON " + edge_table_name(letter) + " (target);")

        
# Create an edge table for each letter of the alphabet and digit.
# I.e. 36 tables for the edges)

def create_edge_tables(conn):
    cur = conn.cursor()
    print ("Creating Wikipedia Edge Tables...")
    for letter in table_suffixes():
        create_edge_table(conn,letter)
        conn.commit()
    
#------------------------------------------------------------------------------
# Crete WikiDb Tables
#------------------------------------------------------------------------------

def create_wiki_db_graph_tables():
    conn = wikidb_connect()
    # Vertices
    create_vertices_table(conn)
    create_vertices_table_indexes(conn)
    conn.commit()
    # Edges
    create_edge_tables(conn)
    return True

#------------------------------------------------------------------------------
# Wiki DB Vertex Table Maintenace
#------------------------------------------------------------------------------

def add_wiki_vertex(vertex_name, conn=None, commit_p=False):
    if "'" in vertex_name:
        return []
    else:
        conn = ensure_connection(conn)
        cur = conn.cursor()
        if find_wiki_vertex(vertex_name, conn) == None:
            cur.execute("INSERT INTO " + vertex_table_name + " (name) VALUES ('" + vertex_name + "');")
            if commit_p == True:
                conn.commit()

#------------------------------------------------------------------------------

def add_wiki_vertices(vertices, conn=None):
    conn = ensure_connection(conn)
    for vertex in vertices:
        add_wiki_vertex(vertex, conn, True)
    conn.commit()

#------------------------------------------------------------------------------

def find_wiki_vertex(vertex_name, conn=None):
    if "'" in vertex_name:
        return None
    else:
        conn = ensure_connection(conn)
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + vertex_table_name + " " + \
                    "WHERE LOWER(name)=LOWER('" + vertex_name + "');")
        rows = cur.fetchall()
        if rows == []:
            return None
        else:
            return rows[0][0]

#------------------------------------------------------------------------------

def vertex_name(vertex_id,  conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + vertex_table_name + " WHERE id=" + str(vertex_id) + ";")
    rows = cur.fetchall()
    return rows[0][1]

#------------------------------------------------------------------------------

def count_wiki_vertices(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM wiki_vertices")
    rows = cur.fetchall()
    return rows[0][0]

#------------------------------------------------------------------------------
# Wiki DB Edge Table Maintenace
#------------------------------------------------------------------------------

DEFAULT_EDGE_TYPE = 'related'

def add_wiki_edge(source_name, target_name, edge_type=DEFAULT_EDGE_TYPE, conn=None, commit_p=False):
    conn = ensure_connection(conn)
    letter = source_name_letter(source_name)
    edge_table = edge_table_name(letter)
    source_id = find_wiki_vertex(source_name, conn)
    target_id = find_wiki_vertex(target_name, conn)
    if source_id==None or target_id==None:
        return None
    else:
        if find_wiki_edge_by_id(edge_table, source_id, target_id, conn) == None:
            cur = conn.cursor()
            cur.execute("INSERT INTO " + edge_table + " (source, target, type) " +\
                        "VALUES (" + str(source_id) + ", " + str(target_id) + ", '" + edge_type + "');")
            if commit_p == True:
                conn.commit()

#------------------------------------------------------------------------------

def add_wiki_edges(source_name, target_names, edge_type='related', conn=None):
    conn = ensure_connection(conn)
    for target_name in target_names:
        add_wiki_edge(source_name, target_name, edge_type=edge_type, conn=conn)
    conn.commit()

#------------------------------------------------------------------------------

def find_wiki_edge_by_id(edge_table, source_id, target_id, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + edge_table + " " + \
                "WHERE source=" + str(source_id) + " AND target=" + str(target_id) + ";")
    rows = cur.fetchall()
    if rows==[]:
        return None
    else:
        return rows

#------------------------------------------------------------------------------

def find_wiki_edge(source_name, target_name, conn=None):
    conn = ensure_connection(conn)
    source_id = find_wiki_vertex(source_name, conn)
    target_id = find_wiki_vertex(target_name, conn)
    letter = source_name_letter(source_name)
    edge_table = edge_table_name(letter)
    if source_id==None or target_id==None:
        return None
    else:
        return find_wiki_edge_by_id(edge_table,source_id, target_id, conn)

#------------------------------------------------------------------------------

def find_wiki_edges(source_name, edge_type=DEFAULT_EDGE_TYPE, conn=None):
    conn = ensure_connection(conn)
    source_id = find_wiki_vertex(source_name, conn)
    letter = source_name_letter(source_name)
    edge_table = edge_table_name(letter)
    if source_id==None:
        return None
    else:
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + edge_table + " " + \
                    "WHERE source=" + str(source_id) + " AND type='" + edge_type + "';")
        rows = cur.fetchall()
        return rows

#------------------------------------------------------------------------------
# Counting Vertices and Edges
#------------------------------------------------------------------------------

# Retturn a dictionaru of table_namme and edge_count.

def count_wiki_edges_by_table(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    edge_counts = {}
    for letter in table_suffixes():
        edge_table = edge_table_name(letter)
        cur.execute("SELECT count(*) FROM " + edge_table)
        rows = cur.fetchall()
        edge_counts.update({edge_table : rows[0][0]})
    return edge_counts
    
#------------------------------------------------------------------------------

def count_wiki_edges(conn=None):
    conn = ensure_connection(conn)
    counts = count_wiki_edges_by_table (conn)
    return sum(counts.values())

#------------------------------------------------------------------------------
# Out Neighors
#------------------------------------------------------------------------------

# Returns a list of neighbor topic names that are pointed to by <topic_name>.
# These will necessarily be stored in the same table, so we only need to
# query one table.

def find_wiki_out_neighbors(topic_name, conn=None):
    conn = ensure_connection(conn)
    topic_id = find_wiki_verte
    cur = conn.cursor()
    if topic_id == None:
        return []
    else:
        letter = source_name_letter(topic_name)
        edge_table = edge_table_name(letter)
        cur.execute("SELECT * FROM " + edge_table + " as we " + \
                    "JOIN " + vertex_table_name + " as wv on we.target = wv.id " + \
                    "WHERE source=" + str(topic_id) + ";")
        rows = cur.fetchall()
        return list(set([row[6] for row in rows]))
 
#------------------------------------------------------------------------------
# In Neighbors
#------------------------------------------------------------------------------

# Returns a list of neighbor topic names that point to <topic_name>. These will
# necessarily be scattered across several tables, so we need to query each of
# them.
 
def find_wiki_in_neighbors(topic_name, tables=None, conn=None):
    conn = ensure_connection(conn)
    topic_id = find_wiki_vertex(topic_name, conn)
    if topic_id == None:
        return []
    else:
        if tables == None:
            tables = edge_table_names()
        cur = conn.cursor()
        all_rows = []
        for edge_table_name in tables:
            cur.execute("SELECT * FROM " + edge_table_name + " as we " + \
                        "JOIN " + vertex_table_name + " as wv on we.source = wv.id " + \
                        "WHERE target=" + str(topic_id) + ";")
            rows = cur.fetchall()
            all_rows += rows
        return list(set([row[6] for row in all_rows]))

#------------------------------------------------------------------------------

# PARALLEL Version

def split_list (a, n):
    k, m = divmod(len(a), n)
    return list (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

#------------------------------------------------------------------------------

# def pdm_worker (tables, procnum, return_dict):
#     results = find_wiki_in_neighbors (tables)
#     return_dict[procnum] = results

#------------------------------------------------------------------------------

def pfind_wiki_in_neighbors(topic_name, conn=None):
    conn = ensure_connection(conn)
    topic_id = find_wiki_vertex(topic_name, conn)
    if topic_id == None:
        return []

    l1, l2, l3, l4 = split_list(edge_table_names(), 4)

    # Use a shared variable to collect results fromm each process
    manager = Manager()
    return_dict = manager.dict()

    # Run four jobs, one for each quadrant of the matrix.
    jobs = [] 
    freeze_support()
    p1 = Process(target=clustering.pdm_worker1, args=(topic_name, l1, 1, return_dict))
    jobs.append(p1)
    p1.start()
    p2 = Process(target=clustering.pdm_worker1, args=(topic_name, l2, 2, return_dict))
    jobs.append(p2)
    p2.start()
    p3 = Process(target=clustering.pdm_worker1, args=(topic_name, l3, 3, return_dict))
    jobs.append(p3)
    p3.start()
    p4 = Process(target=clustering.pdm_worker1, args=(topic_name, l4, 4, return_dict))
    jobs.append(p4)
    p4.start()

    # Gather the results
    for proc in jobs:
        proc.join()

    # Join all the neighbors
    neighbors = return_dict.values()
    
    # Return the distance matrix
    return reduce(lambda a, b : a + b, neighbors)

#------------------------------------------------------------------------------
# Strongly Related Topics
#------------------------------------------------------------------------------

# Find topics that are mutually linkeed to topic_name.

def find_strongly_related_topics (topic_name, conn=None):
    conn = ensure_connection(conn)
    in_vertices = find_wiki_in_neighbors(topic_name, conn)
    out_vertices = find_wiki_out_neighbors(topic_name, conn)
    return list(set(in_vertices) & set(out_vertices))

#------------------------------------------------------------------------------
# SUBTOPICS
#------------------------------------------------------------------------------

# Find _potential subtopics

def find_potential_subtopics (topic_name, conn=None):
    conn = ensure_connection(conn)
    result = []
    in_vertices =  set(find_wiki_in_neighbors(topic_name, conn))
    out_vertices = set(find_wiki_out_neighbors(topic_name, conn))
    vertices = list(in_vertices.union(out_vertices))
    for x in vertices:
        if topic_name.lower() in x.lower():
            result.append(x)
    return result

#------------------------------------------------------------------------------

def compute_wiki_subtopics(topic_name, conn=None):
    conn = ensure_connection(conn)
    topics = find_potential_subtopics(topic_name, conn)
    subtopics = []
    for topic1 in topics:
        topic2 = topic1.replace('_', ' ')
        tokens = nltk.word_tokenize(topic2)
        if tokens[-1].lower() == topic_name.lower():
            subtopics.append(topic1)
    return subtopics

#------------------------------------------------------------------------------

# This computes the subtopics of topic_name and adds subtopic and supertopic
#  edges to the graph.

def add_wiki_subtopics(topic_name, conn=None):
    conn = ensure_connection(conn)
    subtopics = compute_wiki_subtopics (topic_name, conn=None)
    for subtopic in subtopics:
        add_wiki_edge(subtopic, topic_name, edge_type='subtopic', conn=conn)
        add_wiki_edge(topic_name, subtopic, edge_type='supertopic', conn=conn)
    conn.commit()

#------------------------------------------------------------------------------

def subtopic_p (topic1, topic2):
    topic =  topic2.replace('_', ' ')
    tokens = nltk.word_tokenize(topic)
    subtopicp = True
    for t in tokens:
        if not t.lower() in topic1.lower():
            subtopicp = False
    return subtopicp

#------------------------------------------------------------------------------
# Root Topics
#------------------------------------------------------------------------------

# Current finds all topic names that do not contain an underscore. This will be 
# be used to seed the different subtopic hierarchies.

def find_wiki_root_topics(conn=None):
    conn = ensure_connection(conn) 
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + vertex_table_name + " as wv " + \
                "WHERE wv.name NOT SIMILAR TO '%\_%'")
    rows = cur.fetchall()
    return [row[1] for row in rows]


#------------------------------------------------------------------------------

# def generate_comparator (conn=None):
#     def cfn (topic1, topic2):
#         c = ensure_connection(conn)
#         return compare_topics (topic1, topic2, c)
#     return cfn

#------------------------------------------------------------------------------

# Note: <topics> is a list of topic names.

# def compute_topics_distance_matrix (topics, conn=None):
#     cfn = generate_comparator(conn)
#     dm = generate_distance_matrix (topics, topics, cfn)
#     return dm
    
#------------------------------------------------------------------------------
# Run Time
#------------------------------------------------------------------------------

# create_wiki_db_graph_tables()

#------------------------------------------------------------------------------
# End of File
#-----------------------------------------------------------------------------
