#*****************************************************************************
# WIKIDB POSTGRES DB INTERFACE
#*****************************************************************************
#
# Part 0: Generic Functions
# Part 1: Creation operations
# Part 2: Retrieval operations
# Part 3: Status Operations
# Part 4: Maintenance Operations
# Part 5: Deletion operations
# Part 6: Vertex and Edge Types
# Part 7: Root Subtopics Tables
# Part 8: Lexicon Tables
#
#
#*****************************************************************************
# DUMPING AND RESTORING THE DATABASE
#-----------------------------------------------------------------------------

# Dump PG Database
# pg_dump -U postgres wikidb > wikidb.sql

#------------------------------------------------------------------------------
#
# Restore PG Database
# sudo -u postgres psql
#
# First need to create the database
# CREATE DATABASE wikidb
#
# Now restore the dumpled sql file
#
# 1. In PSQL 
# \i /home/pierre/wikidb.sql
#
# 2. At the command line
# pg_restore -U postgres -d wikidb.sql
#
#*****************************************************************************


import sys
import psycopg2
import string
import pprint
import pandas as pd
from datetime import datetime
from  urllib.parse import unquote

from functools import reduce
from multiprocessing import Process, Manager, freeze_support

# Project Imports
import src.processes as pr

#******************************************************************************
# Part 0: DB Connection and generic functions
#******************************************************************************

LHOST = 'localhost'
RHOST = '35.189.76.240'

#------------------------------------------------------------------------------
# DB Connection
#------------------------------------------------------------------------------

# TODO: Move connection parameters into environment variables.

# First try to connect to remote host, otherwise connect to localhost.

# def wikidb_connect():
#     try:
#         conn = psycopg2.connect("dbname='wikidb' user='postgres'" + \
#                                 " password='Terrapin1' host='" + RHOST + "'")
#     except Exception:
#         conn = psycopg2.connect("dbname='wikidb' user='postgres'" + \
#                                 " password='Terrapin1' host='" + LHOST + "'")
#     return conn

def wikidb_connect():
    conn = psycopg2.connect("dbname='wikidb' user='postgres'" + \
                            " password='Terrapin1' host='" + LHOST + "'")
    return conn

#------------------------------------------------------------------------------

def ensure_connection(conn=None):
    if conn == None:
        conn = wikidb_connect()
    return(conn)

#------------------------------------------------------------------------------

def execute_query(query, data=(), conn=None):
    conn = conn if conn != None else ensure_connection()
    cur = conn.cursor()
    cur.execute(query, data)
    conn.commit()

# -----------------------------------------------------------------------------

def run_query(query, data=(), conn=None):
    conn = conn if conn != None else ensure_connection()
    cur = conn.cursor()
    cur.execute(query, data)
    rows = cur.fetchall()
    return rows

# -----------------------------------------------------------------------------

def get_table_columns(table):
    conn = ensure_connection()
    cur = conn.cursor()
    query = "SELECT * FROM information_schema.columns " + \
             "WHERE table_name = '" + table + "';"
    cur.execute(query)
    rows = cur.fetchall()
    rows = [x[3] for x in rows]
    return rows
    
# -----------------------------------------------------------------------------
# Count Table Rows
# -----------------------------------------------------------------------------

# This returns as exact count (slow)

def count_table_rows(table_name,conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM " + table_name + ";")
    rows = cur.fetchall()
    return rows[0][0]

#------------------------------------------------------------------------------

# This retuns a close estimate (extremely fast):

def estimate_table_rows(table,conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    query = "SELECT reltuples::bigint AS estimate FROM pg_class where relname=" + table + ";"
    cur.execute(query)
    rows = cur.fetchall()
    return rows[0][0]

#------------------------------------------------------------------------------
# CSV File Row to DB Row String
#------------------------------------------------------------------------------

# This returns a string suitable for insert query with strings escaped.

def row_to_str (row):
    row_str = ""
    for elmt in row:
        if type(elmt) == str:
            row_str += "'" + elmt + "',"
        else:
            row_str += str(elmt) + ","
    return "(" + row_str[:-1] + ")"

#------------------------------------------------------------------------------

# Splits the list <a> into <n> even pieces.

def split_list (a, n):
    k, m = divmod(len(a), n)
    return list (a[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n))

#*****************************************************************************
# Part 1: Table and Index Creation: Vertices, Edges and Root Vertices
#*****************************************************************************

#------------------------------------------------------------------------------
# Table Names
#------------------------------------------------------------------------------

VERTICES_TABLE = 'wiki_vertices'
ROOT_TOPICS_TABLE='root_topics'
edge_table_prefix = 'wiki_edges_'

#------------------------------------------------------------------------------

# There are about 50,000,000 edges and we divide them amongst 37 tabbles based
# the first letter of te edge's source vertex name.

# Use both letters and digits given the number of digit based topic names.

# TODO: Convert to constant.

def edge_tables_suffixes():
    return list(string.ascii_lowercase) + list(map(str, [0,1,2,3,4,5,6,7,8,9]))

#------------------------------------------------------------------------------

def source_name_letter (name):
    letter = name[0].lower()
    if letter in edge_tables_suffixes():
        return letter
    else:
        return 'z'

#------------------------------------------------------------------------------

DEFAULT_EDGE_TYPE = 'related'

#------------------------------------------------------------------------------
# Vertex Table Creation
#------------------------------------------------------------------------------

create_vertices_str = "CREATE TABLE " + VERTICES_TABLE + " (id serial NOT NULL, " + \
                      "name character varying, " + \
                      "weight integer DEFAULT 0, " + \
                      "CONSTRAINT vertex_id PRIMARY KEY (id));"

def create_vertices_table(conn):
    cur = conn.cursor()
    print ("Creating Wikipedia Vertices Table...")
    cur.execute("DROP TABLE IF EXISTS " + VERTICES_TABLE + ";")
    cur.execute(create_vertices_str)

#------------------------------------------------------------------------------

def create_vertices_table_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON " + VERTICES_TABLE + " ((lower(name)));")
    conn.commit()

#------------------------------------------------------------------------------
# Wiki DB Root Vertices Table Creation
#------------------------------------------------------------------------------

# NB: The priimary key will be a vertex id from the vertex table.

root_vertices_str = "CREATE TABLE " + ROOT_TOPICS_TABLE + \
                    "(id integer NOT NULL, " + \
                      "name character varying, " + \
                      "weight integer DEFAULT 0, " + \
                      "indegree integer DEFAULT 0, " + \
                      "outdegree integer DEFAULT 0, " + \
                      "CONSTRAINT root_id PRIMARY KEY (id));"

#------------------------------------------------------------------------------

def create_root_vertices_table(conn):
    cur = conn.cursor()
    print ("Creating Wikipedia Vertices Table...")
    cur.execute("DROP TABLE IF EXISTS " + ROOT_TOPICS_TABLE + ";")
    cur.execute(root_vertices_str)
    conn.commit()

#------------------------------------------------------------------------------

def create_root_vertices_table_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON " + ROOT_TOPICS_TABLE + " ((lower(name)));")
    conn.commit()

#------------------------------------------------------------------------------
    
def create_root_vertices_tables(conn=None):
    conn = ensure_connection(conn)
    create_root_vertices_table(conn)
    create_root_vertices_table_indexes(conn)

#------------------------------------------------------------------------------
# Add Root Vertex
#------------------------------------------------------------------------------

root_fields = "(id, name, weight, outdegree)"
    
def add_root_vertex(row, conn=None, commit=False):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    query = "INSERT INTO " + ROOT_TOPICS_TABLE + " " + root_fields + \
           " VALUES " + row_to_str(row) + ";"
    try:
        cur.execute(query)
        if commit == True:
            conn.commit()
    except Exception as err:
        print ("Error: " + str(err))
       
#------------------------------------------------------------------------------
# Count Root Vertices
#------------------------------------------------------------------------------

def count_root_vertices(conn=None):
    return count_table_rows(ROOT_TOPICS_TABLE)

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

def edge_table_name(letter):
    if letter in edge_tables_suffixes():
        return edge_table_prefix + letter
    else:
        return edge_table_prefix + '0'

#------------------------------------------------------------------------------

def edge_tables (suffixes=edge_tables_suffixes()):
    return [edge_table_name(x) for x in suffixes]

#------------------------------------------------------------------------------

def create_edge_table_str (letter):
    return "CREATE TABLE " + \
        edge_table_name(letter) + \
        "(id serial NOT NULL, " + \
        "source integer NOT NULL," + \
        "target integer NOT NULL, " + \
        "type character varying, " + \
        "weight integer DEFAULT 0, " + \
        "CONSTRAINT edge_id_" + letter + " PRIMARY KEY (id));"

#------------------------------------------------------------------------------

def create_edge_table(conn, letter):
    cur = conn.cursor()
    cur.execute("DROP TABLE IF EXISTS " + edge_table_name(letter) + ";")
    cur.execute(create_edge_table_str(letter))
    cur.execute("CREATE INDEX ON " + edge_table_name(letter) + " (source);")
    cur.execute("CREATE INDEX ON " + edge_table_name(letter) + " (target);")

#------------------------------------------------------------------------------

# Create an edge table for each letter of the alphabet and digit.
# This is a total of 36 tables for the edges. While this may impede edge
# retrieval, these tables reprsent the raw graph and it anticipateed
# that the system will operate on other graphs built from this raw
# snapshot of the wikipedia pages graph.

def create_edge_tables(conn):
    cur = conn.cursor()
    print ("Creating Wikipedia Edge Tables...")
    for letter in edge_tables_suffixes():
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
    # Root Verticees
    create_root_vertices()
    # Edges
    create_edge_tables(conn)
    return True


#******************************************************************************
# Part 2: Retrieval operations
#******************************************************************************

# NB: The GET functions take an id, the FIND functions take a name or pattern.

#------------------------------------------------------------------------------
# Vertex Table Retrieval Operations
#------------------------------------------------------------------------------

def get_wiki_vertex(vertex_id, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + VERTICES_TABLE + " WHERE id=" + str(vertex_id) + ";")
    rows = cur.fetchall()
    return rows[0] if rows != [] else None

#------------------------------------------------------------------------------

def find_topic(vertex_name, conn=None):
    if "'" in vertex_name:
        return None
    else:
        conn = ensure_connection(conn)
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + VERTICES_TABLE + " " + \
                    "WHERE LOWER(name)=LOWER('" + vertex_name + "');")
        rows = cur.fetchall()
        return rows[0] if rows != [] else None

#------------------------------------------------------------------------------
    
def find_topic_id(topic_name, conn=None):
    topic = find_topic(topic_name)
    if topic is not None:
        return topic[0]
    else:
        return None
    
#------------------------------------------------------------------------------

def vertex_id_name(vertex_id,  conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + VERTICES_TABLE + " WHERE id=" + str(vertex_id) + ";")
    rows = cur.fetchall()
    return rows[0][1]

#------------------------------------------------------------------------------

def vertex_row_name(vertex_row):
    return vertex_row[1]

#------------------------------------------------------------------------------
# Find Topics
#------------------------------------------------------------------------------

# Returns a list of vertex names

def find_topics(vertex_pattern, conn=None):
    if "'" in vertex_pattern:
        return None
    else:
        conn = ensure_connection(conn)
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + VERTICES_TABLE + " " + \
                    "WHERE LOWER(name) like LOWER('" + vertex_pattern + "');")
        rows = cur.fetchall()
        return  rows

#------------------------------------------------------------------------------

def find_all_topics ():
    conn = ensure_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + VERTICES_TABLE + ";")
    rows = cur.fetchall()
    return  rows

#------------------------------------------------------------------------------
    
def find_topic_names(vertex_pattern, conn=None):
    rows = find_topics(vertex_pattern, conn)
    return [row[1] for row in rows] if rows != [] else []

#------------------------------------------------------------------------------
#  Identifying Root Topics
#------------------------------------------------------------------------------

def root_vertex__p (vertex_id):
    vertex_row = find_topic(vertex_id)
    if vertex_name is not None:
        return root_vertex_name_p (vertex_row_name(vertex_row))
    else:
        return False
    
#------------------------------------------------------------------------------

def root_vertex_name_p (vertex_name):
    return '_' not in vertex_name and '#' not in vertex_name

#------------------------------------------------------------------------------

# Identifies root vertices in  vertices table for specified pattern

def identify_root_vertices (pattern, conn=None):
    conn = ensure_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + VERTICES_TABLE + " as wv " + \
                "WHERE wv.name NOT SIMILAR TO '%\_%' " + \
                "AND wv.name NOT SIMILAR TO '%\#%'" + \
                "AND LOWER(wv.name) like LOWER('" + pattern + "');")
    rows = cur.fetchall()
    return rows
    
#------------------------------------------------------------------------------
# Root Vertex Table Retrieval Operations
#------------------------------------------------------------------------------

def get_root_vertices(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + ROOT_TOPICS_TABLE + ";")
    rows = cur.fetchall()
    return rows

#------------------------------------------------------------------------------

def find_root_topic(vertex_name, conn=None):
    if "'" in vertex_name:
        return None
    else:
        conn = ensure_connection(conn)
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + ROOT_TOPICS_TABLE + " " + \
                    "WHERE LOWER(name)=LOWER('" + vertex_name + "');")
        rows = cur.fetchall()
        return rows[0] if rows != [] else None

#------------------------------------------------------------------------------
# Find Root Topic
#------------------------------------------------------------------------------

def find_root_topic_by_name(topic_name):
    conn = ensure_connection()
    cur = conn.cursor()
    query = "SELECT * FROM " + ROOT_TOPICS_TABLE  + \
            " WHERE LOWER(name)=LOWER('" + topic_name + "');"
    cur.execute(query)
    rows = cur.fetchall()
    if rows != []:
        return rows[0]
    else:
        return None

#------------------------------------------------------------------------------

def find_related_root_topics (vertex_name):
    related_topics = find_topic_out_neighbors(vertex_name)
    return [topic for topic in related_topics if root_vertex_name_p(topic)]

#------------------------------------------------------------------------------
# Edge Table Retrieval Operations
#------------------------------------------------------------------------------

def find_edge_by_id(edge_table, source_id, target_id, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + edge_table + " " + \
                "WHERE source=" + str(source_id) + " AND target=" + str(target_id) + ";")
    rows = cur.fetchall()
    if rows==[]:
        return None
    else:
        return rows[0]

#------------------------------------------------------------------------------

def find_edge(source_name, target_name, conn=None):
    conn = ensure_connection(conn)
    source_id = find_topic_id(source_name, conn)
    target_id = find_topic_id(target_name, conn)
    letter = source_name_letter(source_name)
    edge_table = edge_table_name(letter)
    if source_id==None or target_id==None:
        return None
    else:
        return find_edge_by_id(edge_table,source_id, target_id, conn)

#------------------------------------------------------------------------------

# Source can be a string or integer.  Return an integer source id.

def ensure_source_id(source, conn=None):
    if type(source) == str:
        return find_topic_id(source, conn)
    else:
        return source

#------------------------------------------------------------------------------

# NB: <source> can be an id or a name.

def find_edges(source_name, edge_type=DEFAULT_EDGE_TYPE, conn=None):
    conn = ensure_connection(conn)
    source_id = ensure_source_id(source_name, conn)
    #source_id = source_id[0] if source_id is not None else source_id
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
# Topic Out Neighbors
#------------------------------------------------------------------------------

# Returns a list of neighbor topic names that are pointed to by <topic_name>.
# These will necessarily be stored in the same table, so we only need to
# query one table.

def find_topic_out_neighbors(topic_name, conn=None):
    conn = ensure_connection(conn)
    topic_id = find_topic_id(topic_name, conn)
    cur = conn.cursor()
    if topic_id == None:
        return []
    else:
        letter = source_name_letter(topic_name)
        edge_table = edge_table_name(letter)
        cur.execute("SELECT * FROM " + edge_table + " as we " + \
                    "JOIN " + VERTICES_TABLE + " as wv on we.target = wv.id " + \
                    "WHERE source=" + str(topic_id) + ";")
        rows = cur.fetchall()
        return list(set([row[6] for row in rows]))

#------------------------------------------------------------------------------
# Topic In Neighbors
#------------------------------------------------------------------------------

# Returns a list of neighbor topic names that point to <topic_name>. These will
# necessarily be scattered across several tables, so we need to query each of
# them.
 
def _find_topic_in_neighbors(topic_name, tables=None, conn=None):
    conn = ensure_connection(conn)
    topic_id = find_topic_id(topic_name, conn)
    if topic_id == None:
        return []
    else:
        if tables == None:
            tables = edge_tables()
        cur = conn.cursor()
        all_rows = []
        for edge_table in tables:
            cur.execute("SELECT * FROM " + edge_table + " as we " + \
                        "JOIN " + VERTICES_TABLE + " as wv on we.source = wv.id " + \
                        "WHERE target=" + str(topic_id) + ";")
            rows = cur.fetchall()
            all_rows += rows
        return list(set([row[6] for row in all_rows]))

#------------------------------------------------------------------------------
# Topic In Neigbors (PARALLEL Version)
#------------------------------------------------------------------------------

def find_topic_in_neighbors(topic_name, conn=None, threads=8):
    conn = ensure_connection(conn)
    topic_id = find_topic_id(topic_name, conn)
    if topic_id == None:
        return []

    lists = split_list(edge_tables(), threads)

    # Use a shared variable to collect results fromm each process
    manager = Manager()
    return_dict = manager.dict()

    # Run four jobs, one for each quadrant of the matrix.
    jobs = [] 
    freeze_support()
    for index, piece in enumerate(lists):
        p = Process(target=pr.neighbor_worker,
                    args=(topic_name, piece, index, return_dict))
        jobs.append(p)
        p.start()

    # Gather the results
    for proc in jobs:
        proc.join()

    # Join all the neighbors
    neighbors = return_dict.values()
    if neighbors == []:
        return []
    else:
        return reduce(lambda a, b : a + b, neighbors)

#------------------------------------------------------------------------------
# Compute Topic Outdegree
#------------------------------------------------------------------------------

def compute_topic_outdegree(source_name, conn=None):
    conn = ensure_connection(conn)
    id = ensure_source_id(source_name, conn=conn)
    if id is None:
        return None
    else:
        letter = source_name_letter(source_name)
        table = edge_table_name(letter)
        cur = conn.cursor()
        query = "SELECT count(*) FROM " + table + " WHERE source=" + str(id) + ";"
        cur.execute(query)                    
        rows = cur.fetchall()
        return rows[0][0] if rows != [] else 0

#------------------------------------------------------------------------------
# Compute Topic Indegree
#------------------------------------------------------------------------------

def compute_topic_indegree(topic_name, source_id=None, conn=None):
    return len (find_topic_in_neighbors(topic_name))

#------------------------------------------------------------------------------
# Update Topic Indegree and Outdegree
#------------------------------------------------------------------------------

# This computes and stores the indegree, outdegree and weight of all
# the topics in the VERTICES_TABLE.

def update_topics_degrees():
    conn = ensure_connection()
    cur = conn.cursor()
    rows = find_all_topics()
    count = 0
    try:
        for row in rows:
            id = row[0]
            name = row[1]
            if row[5] is None or row[5]==0:
                try:
                    indegree = compute_topic_indegree(name)
                    outdegree = compute_topic_outdegree(name)
                    subtopics = count_topic_subtopics(name)
                    weight = indegree + outdegree + subtopics
                    query = "UPDATE " + VERTICES_TABLE + " SET " + \
                        "indegree = " + str(indegree) + ", " + \
                        "outdegree = " + str(outdegree) + ", " + \
                        "weight = " + str(weight) + " " + \
                        "WHERE id=" + str(id) + ";"
                    cur.execute(query)
                    count += 1
                    if count%200==0:
                        print ('Topics updated: ' + str(count))
                    conn.commit()
                except Exception as err:
                    print ("Error: " + str(err))
        conn.close()
        return True
    except Exception as err:
        print ("Error: " + str(err))
        conn.close()
        return False

#------------------------------------------------------------------------------

def count_processed_degrees():
    conn = ensure_connection()
    cur = conn.cursor()
    rows = find_all_topics()
    x = list(filter(lambda row: row[5] is not None, rows))
    return len(x)

#******************************************************************************
# Part 3: Status Operations
#******************************************************************************

def count_topics(conn=None):
    return count_table_rows('wiki_vertices', conn)

#------------------------------------------------------------------------------

def count_root_topics(conn=None):
    return count_table_rows('wiki_root_vertices', conn)

#------------------------------------------------------------------------------

# vertex can be a vertex_id or a vertex_name.

def count_vertex_out_neighbors (vertex, conn=None):
    return len(find_edges(vertex, conn=conn))

#------------------------------------------------------------------------------

# Retturn a dictionary of table_namme and edge_count.

def count_wiki_edges_by_table(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    edge_counts = {}
    for letter in edge_tables_suffixes():
        edge_table = edge_table_name(letter)
        count = count_table_rows(edge_table, conn=conn)
        edge_counts.update({edge_table : count})
    return edge_counts
    
#------------------------------------------------------------------------------

def count_wiki_edges(conn=None):
    conn = ensure_connection(conn)
    counts = count_wiki_edges_by_table (conn)
    return sum(counts.values())

#******************************************************************************
# Part 4: Maintenance Operations
#******************************************************************************

#------------------------------------------------------------------------------
# Vertex Table INSERT operations
#------------------------------------------------------------------------------

def add_wiki_vertex(vertex_name, conn=None, commit_p=False):
    if "'" in vertex_name:
        return []
    else:
        conn = ensure_connection(conn)
        cur = conn.cursor()
        if find_topic_id(vertex_name, conn) == None:
            cur.execute("INSERT INTO " + VERTICES_TABLE + \
                        " (name) VALUES ('" + vertex_name + "');")
            if commit_p == True:
                conn.commit()

#------------------------------------------------------------------------------

def add_wiki_vertices(vertices, conn=None):
    conn = ensure_connection(conn)
    for vertex in vertices:
        add_wiki_vertex(vertex, conn, True)
    conn.commit()

#------------------------------------------------------------------------------

def update_vertex_weight(vertex_id, conn=None, commit=False):
    conn = ensure_connection(conn)
    weight = count_vertex_out_neighbors (vertex_name, conn=conn)
    cur = conn.cursor()
    try:
        cur.execute("UPDATE " + VERTICES_TABLE + " SET weight = " + str(weight) + \
                    "WHERE id=" + str(vertex_id) + ";")
        if commit==True:
            conn.commit()
        return weight
    except Exception as err:
        print ("Failed to update vertex weight for vertex: " + str(vertex_id))
        return None

#------------------------------------------------------------------------------

def update_all_vertex_weights ():
    count = 0
    for symbol in edge_tables_suffixes():
        rows = find_topic_names(symbol + '%')
        print ("Processinng " + str(len(vertices)) + " vertices starting with " +\
               symbol + "...")
        conn = ensure_connection()
        for row in rows:
            if count%10000==0:
                print ("Processed " + str(count) + " vertices.")
            vertex_id = row[0]
            update_vertex_weight (vertex_id, conn=conn, commit=False)
            count += 1
        conn.commit()

#------------------------------------------------------------------------------
# Root Vertex Table INSERT operations
#------------------------------------------------------------------------------

# A little of a hack too ensure the root vertices table has all it's weights.

def ensure_root_vertex_weight(row):
    id = row[0]
    weight = row[2]
    #outdegree = row[3]
    if weight == 0:
        weight = count_vertex_out_neighbors(row[1])
    #if outdegree == 0:
    #    outdegree = weight
    return [id, row[1], weight, weight]


#------------------------------------------------------------------------------

# This identified the root verticees in the global vertex table that match
# pattern and add them to the root vertex table

def generate_root_vertices_for_prefix (pattern):
    try:
        conn = ensure_connection()
        cur = conn.cursor()
        rows = identify_root_vertices(pattern, conn)
        print ("Found " + str(len(rows)) + " vertices matching " + pattern)
        count = 0
        for  row in rows:
            vertex_id = row[0]
            count += 1
            if count%1000==0:
                print ("Added " + str(count) + " new root vertices...")
            row = ensure_root_vertex_weight(row)
            add_root_vertex(row, conn=conn)
    except Exception as err:
        print (err)
    conn.commit()
    conn.close()

#------------------------------------------------------------------------------

# This completely populates the root vertices table

def generate_root_vertices():
    vertex_prefixes = edge_tables_suffixes()
    for prefix in vertex_prefixes:
        print ("\nProcessing prefix " + prefix)
        generate_root_vertices_for_prefix(prefix+"%")

#------------------------------------------------------------------------------
# Wiki DB Edge Table Maintenace
#------------------------------------------------------------------------------

def add_wiki_edge(source_name, target_name, edge_type=DEFAULT_EDGE_TYPE,
                  conn=None, commit_p=False):
    conn = ensure_connection(conn)
    letter = source_name_letter(source_name)
    edge_table = edge_table_name(letter)
    source_id = find_topic_id(source_name, conn)
    target_id = find_topic_id(target_name, conn)
    if source_id==None or target_id==None:
        return None
    else:
        if find_edge_by_id(edge_table, source_id, target_id, conn) == None:
            cur = conn.cursor()
            cur.execute("INSERT INTO " + edge_table + " (source, target, type) " +\
                        "VALUES (" + str(source_id) + ", " + str(target_id) + ", '" +\
                        edge_type + "');")
            if commit_p == True:
                conn.commit()

#------------------------------------------------------------------------------

def add_wiki_edges(source_name, target_names, edge_type='related', conn=None):
    conn = ensure_connection(conn)
    for target_name in target_names:
        add_wiki_edge(source_name, target_name, edge_type=edge_type, conn=conn)
    conn.commit()

#*****************************************************************************
# Part 6: Vertex and Edge Types
#*****************************************************************************

def strongly_related_p (topic1, topic2):
    edge1 = find_edge(topic1, topic2)
    edge2 = find_edge(topic2, topic1)
    if edge1 is not None and edge2 is not None:
        return True
    else:
        return False

#------------------------------------------------------------------------------

def compute_strongly_related_neighbors(topic1):
    topics = find_topic_out_neighbors(topic1)
    results = []
    for x in topics:
        if strongly_related_p(topic1, x):
            print(x)
            results.append(x)
    return results

    
#------------------------------------------------------------------------------

def update_root_edge_types():
    conn = ensure_connection()
    cur = conn.cursor()
    
    # Get the root vertices
    cur.execute("SELECT id, name FROM " + ROOT_TOPICS_TABLE + ";")
    rows = cur.fetchall()
    print ('\nTotal root vertices: ' + str(count_root_vertices()))

    # Update those vertices that have a category in the dictionary.
    count = 0
    for row in rows:
        topic1_id = row[0]
        topic1 = row[1]
        topics = stro
        if word_entry is not None:
            topic1 = row[1]
            letter1 = source_name_letter(source_name)
            edge_table1 = edge_table_name(letter1)
            topics = compute_strongly_related_neighbors(topic1)
            for topic2 in topics:
                topic2_id = find_topic_id(topic2)
                letter2 = source_name_letter(source_name)
                edge_table2 = edge_table_name(letter2)
                query1 = "UPDATE " + edge_table1 + \
                        " SET type='strongly related' " + \
                        " WHERE source=" + str(topic1_id) + \
                        " AND target=" + str(topic2_id) + ";"
                query2 = "UPDATE " + edge_table2 + \
                        " SET type='strongly related' " + \
                        " WHERE source=" + str(topic2_id) + \
                        " AND target=" + str(topic1_id) + ";"
                cur.execute(query1)
                cur.execute(query2)
                conn.commit()
                count += 1
                if count%100==0:
                    print ("Root topics updated: " + str(count))
    return True

#------------------------------------------------------------------------------

def update_root_vertex_types():
    conn = ensure_connection()
    cur = conn.cursor()
    
    # Get the Untyped root vertices
    cur.execute("SELECT id, name FROM " + ROOT_TOPICS_TABLE + " WHERE type IS NULL;")
    rows = cur.fetchall()
    print ('\nTotal root vertices: ' + str(count_root_vertices()))
    print ('Untyped root vertices: ' + str(len(rows)) + '\n')

    # Update those vertices that have a category in the dictionary.
    count = 0
    for row in rows:
        word_entry = find_dictionary_word(row[1])
        if word_entry is not None:
            id = row[0]
            word_type = word_entry[4]
            if type != '' or type=='NIL':
                query = "UPDATE " + ROOT_TOPICS_TABLE + " SET type='" + word_type + \
                        "' WHERE id=" + str(id) + ";"
                cur.execute(query)
                conn.commit()
                count += 1
                if count%10==0:
                    print ("Root vertices updated: " + str(count))
    return True



#*****************************************************************************
# Part 8: Root Subtopics Table
#*****************************************************************************

#------------------------------------------------------------------------------
# Root Subtopics Table Creation
#------------------------------------------------------------------------------

ROOT_SUBTOPICS_TABLE = 'root_subtopics'

# NB: The priimary key will be a vertex id from the vertex table.

ROOT_SUBTOPICS_STR = "CREATE TABLE " + ROOT_SUBTOPICS_TABLE + \
                    "(id serial NOT NULL, " + \
                    "root_id integer NOT NULL, " + \
                    "subtopic_id integer NOT NULL, " + \
                    "weight integer DEFAULT 0, " + \
                    "CONSTRAINT root_subtopics_id PRIMARY KEY (id));"

#------------------------------------------------------------------------------

def create_root_subtopics_table(conn=None):
    cur = conn.cursor()
    print ("Creating Root Subtopics Table...")
    cur.execute("DROP TABLE IF EXISTS " + ROOT_SUBTOPICS_TABLE + ";")
    cur.execute(ROOT_SUBTOPICS_STR)
    conn.commit()

#------------------------------------------------------------------------------

def create_root_subtopics_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON " + ROOT_SUBTOPICS_TABLE + " ((root_id));")
    cur.execute("CREATE INDEX ON " + ROOT_SUBTOPICS_TABLE + " ((subtopic_id));")
    conn.commit()

#------------------------------------------------------------------------------
    
def create_root_subtopics_tables(conn=None):
    conn = ensure_connection(conn)
    create_root_subtopics_table(conn)
    create_root_subtopics_indexes(conn)

#------------------------------------------------------------------------------
# Find Root SubTopics
#------------------------------------------------------------------------------

# Returns the subtopics of root_id from ROOT_SUBTOPICS_TABLE and joins
# this with the VERTICES_TABLE thus providing details of each subtopic.

def find_root_subtopics_by_id(root_id, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    query = "SELECT * from " + ROOT_SUBTOPICS_TABLE + " as sb "+ \
            " JOIN " + VERTICES_TABLE + " as vt on vt.id = sb.subtopic_id " + \
            " WHERE sb.root_id=" + str(root_id) + ";"
    cur.execute(query)
    rows = cur.fetchall()
    return rows

#------------------------------------------------------------------------------

def find_root_subtopics_by_name(topic_name, conn=None):
    topic = find_root_topic_by_name(topic_name)
    if topic is not None:
        root_id = topic[0]
        rows = find_root_subtopics_by_id(root_id)
        return rows
    else:
        return None
    
#------------------------------------------------------------------------------

def count_root_subtopics(conn=None):
    return count_table_rows(ROOT_SUBTOPICS_TABLE, conn=conn)

#------------------------------------------------------------------------------

def count_topic_subtopics(topic_name):
    topic = find_root_topic(topic_name)
    if topic is not None:
        return len(find_root_subtopics_by_id(topic[0]))
    else:
        return 0
    
#------------------------------------------------------------------------------
# Insert Root Subtopics
#------------------------------------------------------------------------------

# Inserts potential subtopics of each root vertex.

def insert_root_subtopics():
    conn = ensure_connection()
    cur = conn.cursor()
    root_topics = get_root_vertices(conn=conn)
    count = 0
    for root_topic in root_topics:
        root_id = root_topic[0]
        vertex_name = root_topic[1]
        query ="SELECT * FROM " + ROOT_SUBTOPICS_TABLE + \
               " WHERE root_id=" + str(root_id) + ";"
        cur.execute(query)
        processed = cur.fetchall()
        # Only process if unprocessed
        if processed == []:
            # Get all potential_subtopics
            query = "SELECT * FROM " + VERTICES_TABLE + \
                    " WHERE lower(name) LIKE LOWER('%" + vertex_name + "%');"
            cur.execute(query)
            subtopics = cur.fetchall()
            # print ("Root topic: " + vertex_name + ", Subtopics: " + str(len(subtopics)))
            for subtopic in subtopics:
                if len(subtopic) > 1:
                    subtopic_tokens = list(map (lambda x: x.lower(),
                                                subtopic[1].split('_')))
                    if vertex_name.lower() in subtopic_tokens:
                        query = "INSERT INTO " + ROOT_SUBTOPICS_TABLE + \
                                " (root_id, subtopic_id) " + \
                                " VALUES (%s, %s);"
                        execute_query(query, data=(root_id, subtopic[0]), conn=conn)
            conn.commit()
            count += 1
            if count%100==0:
                x = count_root_subtopics()
                print ("Root topics processed: " + str(count))
                print ("Total subtopics added: " + str(x))
                print ("-------------------------------------------------")
    conn.close()
    return True

#------------------------------------------------------------------------------

def count_unprocessed_subtopics():
    conn = ensure_connection()
    cur = conn.cursor()
    root_topics = get_root_vertices(conn=conn)
    count = 0
    for root_topic in root_topics:
        root_id = root_topic[0]
        vertex_name = root_topic[1]
        query ="SELECT * FROM " + ROOT_SUBTOPICS_TABLE + \
               " WHERE root_id=" + str(root_id) + ";"
        cur.execute(query)
        processed = cur.fetchall()
        # Only process if unprocessed
        if processed == []:
            count += 1
    return count


#*****************************************************************************
# Part 8: Lexicon Tables
#*****************************************************************************

#------------------------------------------------------------------------------
# Dictionary Table Creation
#------------------------------------------------------------------------------

DICTIONARY_TABLE = 'dictionary'

# NB: The priimary key will be a vertex id from the vertex table.

dictionary_str = "CREATE TABLE " + DICTIONARY_TABLE + \
                    "(id serial NOT NULL, " + \
                      "word character varying (50) UNIQUE NOT NULL, " + \
                      "base character varying (50), " + \
                      "pos character varying (25) NOT NULL, " + \
                      "category character varying, " + \
                      "all_pos character varying, " + \
                      "definition character varying, " + \
                      "CONSTRAINT dictionary_id PRIMARY KEY (id));"

#------------------------------------------------------------------------------

def create_dictionary_table(conn):
    cur = conn.cursor()
    print ("Creating Dictionary Table...")
    cur.execute("DROP TABLE IF EXISTS " + DICTIONARY_TABLE + ";")
    cur.execute(dictionary_str)
    conn.commit()

#------------------------------------------------------------------------------

def create_dictionary_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON " + DICTIONARY_TABLE + " ((lower(word)));")
    cur.execute("CREATE INDEX ON " + DICTIONARY_TABLE + " ((lower(pos)));")
    cur.execute("CREATE INDEX ON " + DICTIONARY_TABLE + " ((lower(category)));")
    conn.commit()

#------------------------------------------------------------------------------
    
def create_dictionary_tables(conn=None):
    conn = ensure_connection(conn)
    create_dictionary_table(conn)
    create_dictionary_indexes(conn)

#------------------------------------------------------------------------------
# Find Dictionary Word
#------------------------------------------------------------------------------

def find_dictionary_word(word, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    try: 
        cur.execute("SELECT * FROM " + DICTIONARY_TABLE + " " + \
                    "WHERE LOWER(word)=LOWER('" + word + "');")
        rows = cur.fetchall()
        return rows[0] if rows != [] else None
    except Exception:
        return None

#------------------------------------------------------------------------------
# Find Dictionary Word
#------------------------------------------------------------------------------

def find_dictionary_word_by_id(id, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    query = "SELECT * FROM " + DICTIONARY_TABLE + " WHERE id=" + str(id) + ";"
    cur.execute(query)
    rows = cur.fetchall()
    return rows[0] if rows != [] else None

#------------------------------------------------------------------------------
# Find Dictionary Words
#------------------------------------------------------------------------------

def find_dictionary_words(word, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + DICTIONARY_TABLE + " " + \
                "WHERE LOWER(word) LIKE('%" + word + "%');")
    return cur.fetchall()

#------------------------------------------------------------------------------
# Find Dictionary Definitions
#------------------------------------------------------------------------------

def find_dictionary_definitions(definition, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + DICTIONARY_TABLE + " " + \
                "WHERE LOWER(definition) LIKE LOWER('%" + definition + "%');")
    return cur.fetchall()

#------------------------------------------------------------------------------
# Find Defined Words
#------------------------------------------------------------------------------

def find_defined_words(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + DICTIONARY_TABLE + " " + \
                "WHERE definition IS NOT NULL;")
    rows = cur.fetchall()
    return rows if rows != [] else None

#------------------------------------------------------------------------------
# Find Undefine Words
#------------------------------------------------------------------------------

def find_undefined_words(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + DICTIONARY_TABLE + " " + \
                "WHERE definition IS NULL;")
    rows = cur.fetchall()
    return rows if rows != [] else None

#------------------------------------------------------------------------------
# Dictionary Table INSERT operations
#------------------------------------------------------------------------------

# Inserts a single dictionary words into DICTIONARY_TABLE if it does not
# already exist.

# word_entry: [<word> <pos> <base> <definition>]

def add_dictionary_word(word_entry, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()

    # Destructure and process
    word, pos, base, definition = word_entry
    if type(pos)==list:
        if len(pos) > 1:
            other_pos = pos[1]
        else:
            other_pos = pos[0]
        pos = pos[0]
    else:
        other_pos = pos

    # print ('Other Pos: ' + other_pos)
    # Insert or Update
    if find_dictionary_word(word) is None:
        query = "INSERT INTO " + DICTIONARY_TABLE +\
                "(word, base, pos, category, all_pos, definition) " +\
                "VALUES (%s, %s, %s, %s, %s, %s);"
        data = [word, base, pos, pos, other_pos, definition]
        cur.execute(query, data)
    
    else:
        query = "UPDATE " + DICTIONARY_TABLE + " SET " +\
                "pos='" + pos + "'," + \
                "base='" + base + "', " + \
                "all_pos='" + other_pos + "', " + \
                "definition='" + definition + "' " + \
                "WHERE word='" + word + "';"
        cur.execute(query)
        
    # Insert the words 
     
    # Commit and close
    conn.commit()
    conn.close()
        
    return True

#------------------------------------------------------------------------------

# Inserts a df of dictionary words into DICTIONARY_TABLE

def add_dictionary_words(df, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    
    # Inset string
    insert_str = "INSERT INTO " + DICTIONARY_TABLE +\
                 "(word, base, pos, category, all_pos) " +\
                 "VALUES (%s, %s, %s, %s, %s);"

    # Insert the words 
    data_list = [list(row) for row in df.itertuples(index=False)]
    cur.executemany(insert_str, data_list)
     
    # Commit and close
    conn.commit()
    conn.close()
        
    return True

#------------------------------------------------------------------------------
# Update Word Definition
#------------------------------------------------------------------------------

# This commits on each update.

def update_word_definition(id, definition, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    update_str = "UPDATE " + DICTIONARY_TABLE + " SET definition='" + \
                 definition + \
                 "' WHERE id=" + str(id) + ";"
    cur.execute(update_str)
    conn.commit()
    return True

#------------------------------------------------------------------------------
# Counting Dictionary Words
#------------------------------------------------------------------------------

def count_dictionary_words(conn=None):
    return count_table_rows(DICTIONARY_TABLE, conn=conn)

#------------------------------------------------------------------------------

def count_word_definitions(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    result = run_query ("SELECT count(*) from " + DICTIONARY_TABLE + \
                        " WHERE definition IS NOT NULL;",
                        conn=conn)
    return result[0][0]

#------------------------------------------------------------------------------
# NIL Category Entries
#------------------------------------------------------------------------------

# Categories can potentially be created with NIL values from Common Lisp
# generted data. THese functions clean up those values.

def get_nil_category_entries(conn=None):
    conn = ensure_connection(conn)
    query = "SELECT id, category from " + DICTIONARY_TABLE + \
            " WHERE category='NIL'" + \
            " OR category='';"
    results = run_query(query, conn=conn)
    return results

#------------------------------------------------------------------------------

# This replaces all NIL and '' category values in the dictionary with
# a NULL DB value.

def update_nil_category_entries():
    conn = ensure_connection()
    cur = conn.cursor()
    results = get_nil_category_entries(conn=conn)
    count = 0
    print ('\nTotal entries to update: ' + str(len(results)) + '\n')
    for result in results:
        id = result[0]
        update_str = "UPDATE " + DICTIONARY_TABLE + " SET category = NULL" +\
                     " WHERE id = " + str(id) + ";"
        execute_query(update_str)
        count += 1
        if count%1000==0:
            print ('Categories updated: ' + str(count))
    conn.commit()
    return True

#------------------------------------------------------------------------------
# Unknown Words Table Creation
#------------------------------------------------------------------------------

UNKNOWN_WORDS_TABLE = 'dictionary_unknown'

# NB: The primary key will be a vertex id from the vertex table.

UNKNOWN_WORDS_STR = "CREATE TABLE " + UNKNOWN_WORDS_TABLE + \
                    "(id serial NOT NULL, " + \
                    "word character varying, " + \
                    "status character varying, " + \
                    "CONSTRAINT unknown_words_id PRIMARY KEY (id));"

#------------------------------------------------------------------------------

def create_unknown_words_table(conn=None):
    cur = conn.cursor()
    print ("Creating Unknown Words Table...")
    cur.execute("DROP TABLE IF EXISTS " + UNKNOWN_WORDS_TABLE + ";")
    cur.execute(UNKNOWN_WORDS_STR)
    conn.commit()

#------------------------------------------------------------------------------

def create_unknown_words_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON " + UNKNOWN_WORDS_TABLE + " ((lower(word)));")
    conn.commit()

#------------------------------------------------------------------------------
    
def create_unknown_words_tables(conn=None):
    conn = ensure_connection(conn)
    create_unknown_words_table(conn)
    create_unknown_words_indexes(conn)

#------------------------------------------------------------------------------
# Find Unknown Words Word
#------------------------------------------------------------------------------

def find_unknown_word(word, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + UNKNOWN_WORDS_TABLE + " " + \
                "WHERE LOWER(word)=LOWER('" + word + "');")
    rows = cur.fetchall()
    return rows[0] if rows != [] else None
    
#------------------------------------------------------------------------------
# Unknown Words Table INSERT operation
#------------------------------------------------------------------------------

def add_unknown_word(word, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()

    if find_unknown_word(word) is None:
        try:
            # Insert string
            insert_str = "INSERT INTO " + UNKNOWN_WORDS_TABLE +\
                " (word, status) " +\
                "VALUES ('" + word + "', 'unknown');"

            print ("Insert str: " + insert_str)
            # Insert the words 
            cur.execute(insert_str)
     
            # Commit and close
            conn.commit()
            conn.close()
            return True
        except Exception as err:
            print ('Error: ' + str(err))
            return False
    return False

#------------------------------------------------------------------------------
# Unknown Words Table INSERT operation
#------------------------------------------------------------------------------

def add_unknown_words(df, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    
    # Inset string
    insert_str = "INSERT INTO " + UNKNOWN_WORDS_TABLE +\
                 " (word, status) " +\
                 "VALUES (%s, %s);"

    # Insert the words 
    data_list = [list(row) for row in df.itertuples(index=False)]
    cur.executemany(insert_str, data_list)
     
    # Commit and close
    conn.commit()
    conn.close()
        
    return True

#------------------------------------------------------------------------------

def count_unknown_words(conn=None):
    return count_table_rows(UNKNOWN_WORDS_TABLE, conn=conn)


#*****************************************************************************
# Part 9: Quote Table
#*****************************************************************************

#------------------------------------------------------------------------------
# Dictionary Table Creation
#------------------------------------------------------------------------------

QUOTES_TABLE = 'famous_quotes'

# NB: The priimary key will be a vertex id from the vertex table.

quotes_str = "CREATE TABLE " + QUOTES_TABLE + \
    "(id serial NOT NULL, " + \
    "author character varying (50) NOT NULL, " + \
    "quote character varying (500) UNIQUE, " + \
    "topic character varying (50) NOT NULL, " + \
    "source character varying (50) NOT NULL, " + \
    "source_url character varying (200), " + \
    "category character varying (20), " + \
    "created_on timestamp, " + \
    "CONSTRAINT quotes_id PRIMARY KEY (id));"

#------------------------------------------------------------------------------

def create_quotes_table(conn):
    cur = conn.cursor()
    print ("Creating Quotes Table...")
    cur.execute("DROP TABLE IF EXISTS " + QUOTES_TABLE + ";")
    cur.execute(quotes_str)
    conn.commit()

#------------------------------------------------------------------------------

def create_quotes_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON " + QUOTES_TABLE + " ((lower(author)));")
    cur.execute("CREATE INDEX ON " + QUOTES_TABLE + " ((lower(quote)));")
    cur.execute("CREATE INDEX ON " + QUOTES_TABLE + " ((lower(topic)));")
    cur.execute("CREATE INDEX ON " + QUOTES_TABLE + " ((lower(category)));")
    conn.commit()

#------------------------------------------------------------------------------
    
def create_quotes_tables(conn=None):
    conn = ensure_connection(conn)
    create_quotes_table(conn)
    create_quotes_indexes(conn)
    
#------------------------------------------------------------------------------
# Add Topic Quotes
#------------------------------------------------------------------------------

# DF is a datframe with columns:
# author, quote, topic, source, source_url, timestamp

def add_topic_quotes_1(df, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    
    # Inset string
    insert_str = "INSERT INTO " + QUOTES_TABLE +\
                 " (author, quote, topic, source, source_url, created_on) " +\
                 "VALUES (%s, %s, %s, %s, %s, %s);"

    # Insert the quotes
    for index, row in df.iterrows():
        data = list(row)
        try:
            cur.execute(insert_str, data)
            conn.commit()
        except Exception as err:
            conn.close()
            conn = ensure_connection()
            cur = conn.cursor()
     
    # Commit and close
    conn.close()
        
#------------------------------------------------------------------------------

# DF is a datframe with columns:
# author, quote, topic, source, source_url, timestamp

def add_topic_quotes_2(df, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    
    # Inset string
    insert_str = "INSERT INTO " + QUOTES_TABLE +\
                 " (author, quote, topic, source, source_url, created_on) " +\
                 "VALUES (%s, %s, %s, %s, %s, %s);"

    # Insert the words 
    data_list = [list(row) for row in df.itertuples(index=False)]
    cur.executemany(insert_str, data_list)
     
    # Commit and close
    conn.commit()
    conn.close()
        
    return True


#------------------------------------------------------------------------------
# End of File
#-----------------------------------------------------------------------------
