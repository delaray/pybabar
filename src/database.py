#*****************************************************************************
# WIKIDB POSTGRES DB INTERFACE
#*****************************************************************************
#
# Part 1: Creation operations
# Part 2: Retrieval operations
# Part 3: Status Operations
# Part 4: Maintenance Operations
# Part 5: Deletion operations
# Part 6: Lexicon Tables
#
#*****************************************************************************
#
# DUMPING AND RESTORING THE DATABASE
#
# pg_dump -h localhost -p 5432 -U postgres -F c -b -v -f "wikidb.dump" wikidb
#
# NB: Delete and recreate the wikidb before doing a restore.
#
# pg_restore -h localhost -p 5432 -U postgres -d wikidb -v "wikidb.dump"
#
#*****************************************************************************


import sys
import psycopg2
import string
import pprint
import pandas as pd
from  urllib.parse import unquote

from functools import reduce
from multiprocessing import Process, Manager, freeze_support

# Project Imports
import src.processes

#******************************************************************************
# TABLE CREATION: Vertices, Eges, Root Vertices
#******************************************************************************

#*****************************************************************************
# Part 1: Table and Index Creation
#*****************************************************************************

#------------------------------------------------------------------------------
# Table Names
#------------------------------------------------------------------------------

vertex_table = 'wiki_vertices'
ROOT_VERTICES_TABLE='wiki_root_vertices'
edge_table_prefix = 'wiki_edges_'

#------------------------------------------------------------------------------

# There are about 50,000,000 edges and we divide them amongst 37 tabbles based
# the first letter of te edge's source vertex name.

# Use both letters and digits given the number of digit based topic names.

# TODO: Convert to constant.

def table_suffixes():
    return list(string.ascii_lowercase) + list(map(str, [0,1,2,3,4,5,6,7,8,9]))

#------------------------------------------------------------------------------

def source_name_letter (name):
    letter = name[0].lower()
    if letter in table_suffixes():
        return letter
    else:
        return 'z'

#------------------------------------------------------------------------------

DEFAULT_EDGE_TYPE = 'related'

#------------------------------------------------------------------------------
# DB Connection
#------------------------------------------------------------------------------

# TODO: Move connection parameters into environment variables.

def wikidb_connect():
    conn = psycopg2.connect("dbname='wikidb' user='postgres' password='postgres' host='localhost'")
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

# -----------------------------------------------------------------------------

def run_query(query, data=(), conn=None):
    conn = conn if conn != None else ensure_connection()
    cur = conn.cursor()
    cur.execute(query, data)
    rows = cur.fetchall()
    return rows

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

#------------------------------------------------------------------------------
# Vertex Table Creation
#------------------------------------------------------------------------------

create_vertices_str = "CREATE TABLE " + vertex_table + " (id serial NOT NULL, " + \
                      "name character varying, " + \
                      "weight integer DEFAULT 0, " + \
                      "CONSTRAINT vertex_id PRIMARY KEY (id));"

def create_vertices_table(conn):
    cur = conn.cursor()
    print ("Creating Wikipedia Vertices Table...")
    cur.execute("DROP TABLE IF EXISTS " + vertex_table + ";")
    cur.execute(create_vertices_str)

#------------------------------------------------------------------------------

def create_vertices_table_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON " + vertex_table + " ((lower(name)));")
    conn.commit()

#------------------------------------------------------------------------------
# Wiki DB Root Vertices Table Creation
#------------------------------------------------------------------------------

# NB: The priimary key will be a vertex id from the vertex table.

root_vertices_str = "CREATE TABLE " + ROOT_VERTICES_TABLE + \
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
    cur.execute("DROP TABLE IF EXISTS " + ROOT_VERTICES_TABLE + ";")
    cur.execute(root_vertices_str)
    conn.commit()

#------------------------------------------------------------------------------

def create_root_vertices_table_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON " + ROOT_VERTICES_TABLE + " ((lower(name)));")
    conn.commit()

#------------------------------------------------------------------------------
    
def create_root_vertices_tables(conn=None):
    conn = ensure_connection(conn)
    create_root_vertices_table(conn)
    create_root_vertices_table_indexes(conn)

#------------------------------------------------------------------------------

root_fields = "(id, name, weight, outdegree)"
    
def add_root_vertex(row, conn=None, commit=False):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    query = "INSERT INTO " + ROOT_VERTICES_TABLE + " " + root_fields + " VALUES " + row_to_str(row) + ";"
    try:
        cur.execute(query)
        if commit == True:
            conn.commit()
    except Exception as err:
        print ("Error: " + str(err))

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
    if letter in table_suffixes():
        return edge_table_prefix + letter
    else:
        return edge_table_prefix + '0'

#------------------------------------------------------------------------------

def edge_tables (suffixes=table_suffixes()):
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
    cur.execute("SELECT * FROM " + vertex_table + " WHERE id=" + str(vertex_id) + ";")
    rows = cur.fetchall()
    return rows[0] if rows != [] else None

#------------------------------------------------------------------------------

def find_wiki_vertex(vertex_name, conn=None):
    if "'" in vertex_name:
        return None
    else:
        conn = ensure_connection(conn)
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + vertex_table + " " + \
                    "WHERE LOWER(name)=LOWER('" + vertex_name + "');")
        rows = cur.fetchall()
        return rows[0] if rows != [] else None

#------------------------------------------------------------------------------

def vertex_id_name(vertex_id,  conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + vertex_table + " WHERE id=" + str(vertex_id) + ";")
    rows = cur.fetchall()
    return rows[0][1]

#------------------------------------------------------------------------------

def vertex_row_name(vertex_row):
    return vertex_row[1]

#------------------------------------------------------------------------------

# Returns a list of vertex names

def get_wiki_vertices(vertex_pattern, conn=None):
    if "'" in vertex_pattern:
        return None
    else:
        conn = ensure_connection(conn)
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + vertex_table + " " + \
                    "WHERE LOWER(name) like LOWER('" + vertex_pattern + "');")
        rows = cur.fetchall()
        return  rows

#------------------------------------------------------------------------------
    
def find_wiki_vertices(vertex_pattern, conn=None):
    rows = get_wiki_vertices(vertex_pattern, conn)
    return [row[1] for row in rows] if rows != [] else []


#------------------------------------------------------------------------------
#  identify_root_vertices
#------------------------------------------------------------------------------

def root_vertex__p (vertex_id):
    vertex_row = find_wiki_vertex(vertex_id)
    if vertex_name is not None:
        return root_vertex_name_p (vertex_row_name(vertex_row))
    else:
        return False
    
#------------------------------------------------------------------------------

def root_vertex_name_p (vertex_name):
    return '_' not in vertex_name and '#' not in vertex_name

#------------------------------------------------------------------------------

# Identifies root vertices in  verteices table for specified pattern

def identify_root_vertices (pattern, conn=None):
    conn = ensure_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + vertex_table + " as wv " + \
                "WHERE wv.name NOT SIMILAR TO '%\_%' " + \
                "AND wv.name NOT SIMILAR TO '%\#%'" + \
                "AND LOWER(wv.name) like LOWER('" + pattern + "');")
    rows = cur.fetchall()
    return rows

#------------------------------------------------------------------------------
# In Neighbors
#------------------------------------------------------------------------------

# Returns a list of neighbor topic names that are pointed to by <topic_name>.
# These will necessarily be stored in the same table, so we only need to
# query one table.

def find_wiki_out_neighbors(topic_name, conn=None):
    conn = ensure_connection(conn)
    topic_row = find_wiki_vertex(topic_name, conn)
    topic_id = topic_row[0] if topic_row is not None else None
    cur = conn.cursor()
    if topic_id == None:
        return []
    else:
        letter = source_name_letter(topic_name)
        edge_table = edge_table_name(letter)
        cur.execute("SELECT * FROM " + edge_table + " as we " + \
                    "JOIN " + vertex_table + " as wv on we.target = wv.id " + \
                    "WHERE source=" + str(topic_id) + ";")
        rows = cur.fetchall()
        return list(set([row[6] for row in rows]))

#------------------------------------------------------------------------------
# In Neighbors
#------------------------------------------------------------------------------

# Returns a list of neighbor topic names that point to <topic_name>. These will
# necessarily be scattered across several tables, so we need to query each of
# them.
 
def _find_wiki_in_neighbors(topic_name, tables=None, conn=None):
    conn = ensure_connection(conn)
    topic_row = find_wiki_vertex(topic_name, conn)
    topic_id = topic_row[0] if topic_row is not None else None
    if topic_id == None:
        return []
    else:
        if tables == None:
            tables = edge_tables()
        cur = conn.cursor()
        all_rows = []
        for edge_table in tables:
            cur.execute("SELECT * FROM " + edge_table + " as we " + \
                        "JOIN " + vertex_table + " as wv on we.source = wv.id " + \
                        "WHERE target=" + str(topic_id) + ";")
            rows = cur.fetchall()
            all_rows += rows
        return list(set([row[6] for row in all_rows]))

#------------------------------------------------------------------------------
# In Neigbors (PARALLEL Version)
#------------------------------------------------------------------------------

def find_wiki_in_neighbors(topic_name, conn=None, threads=8):
    conn = ensure_connection(conn)
    topic_id = find_wiki_vertex(topic_name, conn)
    topic_id = topic_id[0] if topic_id is not None else topic_id
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
        p = Process(target=neighbor_worker, args=(topic_name, piece, index, return_dict))
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
# Root Vertex Table Retrieval Operations
#------------------------------------------------------------------------------

def get_root_vertex(vertex_id, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + ROOT_VERTICES_TABLE + " WHERE id=" + str(vertex_id) + ";")
    rows = cur.fetchall()
    return rows[0] if rows != [] else None

#------------------------------------------------------------------------------

def get_root_vertices(vertex_id, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + ROOT_VERTICES_TABLE + ";")
    rows = cur.fetchall()
    return rows

#------------------------------------------------------------------------------

def find_root_vertex(vertex_name, conn=None):
    if "'" in vertex_name:
        return None
    else:
        conn = ensure_connection(conn)
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + ROOT_VERTICES_TABLE + " " + \
                    "WHERE LOWER(name)=LOWER('" + vertex_name + "');")
        rows = cur.fetchall()
        return rows[0] if rows != [] else None

#------------------------------------------------------------------------------

def find_related_root_topics (vertex_name):
    related_topics = find_wiki_out_neighbors(vertex_name)
    return [topic for topic in related_topics if root_vertex_name_p(topic)]

#------------------------------------------------------------------------------
# Edge Table Retrieval Operations
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
    source_id = source_id[0] if source_id is not None else source_id
    target_id = target_id[0] if target_id is not None else target_id
    letter = source_name_letter(source_name)
    edge_table = edge_table_name(letter)
    if source_id==None or target_id==None:
        return None
    else:
        return find_wiki_edge_by_id(edge_table,source_id, target_id, conn)

#------------------------------------------------------------------------------

# Source can be a string or integer.  Return an integer source id.

def ensure_source_id(source, conn=None):
    if type(source) == str:
        return find_wiki_vertex(source, conn)[0]
    else:
        return source

#------------------------------------------------------------------------------

# NB: <source> can be an id or a name.

def find_wiki_edges(source_name, edge_type=DEFAULT_EDGE_TYPE, conn=None):
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

    
#******************************************************************************
# Part 3: Status Operations
#******************************************************************************

def count_wiki_vertices(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM wiki_vertices")
    rows = cur.fetchall()
    return rows[0][0]

#------------------------------------------------------------------------------

def count_root_vertices(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM wiki_root_vertices")
    rows = cur.fetchall()
    return rows[0][0]

#------------------------------------------------------------------------------

# vertex can be a vertex_id or a vertex_name.

def count_vertex_out_neighbors (vertex, conn=None):
    return len(find_wiki_edges(vertex, conn=conn))

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
        if find_wiki_vertex(vertex_name, conn) == None:
            cur.execute("INSERT INTO " + vertex_table + " (name) VALUES ('" + vertex_name + "');")
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
        cur.execute("UPDATE " + vertex_table + " SET weight = " + str(weight) + \
                    "WHERE id=" + str(vertex_id) + ";")
        if commit==True:
            conn.commit()
        return find_wiki_vertex(vertex_id, conn=conn)
    except Exception as err:
        print ("Failed to update vertex weight for vertex: " + str(vertex_id))
        return None

#------------------------------------------------------------------------------

def update_all_vertex_weights ():
    count = 0
    for symbol in table_suffixes():
        rows = find_wiki_vertices(symbol + '%')
        print ("Processinng " + str(len(vertices)) + " vertices starting with " + symbol + "...")
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
    vertex_prefixes = table_suffixes()
    for prefix in vertex_prefixes:
        print ("\nProcessing prefix " + prefix)
        generate_root_vertices_for_prefix(prefix+"%")

#------------------------------------------------------------------------------
# Wiki DB Edge Table Maintenace
#------------------------------------------------------------------------------


def add_wiki_edge(source_name, target_name, edge_type=DEFAULT_EDGE_TYPE, conn=None, commit_p=False):
    conn = ensure_connection(conn)
    letter = source_name_letter(source_name)
    edge_table = edge_table_name(letter)
    source_id = find_wiki_vertex(source_name, conn)
    target_id = find_wiki_vertex(target_name, conn)
    source_id = source_id[0] if source_id is not None else source_id
    target_id = target_id[0] if target_id is not None else target_id
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
# BOGUS VERTICES
#------------------------------------------------------------------------------

def find_bogus_vertices (conn=None):
    conn = ensure_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + vertex_table +  \
                " WHERE name like '%\#%'")
    rows = cur.fetchall()
    return rows

#------------------------------------------------------------------------------

def delete_bogus_vertex(vertex_name, conn=None):
    conn = ensure_connection()
    cur = conn.cursor()

    # Gather the vertex and edges
    vertex_row = find_wiki_vertex(vertex_name, conn)
    vertex_id = vertex_row[0] if vertex_row is not None else None
    if vertex_id==None:
        return False

    in_neighbors = find_wiki_in_neighbors(vertex_name, conn=conn)
    out_neighbors = find_wiki_out_neighbors(vertex_name, conn=conn)

    if len(out_neighbors) < 5 and len(in_neighbors) < 5:
        # Delete the vertex first
        cur.execute("DELETE FROM " + vertex_table  + " as wv " + \
                    " WHERE wv.id=" + str(vertex_id) + ";")
        conn.commit()

        # Delete outbound edges
        outbound_edge_table = edge_table_name(vertex_name[0])
        cur.execute("DELETE FROM " + outbound_edge_table  + " as oe " + \
                    " WHERE source=" + str(vertex_id) + ";")
        conn.commit()

        # Delete inbound edges
        for source_name in in_neighbors:
            inbound_edge_table = edge_table_name(source_name[0])
            source_row = find_wiki_vertex(source_name, conn)
            source_id = source_row[0] if source_row is not None else None
            if source_id != None:
                cur.execute("DELETE FROM " + inbound_edge_table  + " as ie " + \
                            " WHERE ie.source=" + str(source_id) + \
                            " AND ie. target=" + str(vertex_id) + ";")
        conn.commit()
        print ("Deleted 1 vertex, " + str(len(out_neighbors)) + " outbound edges and " + \
               str(len(in_neighbors)) + " inbound edges.")
    # Wrap up
    conn.close()
    return True

#------------------------------------------------------------------------------

def delete_bogus_vertices(conn=None):
    conn = ensure_connection()
    bogus_vertices = find_bogus_vertices(conn)
    count = 0
    print ("Found " + str(len(bogus_vertices)) + " bogus vertices.")
    for vertex_row in bogus_vertices:
        count += 1
        if count%100==0:
            print ("Deleted " + str(count) + " bogus vertices...")
        delete_bogus_vertex(vertex_row[1], conn=conn)
    return True

#*****************************************************************************
# Part 5: Lexicon Tables
#*****************************************************************************

#------------------------------------------------------------------------------
# Dictionary Table Creation
#------------------------------------------------------------------------------

DICTIONARY_TABLE = 'dictionary'

# NB: The priimary key will be a vertex id from the vertex table.

dictionary_str = "CREATE TABLE " + DICTIONARY_TABLE + \
                    "(id serial NOT NULL, " + \
                      "word character varying, " + \
                      "base character varying, " + \
                      "pos character varying, " + \
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
    cur.execute("SELECT * FROM " + DICTIONARY_TABLE + " " + \
                "WHERE LOWER(word)=LOWER('" + word + "');")
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
# Dictionary Table INSERT operation
#------------------------------------------------------------------------------

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
 
    # Insert the words
    # for index, row in df.iterrows():
    #     data = list(row)
    #     execute_query(insert_str, data=data, conn=conn)
    
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
    conn = ensure_connection(conn)
    cur = conn.cursor()
    result = run_query ("SELECT count(*) from " + DICTIONARY_TABLE)
    return result[0][0]

#------------------------------------------------------------------------------

def count_defined_words(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    result = run_query ("SELECT count(*) from " + DICTIONARY_TABLE + \
                        " WHERE definition IS NOT NULL;")
    return result[0][0]

#------------------------------------------------------------------------------
# Unknown Words Table Creation
#------------------------------------------------------------------------------

UNKNOWN_WORDS_TABLE = 'dictionary_unknown'

# NB: The priimary key will be a vertex id from the vertex table.

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

def find_unknown_word_word(word, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + UNKNOWN_WORDS_TABLE + " " + \
                "WHERE LOWER(word)=LOWER('" + word + "');")
    rows = cur.fetchall()
    return rows[0] if rows != [] else None
    
#------------------------------------------------------------------------------
# Unknown Words Table INSERT operation
#------------------------------------------------------------------------------

def add_unknown_words(df, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    
    # Inset string
    insert_str = "INSERT INTO " + UNKNOWN_WORDS_TABLE +\
                 "(word, status) " +\
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
    conn = ensure_connection(conn)
    cur = conn.cursor()
    result = run_query ("SELECT count(*) from " + UNKNOWN_WORDS_TABLE)
    return result[0][0]

#*****************************************************************************
# Part 6: Miscellaneous Operations
#*****************************************************************************

#------------------------------------------------------------------------------
# Miscellneous Output Functions.
#------------------------------------------------------------------------------

def print_raw(text, separator):
    text = text + separator
    textbytes = unquote(text).encode("utf-8")
    sys.stdout.buffer.write(textbytes)

#------------------------------------------------------------------------------
# End of File
#-----------------------------------------------------------------------------
