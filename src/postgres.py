import psycopg2
import nltk

#import clustering

#vertex_table_name = 'old_wiki_vertices'
vertex_table_name = 'wiki_vertices'

#edge_table_name = 'old_wiki_edges'
edge_table_name = 'wiki_edges'

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
# Wiki DB Table Creation
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

create_edges_str = "CREATE TABLE wiki_edges (id serial NOT NULL, " + \
                   "source integer NOT NULL," + \
                   "target integer NOT NULL, " + \
                   "type character varying, " + \
                   "weight integer DEFAULT 0, " + \
                   "CONSTRAINT edge_id PRIMARY KEY (id));"

def create_edges_table(conn):
    cur = conn.cursor()
    print ("Creating Wikipedia Edges Table...")
    cur.execute("DROP TABLE IF EXISTS " + edge_table_name + ";")
    cur.execute(create_edges_str)

#------------------------------------------------------------------------------

def create_edges_table_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON " + edge_table_name + " (source);")
    cur.execute("CREATE INDEX ON " + edge_table_name + " (target);")
    conn.commit()
    
#------------------------------------------------------------------------------

def create_wiki_db_graph_tables():
    conn = wikidb_connect()
    create_vertices_table(conn)
    create_vertices_table_indexes(conn)
    create_edges_table(conn)
    create_edges_table_indexes(conn)
    conn.commit()
    return None 

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
    # cur.execute("SELECT count(*) FROM " + vertex_table_name)
    cur.execute("SELECT count(*) FROM wiki_vertices")
    rows = cur.fetchall()
    return rows[0][0]

#------------------------------------------------------------------------------
# Wiki DB Edge Table Maintenace
#------------------------------------------------------------------------------

DEFAULT_EDGE_TYPE = 'related'

def add_wiki_edge(source_name, target_name, edge_type=DEFAULT_EDGE_TYPE, conn=None, commit_p=False):
    conn = ensure_connection(conn)
    source_id = find_wiki_vertex(source_name, conn)
    target_id = find_wiki_vertex(target_name, conn)
    if source_id==None or target_id==None:
        return None
    else:
        if find_wiki_edge_by_id(source_id, target_id, conn) == None:
            cur = conn.cursor()
            cur.execute("INSERT INTO " + edge_table_name + " (source, target, type) " +\
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

def find_wiki_edge_by_id(source_id, target_id, conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + edge_table_name + " " + \
                "WHERE source=" + str(source_id) + "AND target=" + str(target_id) + ";")
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
    if source_id==None or target_id==None:
        return None
    else:
        return find_wiki_edge_by_id(source_id, target_id, conn)

#------------------------------------------------------------------------------

def find_wiki_edges(source_name, edge_type, conn=None):
    conn = ensure_connection(conn)
    source_id = find_wiki_vertex(source_name, conn)
    if source_id==None:
        return None
    else:
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + edge_table_name + " " + \
                    "WHERE source=" + str(source_id) + "AND type='" + edge_type + "';")
        rows = cur.fetchall()
        return rows

#------------------------------------------------------------------------------

def count_wiki_edges(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    # cur.execute("SELECT count(*) FROM " + edge_table_name)
    cur.execute("SELECT count(*) FROM wiki_edges")
    rows = cur.fetchall()
    return rows[0][0]

#------------------------------------------------------------------------------
# IN Neighbors and OUT Neighbors
#------------------------------------------------------------------------------

def find_wiki_in_neighbors(topic_name, conn=None):
    conn = ensure_connection(conn)
    topic_id = find_wiki_vertex(topic_name, conn)
    if topic_id == None:
        return []
    else:
        cur = conn.cursor()
        cur.execute("SELECT * FROM " + edge_table_name + " as we " + \
                    "JOIN " + vertex_table_name + " as wv on we.source = wv.id " + \
                    "WHERE target=" + str(topic_id) + ";")
        rows = cur.fetchall()
        return list(set([row[6] for row in rows]))

#------------------------------------------------------------------------------

def find_wiki_out_neighbors(topic_name, conn=None):
    conn = ensure_connection(conn)
    topic_id = find_wiki_vertex(topic_name, conn)
    cur = conn.cursor()
    if topic_id == None:
        return []
    else:
        cur.execute("SELECT * FROM " + edge_table_name + " as we " + \
                    "JOIN " + vertex_table_name + " as wv on we.target = wv.id " + \
                    "WHERE source=" + str(topic_id) + ";")
        rows = cur.fetchall()
        return list(set([row[6] for row in rows]))
    
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

# Current finds all topic names that do not contain an underscore. This wwill be 
# be used to seed the different subtopic hierarchies.

def find_wiki_root_topics(conn=None):
    conn = ensure_connection(conn) 
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + vertex_table_name + " as wv " + \
                "WHERE wv.name NOT SIMILAR TO '%\_%'")
    rows = cur.fetchall()
    return [row[1] for row in rows]

#------------------------------------------------------------------------------

def pdm_worker (l1, l2, procnum, return_dict):
    conn = ensure_connection()
    m = clustering.generate_distance_matrix (l1, l2, conn)
    return_dict[procnum] = m


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
#------------------------------------------------------------------------------
