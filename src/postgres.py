import psycopg2
import nltk

namedict = ({"first_name":"Joshua", "last_name":"Drake"},
            {"first_name":"Steven", "last_name":"Foo"},
            {"first_name":"David", "last_name":"Bar"})


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

create_vertices_str = "CREATE TABLE wiki_vertices(id serial NOT NULL, " + \
                      "name character varying, " + \
                      "weight integer DEFAULT 0, " + \
                      "CONSTRAINT vertextid PRIMARY KEY (id));"

def create_vertices_table(conn):
    cur = conn.cursor()
    print ("Creating Wikipedia Vertices Table...")
    cur.execute("DROP TABLE IF EXISTS wiki_vertices;")
    cur.execute(create_vertices_str)

#------------------------------------------------------------------------------

def create_vertices_table_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON wiki_vertices ((lower(name)));")
    conn.commit()
    
#------------------------------------------------------------------------------

create_edges_str = "CREATE TABLE wiki_edges (id serial NOT NULL, " + \
                   "source integer NOT NULL," + \
                   "target integer NOT NULL, " + \
                   "type character varying, " + \
                   "weight integer DEFAULT 0, " + \
                   "CONSTRAINT edgeid PRIMARY KEY (id));"

def create_edges_table(conn):
    cur = conn.cursor()
    print ("Creating Wikipedia Edges Table...")
    cur.execute("DROP TABLE IF EXISTS wiki_edges;")
    cur.execute(create_edges_str)

#------------------------------------------------------------------------------

def create_edges_table_indexes(conn):
    cur = conn.cursor()
    cur.execute("CREATE INDEX ON wiki_edges (source);")
    cur.execute("CREATE INDEX ON wiki_edges (target);")
    conn.commit()
    
#------------------------------------------------------------------------------

def create_wiki_db_graph_tables():
    conn = wikidb_connect()
    create_vertices_table(conn)
    conn.commit()
    create_edges_table(conn)
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
        if find_wiki_vertex(vertex_name, conn) == []:
            cur.execute("INSERT INTO wiki_vertices (name) VALUES ('" + vertex_name + "');")
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
        cur.execute("SELECT * FROM wiki_vertices " + \
                    "WHERE LOWER(name)=LOWER('" + vertex_name + "');")
        rows = cur.fetchall()
        return rows[0][0]

#------------------------------------------------------------------------------

def vertex_name(vertex_id,  conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT * FROM wiki_vertices WHERE id=" + str(vertex_id) + ";")
    rows = cur.fetchall()
    return rows[0][1]

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
    source_id = find_wiki_vertex(source_name, conn)
    target_id = find_wiki_vertex(target_name, conn)
    if source_id==None or target_id==None:
        return None
    else:
        cur = conn.cursor()
        cur.execute("INSERT INTO wiki_edges (source, target, type) " +\
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

def find_wiki_edge(source_name, target_name, conn=None):
    conn = ensure_connection(conn)
    source_id = find_wiki_vertex(source_name, conn)
    target_id = find_wiki_vertex(target_name, conn)
    if source_id==None or target_id==None:
        return None
    else:
        cur = conn.cursor()
        cur.execute("SELECT * FROM wiki_edges " + \
                    "WHERE source=" + str(source_id) + "AND target=" + str(target_id) + ";")
        rows = cur.fetchall()
        return rows

#------------------------------------------------------------------------------

def find_wiki_edges(source_name, edge_type, conn=None):
    conn = ensure_connection(conn)
    source_id = find_wiki_vertex(source_name, conn)
    if source_id==None:
        return None
    else:
        cur = conn.cursor()
        cur.execute("SELECT * FROM wiki_edges " + \
                    "WHERE source=" + str(source_id) + "AND type='" + edge_type + "';")
        rows = cur.fetchall()
        return rows

#------------------------------------------------------------------------------

def count_wiki_edges(conn=None):
    conn = ensure_connection(conn)
    cur = conn.cursor()
    cur.execute("SELECT count(*) FROM wiki_edges ")
    rows = cur.fetchall()
    return rows[0][0]

#------------------------------------------------------------------------------
# IN and OUT Neighbors
#------------------------------------------------------------------------------

def find_wiki_in_neighbors(topic_name, conn=None):
    conn = ensure_connection(conn)
    topic_id = find_wiki_vertex(topic_name, conn)
    if topic_id == None:
        return []
    else:
        cur = conn.cursor()
        cur.execute("SELECT * FROM wiki_edges as we " + \
                    "JOIN wiki_vertices as wv on we.source = wv.id " + \
                    "WHERE target=" + str(topic_id) + ";")
        rows = cur.fetchall()
        return [row[5] for row in rows]

#------------------------------------------------------------------------------

def find_wiki_out_neighbors(topic_name, conn=None):
    conn = ensure_connection(conn)
    topic_id = find_wiki_vertex(topic_name, conn)
    cur = conn.cursor()
    if topic_id == None:
        return []
    else:
        cur.execute("SELECT * FROM wiki_edges as we " + \
                    "JOIN wiki_vertices as wv on we.target = wv.id " + \
                    "WHERE source=" + str(topic_id) + ";")
        rows = cur.fetchall()
        return [row[5] for row in rows]
    
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
    out_vertices = find_wiki_out_neighbors(topic_name, conn)
    out_vertices = list(set(out_vertices))
    for x in out_vertices:
        if topic_name.lower() in x.lower():
            result.append(x)
    result.remove(topic_name)
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
# Run Time
#------------------------------------------------------------------------------

# create_wiki_db_graph_tables()


#------------------------------------------------------------------------------
# End of File
#------------------------------------------------------------------------------
