
#*****************************************************************************
# Part 9: # BOGUS VERTICES
#*****************************************************************************

# Finds Topics that contain the hashtag charcter '#'.

def find_bogus_vertices (conn=None):
    conn = ensure_connection()
    cur = conn.cursor()
    cur.execute("SELECT * FROM " + VERTICES_TABLE +  \
                " WHERE name like '%\#%'")
    rows = cur.fetchall()
    return rows

#------------------------------------------------------------------------------

# Deletes topics and corresponding edges that contain the hashtag charcter '#'.

def delete_bogus_vertex(vertex_name, conn=None):
    conn = ensure_connection()
    cur = conn.cursor()

    # Gather the vertex and edges
    vertex_row = find_topic(vertex_name, conn)
    vertex_id = vertex_row[0] if vertex_row is not None else None
    if vertex_id==None:
        return False

    in_neighbors = find_topic_in_neighbors(vertex_name, conn=conn)
    out_neighbors = find_topic_out_neighbors(vertex_name, conn=conn)

    if len(out_neighbors) < 5 and len(in_neighbors) < 5:
        # Delete the vertex first
        cur.execute("DELETE FROM " + VERTICES_TABLE  + " as wv " + \
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
            source_row = find_topic(source_name, conn)
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

#------------------------------------------------------------------------------
# End of File
#-----------------------------------------------------------------------------
