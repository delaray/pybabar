import psycopg2

try:
    connect_str = "dbname='wikidb' user='postgres' host='localhost' " +  "password='postgres'"
    # use our connection values to establish a connection
    conn = psycopg2.connect(connect_str)

    # create a psycopg2 cursor that can execute queries
    cursor = conn.cursor()
    # Delete previous table
    cursor.execute("DROP TABLE IF EXISTS wikivertices;")
    create_vertices_str = "CREATE TABLE wikivertices(id serial NOT NULL, name character varying, CONSTRAINT pid PRIMARY KEY (id));"    # create a new table with a single column called "name"
    cursor.execute(create_vertices_str)
    conn.commit()
    # Insert a record
    cursor.execute("""INSERT INTO wikivertices (name) VALUES ('foo');""")
    cursor.execute("""INSERT INTO wikivertices (name) VALUES ('bar');""")
    # run a SELECT statement - no data in there, but we can try it
    cursor.execute("""SELECT * from wikivertices""")
    rows = cursor.fetchall()
    print(rows)
except Exception as e:
    print("Uh oh, can't connect. Invalid dbname, user or password?")
    print(e)
