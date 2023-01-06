"""
Functions to create, drop and query to mobility tables.
"""

# Imports
import psycopg2
import pandas as pd

#########################
###### Drop tables ######
#########################

def drop_table(table_name):
    """
    Drop tables in the PostgreSQL database
    """
    conn = None
    try:
        # read the connection parameters
        config = {'dbname': 'MITMA', 'user': 'julian', 'host': 'localhost', 'password': '1234'}
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        # create table one by one
        cur.execute("DROP TABLE " + table_name)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            
###########################
###### Create tables ######
###########################

def create_mitma_cat_raw_tables():
    """
    Creates a table with the MITMA raw data in the PostgreSQL database
    """
    conn = None
    queries = (
        """
        CREATE TABLE mitma_cat_raw (
            parameter_id SERIAL PRIMARY KEY,
            datetime timestamp default NULL,
            source VARCHAR(255) NOT NULL,
            target VARCHAR(255) NOT NULL,
            motiv_source VARCHAR(255) NOT NULL,
            motiv_target VARCHAR(255) NOT NULL,
            residency VARCHAR(255) NOT NULL,
            hour INT NOT NULL,
            distance VARCHAR(255) NOT NULL,
            trips FLOAT NOT NULL,
            trips_km FLOAT NOT NULL,
        """)
    try:
        # read the connection parameters
        config = {'dbname': 'MITMA', 'user': 'julian', 'host': 'localhost', 'password': '1234'}
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        # create table
        for query in queries:
            cur.execute(query)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_mitma_trips_tables():
    """
    Creates a table with the incoming, outgoing and internal 
    trips for each MITMA region and another with the fluxes
    of each pair of MITMA regions. 
    """
    conn = None
    queries = (
        """
        CREATE TABLE mitma_trips (
            parameter_id SERIAL PRIMARY KEY,
            datetime timestamp default NULL,
            source VARCHAR(255) NOT NULL,
            trips_outcoming FLOAT NOT NULL,
            trips_incoming FLOAT NOT NULL,
            trips_internal FLOAT NOT NULL
        )
        """,
        """ CREATE TABLE mitma_trips_matrix (
                parameter_id SERIAL PRIMARY KEY,
                datetime timestamp default NULL,
                source VARCHAR(255) NOT NULL,
                destination VARCHAR(255) NOT NULL,
                trips FLOAT NOT NULL,
                )
        """)
    try:
        # read the connection parameters
        config = {'dbname': 'MITMA', 'user': 'julian', 'host': 'localhost', 'password': '1234'}
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        # create table one by one
        for query in queries:
            cur.execute(query)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()


def create_mitma_tables():
    """
    Creates a table with the q, r and p mobility indexes for each MITMA
    region and another with the fluxes of p index of each pair of MITMA
    regions. 
    """
    conn = None
    queries = (
        """
        CREATE TABLE mitma_qrp (
            parameter_id SERIAL PRIMARY KEY,
            datetime timestamp default NULL,
            source VARCHAR(255) NOT NULL,
            q FLOAT NOT NULL,
            r FLOAT NOT NULL,
            p FLOAT NOT NULL
        )
        """,
        """ CREATE TABLE mitma_flux (
                parameter_id SERIAL PRIMARY KEY,
                datetime timestamp default NULL,
                source VARCHAR(255) NOT NULL,
                target VARCHAR(255) NOT NULL,
                p FLOAT NOT NULL
                )
        """)
    try:
        # read the connection parameters
        config = {'dbname': 'MITMA', 'user': 'julian', 'host': 'localhost', 'password': '1234'}
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**config)
        cur = conn.cursor()
        # create table one by one
        for query in queries:
            cur.execute(query)
        # close communication with the PostgreSQL database server
        cur.close()
        # commit the changes
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()



##########################
###### Query tables ######
##########################

def query_parameters_cat(query, mitma_layers):
    """
    Performs a query using the mitma layers list to filter MITMA
        regions.
    Args:
        query (string): sql query with a 'parameter_array' variable to 
                        filter the query
        mitma_layers (list of str): MITMA zones to filter the query
    Returns:
        dataframe: result of query
    """
    try:
        # read the connection parameters
        config = {'dbname': 'MITMA', 'user': 'julian', 'host': 'localhost', 'password': '1234'}
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        # create table one by one
        cursor.execute(query, {"parameter_array": list(mitma_layers)})
        # close communication with the PostgreSQL database server
        # records = cursor.fetchall()
        df = pd.DataFrame(cursor.fetchall())
        df.columns = [x.name for x in cursor.description]

        cursor.close()
        # commit the changes
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return df


def query_no_parameters(query):
    """
    Performs an sql query.
    Args:
        query (string): sql query
    Returns:
        dataframe: result of query
    """

    try:
        # read the connection parameters
        config = {'dbname': 'MITMA', 'user': 'julian', 'host': 'localhost', 'password': '1234'}
        # connect to the PostgreSQL server
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        # create table one by one
        cursor.execute(query)
        # close communication with the PostgreSQL database server
        # records = cursor.fetchall()
        df = pd.DataFrame(cursor.fetchall())
        df.columns = [x.name for x in cursor.description]

        cursor.close()
        # commit the changes
        conn.commit()

    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()

    return df

