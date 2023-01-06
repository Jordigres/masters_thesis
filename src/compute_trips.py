"""
Script that computes and stores the mitma_trips table. 
It includes the incoming, outgoing and internal trips for each MITMA 
region.
"""

# Imports
import os
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

from datetime import datetime, timedelta
from mobility_parameters import *
from database_utils import *

# Paths
dir_maestra_1 = r'data_1TB/MITMA/maestra_1_mitma_distritos/'
dir_maestra_2 = r'data_1TB/MITMA/maestra_2_mitma_distritos/'
# dir_maestra_1 = r'../../data_1TB/MITMA/maestra_1_mitma_distritos/'
# dir_maestra_2 = r'../../data_1TB/MITMA/maestra_2_mitma_distritos/'
filename_maestra_1 = r'_maestra_1_mitma_distrito.txt.gz'
filename_maestra_2 = r'_maestra_2_mitma_distrito.txt.gz'

def get_last_datetime():
    """
    Gets last datetime in mitma_trips table
    """
    conn = None
    querys = (
        """
        SELECT datetime FROM mitma_trips ORDER BY datetime DESC LIMIT 1
        """
    ,)
    
    try:
        # Read the connection parameters
        config = {'dbname': 'MITMA', 'user': 'julian', 'host': 'localhost', 'password': '1234'}
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        # Execute query
        dates = []
        for query in querys:
            cursor.execute(query)
        # Close communication with the PostgreSQL database server
            records = cursor.fetchall() 
            dates.append(records[0][0])
        cursor.close()
        # Commit the changes
        conn.commit()
    
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    
    finally:
        if conn is not None:
            conn.close()
        return dates
    

def compute_parameters_trips(first_date, last_date = None):
    """
    Computes and stores mitma_trips table. This is incoming, outgoing and
    internal trips for each MITMA region. 

    Args:
        first_date (str): to start the computation in this date
        last_date (str, optional): to finish the computation in this 
            date. Defaults to None.
    """
    compute_day = False
    engine = create_engine('postgresql://julian:1234@127.0.0.1:5432/MITMA')

    print('Create dbs')
    create_mitma_trips_tables()

    for filename in sorted(os.listdir(dir_maestra_1)):

        if filename.split('_')[0] == first_date:
            compute_day = True

        if filename.split('_')[0] == last_date:
            compute_day = False
            break

        if filename.endswith(".txt.gz") and compute_day:

            print('>- ' + filename.split('_')[0] + ' -<')
            print('Loading data...')
            df_matrix = pd.read_csv(os.path.join(dir_maestra_1, filename.split('_')[0] + filename_maestra_1), sep='|', dtype={'origen': str, 'destino': str})

            df_internal = df_matrix[df_matrix.destino == df_matrix.origen].groupby(['fecha', 'origen', 'destino']).sum().reset_index()[['fecha', 'origen', 'viajes']]
            df_outcoming = df_matrix[df_matrix.destino != df_matrix.origen].groupby(['fecha', 'origen']).sum().reset_index()[['fecha', 'origen', 'viajes']]
            df_incoming = df_matrix[df_matrix.destino != df_matrix.origen].groupby(['fecha', 'destino']).sum().reset_index()[['fecha', 'destino', 'viajes']]
            
            print('Merging data...')
            df_merged = df_outcoming.merge(df_incoming, left_on=['fecha', 'origen'], right_on=['fecha', 'destino'], 
                               how='outer', suffixes=['_outcoming', '_incoming'])
            
            df_merged = df_merged.merge(df_internal, left_on=['fecha', 'origen'], right_on=['fecha', 'origen'], 
                               how='outer')
            
            df_merged.rename(columns={'origen':'source', 'viajes_outcoming': 'trips_outcoming', 'viajes_incoming':'trips_incoming', 
                          'viajes':'trips_internal'}, inplace=True)
            
            
            df_merged['datetime'] = datetime.strptime(filename.split('_')[0], '%Y%m%d')
            df_merged.drop(columns=['destino', 'fecha'], inplace=True)
            df_merged = df_merged.fillna(0)
            
            print('Merging saving...')
            df_merged.to_sql('mitma_trips', con=engine, if_exists='append', index=False)
            
            print('Done...')


# Execution          
dates = get_last_datetime()
first_date = (dates[0] + timedelta(days=1)).strftime("%Y%m%d")
print(first_date)
compute_parameters_trips(first_date=first_date, last_date=None)

    
