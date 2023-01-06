"""
Script that computes and stores mitma_qrp and mitma_flux tables. 
The first one includes the mobility indexes q, r and p for each MITMA
region and the second the p mobility index fluxes between two regions
"""

# Imports
import os
import pandas as pd
import numpy as np
import psycopg2
from sqlalchemy import create_engine
import pandas as pd

from datetime import datetime, timedelta
from mobility_parameters import *
from database_utils import *

# Paths
dir_maestra_1 = r'data_1TB/MITMA/maestra_1_mitma_distritos/'
dir_maestra_2 = r'data_1TB/MITMA/maestra_2_mitma_distritos/'
filename_maestra_1 = r'_maestra_1_mitma_distrito.txt.gz'
filename_maestra_2 = r'_maestra_2_mitma_distrito.txt.gz'


def get_last_datetime():
    """
    Gets last datetime in mitma_trips table
    """

    conn = None
    queries = (
        """
        SELECT datetime FROM mitma_qrp ORDER BY datetime DESC LIMIT 1
        """
    ,)
    
    try:
        # Read the connection parameters
        config = {'dbname': 'MITMA', 'user': 'julian', 'host': 'localhost', 'password': '1234'}
        conn = psycopg2.connect(**config)
        cursor = conn.cursor()
        # Execute queries
        dates = []
        for query in queries:
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


####################################
###### q,r,p mobility indexes ######
####################################    

def compute_q(df_population):
    """
    Compute the following mobility parameters:
    q: ratio of population that is not doing any trip
    nq: ratio of population that is doing 1, 2 or more than 2 trips

    Args:
        df_population (dataframe): with the number of trips and users for each patch

    Returns:
        dataframe: with q and nq mobility indexes for each source MITMA region
    """
    q_list = []  # ratio of population is not moving at all (no trips)
    nq_list = []  # ratio of population is doing some trip
    patchs = df_population.distrito.unique()

    for patch in patchs:
        viajes_q = df_population[
            (df_population.distrito == patch) & (df_population.numero_viajes == '0')].personas.sum()
        viajes_nq = df_population[
            (df_population.distrito == patch) & (df_population.numero_viajes != '0')].personas.sum()
        q_list.append(viajes_q / (viajes_q + viajes_nq))
        nq_list.append(1 - (viajes_q / (viajes_q + viajes_nq)))

    df_q = pd.DataFrame({'source': patchs, 'q': q_list, 'nq': nq_list})

    return df_q


def compute_qrp(df_population, df_matrix_daily):
    """
    Compute the following parameters:
        q: ratio of population that is not doing any trip
        r: ratio of population is moving from the same origin/destination patch
        p: ration of population is moving from a particular patch to a different patch

    Args:
        df_population (dataframe): with the number of trips and users for each patch
        df_matrix_daily (dataframe): 

    Returns:
        dataframe: with the 3 mobility indexes for each MITMA region
    """
    q_list = []  # population ratio is not moving at all
    r_list = []  # population ration is moving in the same origin/destination area
    p_list = []  # population ratio is moving from their our area to other areas
    source = []
    patchs = df_population.distrito.unique()

    for patch in patchs:

        viajes_r = 0
        viajes_p = 0
        source.append(patch)

        # Compute q
        viajes_q = df_population[
            (df_population.distrito == patch) & (df_population.numero_viajes == '0')].personas.sum()
        viajes_nq = df_population[
            (df_population.distrito == patch) & (df_population.numero_viajes != '0')].personas.sum()
        q = viajes_q / (viajes_q + viajes_nq)
        q_list.append(q)

        # Compute nq
        nq = 1 - q

        # Compute r
        if True in set((df_matrix_daily['origen'] == patch) & (df_matrix_daily['destino'] == patch)):
            viajes_r = df_matrix_daily[
                (df_matrix_daily['origen'] == patch) & (df_matrix_daily['destino'] == patch)].viajes.sum()

        # Compute p
        if True in set((df_matrix_daily['origen'] == patch) & (df_matrix_daily['destino'] != patch)):
            viajes_p = df_matrix_daily[
                (df_matrix_daily['origen'] == patch) & (df_matrix_daily['destino'] != patch)].viajes.sum()

        # Add indexes
        try:
            r = viajes_r / (viajes_r + viajes_p)
            r_list.append(r * nq)
            p = 1 - r
            p_list.append(p * nq)
        except:
            if viajes_r == 0:
                r_list.append(0)
                p_list.append(nq)
            if viajes_p == 0:
                r_list.append(nq)
                p_list.append(0)

    df = pd.DataFrame({'source': source, 'q': q_list, 'r': r_list, 'p': p_list})

    return df


######################
###### p fluxes ######
######################

def compute_flux(df_population, df_matrix_daily):
    """
    Computes mobility fluxes between different MITMA regions

    Args:
        df_population (dataframe): with the number of trips and users for
            each patch
        df_matrix_daily (dataframe): 

    Returns:
        dataframe: with the p mobility index fluxes for each pair of
            MITMA region.
    """
    
    p_list = []  # ratio of population moving between patches
    source_list = []  # source patch
    target_list = []  # target patch

    patchs = df_population.distrito.unique()

    for patch in patchs:

        viajes_p = 0

        df_tmp = pd.DataFrame()

        if (True in set((df_matrix_daily['origen'] == patch) & (df_matrix_daily['destino'] != patch))):
            viajes_p = df_matrix_daily[
                (df_matrix_daily['origen'] == patch) & (df_matrix_daily['destino'] != patch)].viajes.sum()

            df_tmp = df_matrix_daily[(df_matrix_daily['origen'] == patch) & (df_matrix_daily['destino'] != patch)]
            df_tmp = df_tmp.assign(p=(df_tmp.viajes / viajes_p).values)

        if not df_tmp.empty:
            source_list.extend(df_tmp['origen'].tolist())
            target_list.extend(df_tmp['destino'].tolist())
            p_list.extend(df_tmp['p'].tolist())

    df = pd.DataFrame({'source': source_list, 'target': target_list, 'p': p_list})

    return df


def compute_parameters(first_date, last_date = None):
    """
    Computes and stores mitma_trips_matrix table. 
    It contains the sum of trips for each pair of MITMA regions.

    Args:
        first_date (str): to start the computation in this date
        last_date (str, optional): to finish the computation in this 
            date. Defaults to None.
    """
    compute_day = False
    engine = create_engine('postgresql://julian:1234@127.0.0.1:5432/MITMA')

    print('Create dbs')
    create_mitma_tables()

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
            df_matrix_pop = pd.read_csv(os.path.join(dir_maestra_2, filename.split('_')[0] + filename_maestra_2), sep='|', dtype={'distrito': str})

            # Filtering getting data from home and agreggated by day
            df_matrix_from_home = df_matrix[(df_matrix.actividad_origen=='casa')].groupby(['origen', 'destino']).agg({'viajes': sum}).reset_index()

            print('Computing qrp parameters...')
            # qrp
            df_qrp_from_home = compute_qrp(df_matrix_pop, df_matrix_from_home)
            df_qrp_from_home['datetime'] = datetime.strptime(filename.split('_')[0], '%Y%m%d')
            print('Saving qrp to sql...')
            df_qrp_from_home.to_sql('mitma_qrp', con=engine, if_exists='append', index=False)

            # flux
            print('Computing flux...')
            df_flux_from_home = compute_flux(df_matrix_pop, df_matrix_from_home)
            df_flux_from_home['datetime'] = datetime.strptime(filename.split('_')[0], '%Y%m%d')
            print('Saving flux to sql...')
            df_flux_from_home.to_sql('mitma_flux', con=engine, if_exists='append', index=False)

        else:
            continue

    print('End processing')

# Execution
dates = get_last_datetime()
first_date = (dates[1] + timedelta(days=1)).strftime("%Y%m%d")
print(first_date)
compute_parameters(first_date, last_date = None)

    
