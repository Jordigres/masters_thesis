"""
Contains list of locations, phases and motivs to perform the notebook's
studies. Also, there are functions to allow easy queries to the MITMA 
tables. 
"""

# Imports
import numpy as np
import pandas as pd
import geopandas as gpd

from src.database_utils import *


############################################################### 
########################### Context ###########################
###############################################################

# Catalunya's province postal codes
bcn_prov = '08'
girona_prov = '17'
tarraco_prov = '43'
lleida_prov = '25'

# Postal codes and strings
location_info = { 
    'cat': [None, 'Catalunya'],
    'bcn': [bcn_prov, 'Barcelona'], 
    'girona': [girona_prov, 'Girona'], 
    'tarraco': [tarraco_prov, 'Tarragona'], 
    'lleida': [lleida_prov, 'Lleida']}

# List of postal codes and strings
province_group = list(location_info.items())

# Metric strings for the plot labels
metric_strings = {
    'total': 'Incoming + Outcoming + Stay',
    'm': 'Move',
    'q': 'Don\'t move', 'r': 'Move in the area', 'p': 'Move to other areas', }

# Phases and motiv of study
phases_list = [
    {'precovid':        {'start':'2020-02-14', 'end':'2020-03-14'}},
    {'lockdown':        {'start':'2020-03-15', 'end':'2020-03-29'}},
    {'mobilitat_essenc':{'start':'2020-03-30', 'end':'2020-04-09'}},
    {'fase_0':          {'start':'2020-04-10', 'end':'2020-05-10'}},
    {'desescalada':     {'start':'2020-05-11', 'end':'2020-06-18'}},
    {'no_restriccions': {'start':'2020-06-19', 'end':'2020-10-24'}},
    {'alerta_5_inici':  {'start':'2020-10-25', 'end':'2020-11-22'}},
    {'alerta_5_tr1':    {'start':'2020-11-23', 'end':'2020-12-13'}},
    {'alerta_5_tr1_2':  {'start':'2020-12-14', 'end':'2020-12-20'}},
    {'nadal':           {'start':'2020-12-21', 'end':'2020-12-31'}}
    ]

motiv_list = [
    {'baseline': {'start':'2020-02-17', 'end':'2020-03-01'}},
    {'summer':   {'start':'2020-07-01', 'end':'2020-08-31'}},
    {'schools':  {'start':'2020-09-14', 'end':'2020-09-20'}},
    {'clousure_weekend1': {'start':'2020-10-30', 'end':'2020-11-01'}},
    {'clousure_weekend2': {'start':'2020-11-06', 'end':'2020-11-08'}},
    {'clousure_weekend3': {'start':'2020-11-13', 'end':'2020-11-15'}}
    ]

# Load Catalunya MITMA layers
df_mitma_abs_overlap = pd.read_csv('data/raw/overlap_mitma_abs.csv')
mitma_layers_cat = df_mitma_abs_overlap.m_id.unique()
mitma_layers_cat = [str(i).zfill(3) for i in mitma_layers_cat]

############################################################### 
##################### Query and add data ######################
###############################################################


def query_raw_data_or_trips_or_flux_matrix_between_dates(
    table, date1=None, date2=None, municipality=None, province=None, 
    municipalities_groups=None, province_groups=None):
    """
    Queries data to mitma_raw_data, mitma_trips_matrix, or mitma_flux 
    tables. It can be filtered by location and by date.

    Args:
        table (str): mitma_cat_raw, mitma_trips_matrix or mitma_flux
        date1 (str, optional): start date in format %Y-%m-%d. Defaults 
            to None.
        date2 (str, optional): end date in format %Y-%m-%d. Defaults 
            to None.
        municipality (str, optional): MITMA code of a municipality to 
            filter the query. Defaults to None.
        province (str, optional): MITMA codes of a province to filter
            the query. Defaults to None.
        municipalities_groups (list of str, optional): MITMA codes of 
            multiple municipalities to filter the query. Defaults to None.
        province_groups (list of str, optional): MITMA codes of multiple
            provinces to filter the query. Defaults to None.

    Returns:
        dataframe: query result.
    """
    df = pd.DataFrame()

    # Query MITMA data
    if date1 is not None:
        print(f"Querying {table} data from {date1} to {date2}")
        query = f"SELECT * FROM {table} \
                  WHERE source = ANY(%(parameter_array)s) \
                  AND datetime between '{date1}' and '{date2}'"
    else:
        print(f"Querying all {table} data")
        query = f"SELECT * FROM {table} \
                  WHERE source = ANY(%(parameter_array)s)"

    df_query = query_parameters_cat(query, mitma_layers_cat)
    
    # Filter data by location postal code
    if municipality is not None:
        df = df.append(df_query[
            (df_query['source'].str[:5] == municipality) & 
            (df_query['target'].str[:5] == municipality)])    
    elif province is not None:
        df = df.append(df_query[
            (df_query['source'].str[:2] == province) | 
            (df_query['target'].str[:2] == province)])
    elif municipalities_groups is not None:
        for postal_code in municipalities_groups:
            df = df.append(df_query[
                (df_query['source'] == postal_code) | 
                (df_query['target'] == postal_code)])
    elif province_groups is not None:
        for mg in province_groups:
            postal_code = mg[1][0]
            df = df.append(df_query[
                (df_query['source'].str[:2] == postal_code) | 
                (df_query['target'].str[:2] == postal_code)])
    else:
        print('Set city or province')

    # Set datetime and drop column
    if table == 'mitma_cat_raw':
        df['datetime-hour'] = pd.to_datetime(
            df['datetime'].astype(str) + ' ' + df['hour'].astype(
                str).str.zfill(2), format='%Y%m%d %H')
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d')
    else:
        df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d')
        df.drop(['parameter_id'], axis=1, inplace=True)
    
    return df


def query_trips_iio_or_qrp_between_dates(
    table, date1=None, date2=None, municipality=None, 
    province=None, municipalities_groups=None, province_groups=None):
    """
    Queries data to mitma_raw_data, mitma_trips_matrix, or mitma_flux 
    tables. It can be filtered by location and by date.

    Args:
        table (str): mitma_cat_raw, mitma_trips_matrix or mitma_flux
        date1 (str, optional): start date in format %Y-%m-%d. Defaults 
            to None.
        date2 (str, optional): end date in format %Y-%m-%d. Defaults 
            to None.
        municipality (str, optional): MITMA code of a municipality to 
            filter the query. Defaults to None.
        province (str, optional): MITMA codes of a province to filter
            the query. Defaults to None.
        municipalities_groups (list of str, optional): MITMA codes of 
            multiple municipalities to filter the query. Defaults to None.
        province_groups (list of str, optional): MITMA codes of multiple
            provinces to filter the query. Defaults to None.

    Returns:
        dataframe: query result.
    """       
    df = pd.DataFrame()

    # Query MITMA data
    if date1 is not None:
        print(f"Querying {table} data from {date1} to {date2}")
        query = f"SELECT * FROM {table} \
                  WHERE source = ANY(%(parameter_array)s) \
                  AND datetime between '{date1}' and '{date2}' \
                  ORDER BY datetime ASC"
    else:
        print(f"Querying all {table} data")
        query = f"SELECT * FROM {table} \
                  WHERE source = ANY(%(parameter_array)s) \
                  ORDER BY datetime ASC"

    df_query = query_parameters_cat(query, mitma_layers_cat)
    
    # Filter data by location postal code
    if municipality is not None:
        df = df.append(df_query[df_query['source'].str[:5] == municipality])  
    elif province is not None:
        df = df.append(df_query[df_query['source'].str[:2] == province])
    elif municipalities_groups is not None:
        for postal_code in municipalities_groups:
            df = df.append(df_query[df_query['source'] == postal_code])
    elif province_groups is not None:
        for mg in province_groups:
            postal_code = mg[1][0]
            df = df.append(df_query[df_query['source'].str[:2] == postal_code])
    else:
        print('Set city or province')
    
    # Set datetime column
    df['datetime'] = pd.to_datetime(df['datetime'], format='%Y-%m-%d')

    # Reorder columns and add new ones
    if table == 'mitma_trips':
        df = df[['datetime', 'source', 'trips_internal', 'trips_incoming',
            'trips_outcoming']]
        df['total'] = df['trips_internal'] + df['trips_incoming'] + df[
            'trips_outcoming']
        df.columns = ['datetime', 'source', 'internal', 'incoming', 
            'outcoming', 'total']
    else:
        df = df.drop(columns=['parameter_id'])
        df['m'] = 1 - df['q']

        # Set datetime
        df.set_index('datetime', inplace=True)
        df['weekday'] = df.index.weekday

        # Add geopandas data to each source
        # Filter Catalunya MITMA zones
        mitma_gpd = gpd.read_file('../../data_1TB/MITMA/zonificación/distritos_mitma.shp')
        mitma_gpd_cat = mitma_gpd[
            (mitma_gpd['ID'].str[:2] == '25') | (mitma_gpd['ID'].str[:2] == '08') |
            (mitma_gpd['ID'].str[:2] == '17') | (mitma_gpd['ID'].str[:2] == '43')]

        # Join mqrp parameters with MITMA zones
        df = mitma_gpd.set_index('ID').join(df.reset_index().set_index(
            'source')).reset_index()
        df.rename(columns = {'index':'source'}, inplace = True)
        df = df.dropna()

        # Reorder columns
        df = df[['datetime', 'source', 'weekday', 'q', 'r', 'p', 'm', 'geometry']]
        df = df.set_index('datetime')
        
    return df


def add_phases_and_motivs(df, phases_list, motiv_list):
    """
    Adds a phase and a motiv columns to a mitma_qrp dataframe

    Args:
        df (dataframe): which corresponds to the result of a mitma_qrp table
        phases_list (list of dict): dict with format 
            {'name':{'start':'%Y-%m-%d','end':'%Y-%m-%d'}}
        motiv_list (list of dict): dict with format 
            {'name':{'start':'%Y-%m-%d','end':'%Y-%m-%d'}}

    Returns:
        dataframe: mitma_qrp dataframe with phase and motiv columns
    """
    # Geopandas dataframe to pandas dataframe to allow datetime comparisons
    if 'geometry' in df.columns:
        df_geometry = df['geometry']
        df = df[['source', 'weekday', 'q', 'r', 'p', 'm']]

    # Add columns
    df['phase'], df['motiv']  = '', ''

    # Add phases according to dates
    for phase in phases_list:
        for phase_name, phase_dates in phase.items():
            start, end = phase_dates['start'], phase_dates['end'] 
            df.loc[(df.index >= start) & (df.index <= end), 'phase'] = phase_name

    # Add motivs according to dates
    for motiv in motiv_list:
        for motiv_name, motiv_dates in motiv.items():
            start, end = motiv_dates['start'], motiv_dates['end'] 
            df.loc[(df.index >= start) & (df.index <= end), 'motiv'] = motiv_name

    # Add geopandas column again
    df['geometry'] = df_geometry
    
    # Reorder columns
    df = df[['source', 'weekday', 'phase', 'motiv', 'q', 'r', 'p', 'm', 'geometry']]
    
    return df


def compute_fluxes_by_area(df, by_hour=False):
    """
    Computes trip fluxes (incoming, outgoing and internal) from a 
    mitma_raw_cat query dataframe.

    Args:
        df (dataframe): which corresponds to the result of a mitma_raw_cat
            table
        by_hour (bool, optional): True returns fluxes grouped hourly, 
            false daily. Defaults to False.

    Returns:
        set(dataframe): set of 4 dataframes. Internal trips, outcoming trips, 
            incoming trips and a last daframe with all three merged.  
    """
    
    if by_hour:
        internal = df[df.target == df.source].groupby(
            ['datetime', 'source', 'target', 'hour']).sum().reset_index()
        outcoming = df.groupby(['datetime', 'hour', 'source']).sum().reset_index()
        incoming = df.groupby(['datetime', 'hour', 'target']).sum().reset_index()
        
        merged = outcoming.merge(
            incoming, left_on=['datetime', 'source', 'hour'], 
            right_on=['datetime', 'target', 'hour'], how='outer')[[
                'datetime', 'hour', 'source', 'target', 'trips_x', 'trips_y']]

        merged.source.fillna(merged.target, inplace=True)
        merged.drop(columns='target', inplace=True)
        merged.rename(
            columns={'source':'patch', 'trips_x': 'outcoming', 'trips_y':'incoming'},
            inplace=True)
        
        merged = merged.merge(
            internal, left_on = ['datetime', 'patch', 'hour'], 
            right_on=['datetime', 'source', 'hour'], how='outer')[[
                'datetime', 'hour', 'patch', 'target', 
                'trips', 'outcoming', 'incoming']]
        
    else:
        internal = df[df.target == df.source].groupby(
            ['datetime', 'source', 'target']).sum().reset_index()
        outcoming = df.groupby(['datetime', 'source']).sum().reset_index()
        incoming = df.groupby(['datetime', 'target']).sum().reset_index()

        merged = outcoming.merge(
            incoming, left_on=['datetime', 'source'], 
            right_on=['datetime', 'target'], how='outer')[[
                'datetime', 'source', 'target', 'trips_x', 'trips_y']]

        merged.source.fillna(merged.target, inplace=True)
        merged.drop(columns='target', inplace=True)
        merged.rename(
            columns={'source':'patch', 'trips_x': 'outcoming', 'trips_y':'incoming'},
            inplace=True)

        merged = merged.merge(
            internal, left_on = ['datetime', 'patch'], right_on=['datetime', 'source'], 
            how='outer')[['datetime', 'patch', 'target', 'trips', 'outcoming', 'incoming']]
        merged.drop(columns='target', inplace=True)

    merged = merged.fillna(0)

    # Set datetime format
    merged['datetime-hour'] = pd.to_datetime(
        merged['datetime'].astype(str) + ' ' + merged['hour'].astype(str).str.zfill(2),
        format='%Y%m%d %H')

    # Compute final columns
    merged['outcoming'] =  merged['outcoming'] - merged['trips']
    merged['incoming'] =  merged['incoming'] - merged['trips']
    merged.rename(columns={'trips':'internal'}, inplace=True)
    merged['total'] = merged['internal'] + merged['outcoming'] + merged['incoming'] 

    merged = merged.rename(columns={'patch': 'source'})
        
    return internal, outcoming, incoming, merged


def set_id(df):
    """
    Creates a column with the MITMA region corresponding to a 
    Barcelona's district. It must be used with Barcelona's open data. 

    Args:
        df (dataframe): Barcelona's open data with a 'codi districte' 
            columns 

    Returns:
        dataframe: df with a source column containing the corresponding
            MITMA region.
    """
    try:
        # 'Districte' with a capital letter at the begining
        try:
            df['Codi_Districte'] = df['Codi_Districte'].astype(str)
            df['source'] = '08019' + df['Codi_Districte'].str.zfill(2)
        except:
            pass

        # 'districte' without a capital letter at the begining
        try:
            df['Codi_districte'] = df['Codi_districte'].astype(str)
            df['source'] = '08019' + df['Codi_districte'].str.zfill(2)
        except:
            pass
    except:
        print("Please review district code column name. \nIt should Codi_Districte or Codi_districte")
    
    return df