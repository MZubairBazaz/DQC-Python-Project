import pandas as pd
import numpy as np
import math as mt
import common_functions as comm_func
from common_classes import Conditional
import constant_variables as const_var
import os
import glob
from mpf_trad_schema import prophet_output_trad_schema, prophet_par_index_output_trad_schema, python_par_index_output_schema, python_output_schema
from time import time
import pytz
from datetime import datetime
from datetime import timedelta
import csv
from common_data_reconciliation import Data_Reconciliation

# Instantiate the class for validation
data_rec = Data_Reconciliation()

# MPF Generation
@comm_func.timer_func
def output_to_file(poldata_df, output_path, val_filename, phase):
    """
    This method outputs MPFs by product.
    """
    # Set output schema
    if (phase == 'PAR_COHORT_INDEX'):
        schema = prophet_par_index_output_trad_schema()
        python_schema = python_par_index_output_schema()
    else:
        schema = prophet_output_trad_schema()
        python_schema = python_output_schema()
    columns = list(schema.keys())

    # Create new field to separate IF and NB
    poldata_df = poldata_df.copy()
    poldata_df['POL_NUMBER'] = poldata_df['POL_NUMBER'].astype(int)
    poldata_df['IFNB'] = np.where(poldata_df['SPCODE'] < 51, 'IF', 'NB')

    # For output of IFNB together
    for (prod, val_type), group in poldata_df.groupby(['PRODUCT_NAME', 'VAL_TYPE']):
        filtered_df = group[columns].copy()
        filtered_df = filtered_df.sort_values(by = ['SPCODE', 'POL_NUMBER'])
        filtered_df.insert(0, '!', '*')

        # Add double quotes for string variables
        # double_quote_list = ['PROD_CD', 'BENEFIT_CODE', 'POL_NO_IFRS17']
        for column in columns:
            if schema[column] == 'T255':
                filtered_df[column] = filtered_df[column].replace('"', '')
                filtered_df[column] = '"' + filtered_df[column].astype(str) + '"'
            if ((schema[column] == 'N') & (filtered_df[column].dtypes == 'float64')):
                filtered_df[column] = ['%.16g' % n for n in filtered_df[column]]

        # Append top few rows
        firstrow = pd.DataFrame({"A": ['VAL DCS#']})
        secondrow =pd.DataFrame({"A": ["Output_Format"],
                                 "B": ["C Library#"]
                                 })
        thirdrow = pd.DataFrame({"A": ["NUMLINES"],
                                 "B": [str(len(filtered_df)) + "#"]
                                 })

        # Create output folder if does not exist
        os.makedirs(output_path + "IFNB/" + val_type, exist_ok=True)

        path = output_path + "IFNB/" + f"{val_type}/" + f"{prod}.PRO"

        # Output first few rows
        firstrow.to_csv(path, header=False, index=False)
        secondrow.to_csv(path, mode='a',header=False, index=False)
        thirdrow.to_csv(path, mode='a',header=False, index=False)

        var_type_row = pd.DataFrame({'!': ['VARIABLE_TYPES'], **{k: [v] for k, v in schema.items()}}, index=[0])
        var_type_row.to_csv(path, mode='a',header=False, index=False)
        
        # Output data
        filtered_df.to_csv(path, mode='a', index=False, quoting = csv.QUOTE_NONE) 

    # For output of IF and NB separately
    for (prod, val_type, ifnb), group in poldata_df.groupby(['PRODUCT_NAME', 'VAL_TYPE', 'IFNB']):
        filtered_df = group[columns].copy()
        filtered_df = filtered_df.sort_values(by = ['SPCODE', 'POL_NUMBER'])
        filtered_df.insert(0, '!', '*')

        # Add double quotes for string variables
        # double_quote_list = ['PROD_CD', 'BENEFIT_CODE', 'POL_NO_IFRS17']
        for column in columns:
            if schema[column] == 'T255':
                filtered_df[column] = filtered_df[column].replace('"', '')
                filtered_df[column] = '"' + filtered_df[column].astype(str) + '"'
            if ((schema[column] == 'N') & (filtered_df[column].dtypes == 'float64')):
                filtered_df[column] = ['%.16g' % n for n in filtered_df[column]]

        # Append top few rows
        firstrow = pd.DataFrame({"A": ['VAL DCS#']})
        secondrow =pd.DataFrame({"A": ["Output_Format"],
                                 "B": ["C Library#"]
                                 })
        thirdrow = pd.DataFrame({"A": ["NUMLINES"],
                                 "B": [str(len(filtered_df)) + "#"]
                                 })

        # Create output folder if does not exist
        os.makedirs(output_path + f"{ifnb}/" + val_type, exist_ok=True)

        path = output_path + f"{ifnb}/" + f"{val_type}/" + f"{prod}.PRO"

        # Output first few rows
        firstrow.to_csv(path, header=False, index=False)
        secondrow.to_csv(path, mode='a',header=False, index=False)
        thirdrow.to_csv(path, mode='a',header=False, index=False)

        var_type_row = pd.DataFrame({'!': ['VARIABLE_TYPES'], **{k: [v] for k, v in schema.items()}}, index=[0])
        var_type_row.to_csv(path, mode='a',header=False, index=False)

        # Output data
        filtered_df.to_csv(path, mode='a', index=False, quoting = csv.QUOTE_NONE)  

        # Validation table
        data_rec.add_row_agg_prod_lvl(filtered_df, prod, val_type)   
    
    # Output validation table
    data_rec.output_val_table_append(val_filename)
    data_rec.clear_val_table()

# Output Summary file
@comm_func.timer_func
def output_summary(poldata_df):

    poldata_df = poldata_df.copy()
    poldata_df['RECORD_MPF'] = 1

    GroupByList = ['VAL_TYPE', 'SPCODE', 'PRODUCT_NAME']
    SumList = const_var.summary_list()
    summary_df = poldata_df.groupby(GroupByList, dropna = False)[SumList].agg('sum').reset_index()
    summary_df = summary_df.sort_values(by = ['VAL_TYPE', 'SPCODE', 'PRODUCT_NAME'])

    return summary_df

# Output Summary file for PAR_COHORT_INDEX
@comm_func.timer_func
def output_summary_par_index(poldata_df_par_index):

    poldata_df_par_index = poldata_df_par_index.copy()
    poldata_df_par_index['RECORD_MPF'] = 1

    GroupByList = ['VAL_TYPE', 'SPCODE', 'PRODUCT_NAME']
    SumList = const_var.summary_par_index_list()
    summary_df_par_index = poldata_df_par_index.groupby(GroupByList, dropna = False)[SumList].agg('sum').reset_index()
    summary_df_par_index = summary_df_par_index.sort_values(by = ['VAL_TYPE', 'SPCODE', 'PRODUCT_NAME'])

    return summary_df_par_index

# Output Summary file for ETL2A
@comm_func.timer_func
def output_summary_etl2a(poldata_df_AS):

    poldata_df_AS = poldata_df_AS.copy()
    poldata_df_AS['RECORD_MPF'] = 1

    GroupByList = ['VAL_TYPE', 'SPCODE', 'PRODUCT_NAME']
    SumList_AS = const_var.summary_list()
    summary_df_as = poldata_df_AS.groupby(GroupByList, dropna = False)[SumList_AS].agg('sum').reset_index()
    summary_df_as = summary_df_as.sort_values(by = ['VAL_TYPE', 'SPCODE', 'PRODUCT_NAME'])

    return summary_df_as 

