# Import Python Packages
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta as rd
import os

# Import Python Packages for Timer Function
from time import time
import pytz
from datetime import datetime
from datetime import timedelta
 
# Export a dataframe as a csv file
def export_df_to_csv(df, csv_file_name):
    df.to_csv(csv_file_name, encoding='utf-8', index=False)
 
# Define a generic form to add a new column to the poldata_df based on an existing valuation extract field without changes
def add_col_valex(poldata_df, target_col_name, source_col_name):
    poldata_df[target_col_name] = poldata_df[source_col_name]

    return poldata_df

#  Add 'MONTH' column to the poldata_df based on an existing dataframe date column
def add_col_month(poldata_df, target_col_name, date_col_name):   
    poldata_df[target_col_name] = pd.to_datetime(poldata_df[date_col_name]).dt.month
    
    return poldata_df

#  Add 'YEAR' column to the poldata_df based on an existing dataframe date column
def add_col_year(poldata_df, target_col_name, date_col_name):   
    poldata_df[date_col_name] = pd.to_datetime(poldata_df[date_col_name])
    poldata_df[target_col_name] = poldata_df[date_col_name].dt.year
    
    return poldata_df

#  Add 'DATE' column to the poldata_df based on an existing dataframe date column
def add_col_day(poldata_df, target_col_name, date_col_name):   
    poldata_df[target_col_name] = pd.to_datetime(poldata_df[date_col_name]).dt.day
    
    return poldata_df

# Apply rounding on data
def apply_rounding_on_col(poldata_df, target_col_name, decimal_place):
   
    precision = 10 ** decimal_place
 
    poldata_df[target_col_name] = np.where(
        ((poldata_df[target_col_name].astype(float)*precision % 1) >=0.5),
        (poldata_df[target_col_name] * precision).apply(np.ceil) / precision,
        round(poldata_df[target_col_name], decimal_place))
    
    return poldata_df[target_col_name]

# Default a specified column to nil
def default_col_null_to_zero(poldata_df, col_name):
    poldata_df[col_name] = poldata_df[col_name].fillna(0)

    return poldata_df

# Default a specified column to specific string
def default_col_null_to_string_or_col(poldata_df, col_name, default_string):
    poldata_df[col_name] = poldata_df[col_name].fillna(default_string)

    return poldata_df

# Drop a soecified column (as a list)
def drop_col(poldata_df, target_col_name):
    # Drops 'target_col_name' column from the poldata_df
    poldata_df = poldata_df.drop(target_col_name, axis = 1)
    
    return poldata_df

# Add a column to the dataframe based on a specified groupby transformation
def add_groupby_col(poldata_df, groupby_key_list, target_col_name, new_col_name, agg_func):
    # agg_func can be 'sum', 'max', 'min', 'mean' 
    group_by_df = poldata_df.groupby(groupby_key_list)[target_col_name].transform(agg_func)
    poldata_df[new_col_name] = group_by_df
    
    return poldata_df

# Merge two tables and output columns
def add_merge_col(poldata_df, tbl_df, output_col, left_key, right_key, join_type):
    poldata_df = poldata_df.merge(tbl_df[right_key+output_col], left_on = left_key, right_on = right_key, how=join_type)

    return poldata_df

def add_conditional_column_to_df(df, new_column_name, conditions, results, default = None):
    '''

    Function to add a new column to dataframe based on a condition (as a list) and results.
    Inputs:
    pol_data_df -> df, Input dataframe
    new-column_name -> Name of column to be added
    condition -> Conditional statement, must reference df!
    value -> Result value based on output
    default -> Value if condition is not true

    Return:
    poldata_df with populated new column
    '''

    # poldata_df[new_column_name] = default
    # poldata_df.loc[condition, new_column_name] = value

    df[new_column_name] = np.select(conditions, results, default)
    return df

def add_column_via_assignment(poldata_df, new_column_name, value_expression):
    '''
    Function to be used when adding new column based on non conditional assignment.  Can be used with expressions rather than just simple column assignment.
 
    pol_data_df -> df, Input dataframe
    new_column_name -> Name of column to be added
    value_expression -> Expression containing the value assignment
 
    Return:
    pol_data_df with populated enw column
    '''
    #Instantiate the column
    poldata_df[new_column_name] = value_expression
    return poldata_df
 
def case_conditional_column_assignment(poldata_df, new_column_name, list_of_conditions, default=None):
    poldata_df[new_column_name] = default
    for cond in list_of_conditions:
        poldata_df.loc[cond.condition, new_column_name] = cond.value
    return poldata_df

def input_filter_contract_type(CNTTYPE_input_switch, poldata_csv, LOB):
    CNTTYPE_input_switch = CNTTYPE_input_switch[(CNTTYPE_input_switch['RUN_IND'] == 'Y') & (CNTTYPE_input_switch['LOB'] == LOB)]
    poldata_csv = poldata_csv[poldata_csv['CNTTYPE'].isin(CNTTYPE_input_switch['CNTTYPE'])]
    return poldata_csv

def output_filter_product(prod_output_switch, poldata_df_mpf, LOB):
    prod_output_switch = prod_output_switch[(prod_output_switch['OUTPUT_MPF'] == 'Y') & (prod_output_switch['LOB'] == LOB)]
    poldatpoldata_df_mpf = poldata_df_mpf[poldata_df_mpf['PRODUCT_NAME'].isin(prod_output_switch['PRODUCT_NAME'])]
    return poldata_df_mpf

def return_groupby_df(input_df, groupby_key, groupby_agg_funcs, groupby_rename_list):
    groupby_df = input_df.groupby(groupby_key).agg(groupby_agg_funcs).reset_index()
    groupby_df = groupby_df.rename(columns=groupby_rename_list)

    return groupby_df

# This function shows the execution time of the function object passed. 
# It is applied as wrapper/deocrator function to all other functions below.
def timer_func(func):
    def wrap_func(*args, **kwargs):
       
        t1 = time()
        result = func(*args, **kwargs)
        t2 = time()
       
        fmt = '%Y-%m-%d %H:%M:%S'
        # fmt = '%Y-%m-%d %H:%M:%S %Z%z'
        server_t1 = datetime.fromtimestamp(t1)
        server_t2 = datetime.fromtimestamp(t2)
               
        local = 'Hongkong' #to be parametrized later
       
        local_t1 = server_t1.astimezone(pytz.timezone(local)).strftime(fmt)
        local_t2 = server_t2.astimezone(pytz.timezone(local)).strftime(fmt)
        time_elapsed = str(timedelta(seconds=t2-t1)).split(':')
        if time_elapsed[2][:-7] == '':
            time_elapsed_secs = '00'
        else:
            time_elapsed_secs = time_elapsed[2][:-7]
             
        # print(f'{local_t1}: Function {func.__name__!r} START')
        print(f'{local_t2}: Time elapsed: {time_elapsed[0]} Hours {time_elapsed[1]} Minutes {time_elapsed_secs} Seconds. Function {func.__name__!r} END')
         
        return result
    return wrap_func
