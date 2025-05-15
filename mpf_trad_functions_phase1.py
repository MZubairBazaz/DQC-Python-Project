import pandas as pd
import numpy as np
import math as mt
import common_functions as comm_func
import constant_variables as const_var
# from time import time
import pytz
from datetime import datetime
from datetime import timedelta

# Add "RECORD_TO_KEEP" indicator of 1, to tag starting records from validation extract. 
# This field is updated to 0 if the record is dropped, or halved if the record is split.
@comm_func.timer_func
def add_col_record_to_keep(poldata_df):
    poldata_df['RECORD_TO_KEEP'] = 1

    return poldata_df

# Add 'FILE" column to the poldata_df
@comm_func.timer_func
def add_col_file(poldata_df):     
    conditions = [
        (poldata_df['FileName'].str[7:9] == const_var.IF),
        (poldata_df['FileName'].str[7:9] == const_var.ET),
        (poldata_df['FileName'].str[7:9] == const_var.EX)
    ]    
 
    results = [
         const_var.IF,  
         const_var.ET,  
         const_var.EX
    ]
    
    default = "NA"
    
    # poldata_df['FILE'] = np.select(conditions, results, default)
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'FILE', conditions, results, default)

    return poldata_df

# Add policy number 'POL_NUMBER' column to the poldata_df, based on the existing valuation extract field 'CHDRNUM'.
@comm_func.timer_func
def add_col_pol_number(poldata_df):
    poldata_df = comm_func.add_col_valex(poldata_df, 'POL_NUMBER', 'CHDRNUM')

    return poldata_df

# Add the exit month of the policy 'EXTMONTH' to the poldata_df, based on the month value from the existing valuation extract field date 'ZMOVEDTE'.
@comm_func.timer_func
def add_col_extmonth(poldata_df):
    poldata_df = comm_func.add_col_month(poldata_df, 'EXTMONTH', 'ZMOVEDTE')

    return poldata_df

# Add 'DUR1' column to the poldata_df.
@comm_func.timer_func
def add_col_dur1(poldata_df, val_date_const):
    val_month = val_date_const.month
    val_year = val_date_const.year

    conditions = [
        val_month < 6, 
        val_month >= 6
    ]

    results = [
        (val_year - 2005 - 1) * 100 + (6 + val_month),
        (val_year - 2005) * 100 + (val_month - 6)
    ]

    default = np.nan

    # poldata_df['DUR1'] = np.select(conditions, results, default)
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'DUR1', conditions, results, default)

    return poldata_df

# Add 'DUR2' column to the poldata_df
@comm_func.timer_func
def add_col_dur2(poldata_df, val_date_const):   
    val_month = val_date_const.month
    val_year = val_date_const.year
    
    conditions = [
        val_month < 6, 
        val_month >= 6
    ]

    results = [
        (val_year - 2009 - 1) * 100 + (6 + val_month), 
        (val_year - 2009) * 100 + (val_month - 6)
    ]

    default = np.nan

    # poldata_df['DUR2'] = np.select(conditions, results, default)    
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'DUR2', conditions, results, default)

    return poldata_df

# Add 'DURATIONIF_M' column to the poldata_df
@comm_func.timer_func
def add_col_durationif_m(poldata_df):
    conditions = [
        (poldata_df['FILE'] == const_var.ET),
        (poldata_df['FILE'] == const_var.IF),
        (poldata_df['FILE'] == const_var.EX)  
    ]

    results = [
        0,
        np.maximum(1, ((poldata_df['ZYYMMINF'] / 100).astype(int) * 12 + (poldata_df['ZYYMMINF'] % 100))).astype(int),
        ((poldata_df['ZYYMMINF'] / 100).astype(int) * 12 + (poldata_df['ZYYMMINF'] % 100)).astype(int)
    ]
    
    default = 0

    # poldata_df['DURATIONIF_M'] = np.select(conditions, results, default)
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'DURATIONIF_M', conditions, results, default)

    return poldata_df

# Add 'ENTRY_YEAR' column to the poldata_df
@comm_func.timer_func
def add_col_entry_year(poldata_df):
    poldata_df = comm_func.add_col_year(poldata_df, 'ENTRY_YEAR', 'CRRCD')

    return poldata_df

# Add 'ENTRY_MONTH' column to the poldata_df
@comm_func.timer_func
def add_col_entry_month(poldata_df):
    poldata_df = comm_func.add_col_month(poldata_df, 'ENTRY_MONTH', 'CRRCD')

    return poldata_df

# Add 'ISSUE_MONTH' column to the poldata_df
@comm_func.timer_func
def add_col_issue_month(poldata_df):

    poldata_df = comm_func.add_col_month(poldata_df, 'ISSUE_MONTH', 'HISSDTE')

    return poldata_df

# Add 'ISSUE_YEAR' column to the poldata_df
@comm_func.timer_func
def add_col_issue_year(poldata_df):

    poldata_df = comm_func.add_col_year(poldata_df, 'ISSUE_YEAR', 'HISSDTE')

    return poldata_df

# Add 'OCC_CLASS' column to the poldata_df
@comm_func.timer_func
def add_col_occ_class(poldata_df):
    poldata_df = comm_func.add_col_valex(poldata_df, 'OCC_CLASS', 'OCCUP')

    return poldata_df

# Add 'BEN_PLAN" column to the poldata_df
@comm_func.timer_func
def add_col_ben_plan(poldata_df):   
    poldata_df['BEN_PLAN'] = poldata_df['BENPLN']
    poldata_df = comm_func.default_col_null_to_string_or_col(poldata_df, 'BEN_PLAN', "NA")

    return poldata_df

# Update column 'BENPLN" column in the poldata_df
@comm_func.timer_func
def upd_col_benpln(poldata_df):
    # Archive existing column
    poldata_df = poldata_df.rename(columns={
        'BENPLN': 'BENPLN_x0', 
    })

    poldata_df['BENPLN'] = poldata_df['BENPLN_x0']
    poldata_df = comm_func.default_col_null_to_string_or_col(poldata_df, 'BENPLN', "")

    return poldata_df

# Add 'BANCA' column to the poldata_df
@comm_func.timer_func
def add_col_banca(poldata_df):     
    conditions = [
        (poldata_df['AGNTNUM'].str[:2] == "9U"),
        (poldata_df['AGNTNUM'].str[:2] == "9B"),
        (poldata_df['AGNTNUM'].str[:2] == "9C"),
        ((poldata_df['AGNTNUM'].str[:2] == "9D") & (poldata_df['CNTTYPE'] == "5GM")),
        (poldata_df['AGNTNUM'].str[:2] == "9I"),
        ((poldata_df['AGNTNUM'].str[:2] == "99") & (poldata_df['CNTTYPE'] == "5SP")),
        (poldata_df['CNTTYPE'].isin(["1GM", "2MC", "7GM"]) | (poldata_df['AGNTNUM'].str[0] == "9"))
    ]

    results = [
         "UO",    # UOB
         "AL",    # Alliance Bank
         "CM",    # CIMB
         "DB",    # Bank of Tokyo-Mitsuibishi UFJ(M) Berhad
         "IC",    # ICBC
         "OTB", 
         "SC"     # SCB
    ]

    default = "NA"
    
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'BANCA', conditions, results, default)
    
    return poldata_df

# Add 'AGENCY' column to the poldata_df
@comm_func.timer_func
def add_col_agency(poldata_df):
    CNTTYPE_list = ['1GM', '2MC', '7GM']

    conditions = [
        ((poldata_df['AGNTNUM'].str[0] == "9") | (poldata_df['CNTTYPE'].isin(CNTTYPE_list))),
        (poldata_df['AGNTNUM'].str[0] == "8"),
        (poldata_df['AGNTNUM'].str[0] == "6"),
        (poldata_df['AGNTNUM'].str[0] == "3")  
    ]

    results = [
        "BA",    # BANCE & MRTA(SCB) PRODUCT
        "BR",    # Broker
        "FA",    # Financial Advisor
        "DS"     # Direct selling
    ]

    default = "AG"
    
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'AGENCY', conditions, results, default)

    return poldata_df

# Add 'ANNUAL_PREM' column to the poldata_df
@comm_func.timer_func
def add_col_annual_prem(poldata_df):
    
    condition = [(poldata_df['BILLFREQ'] == 0)]
    value = [0]
    default = round((poldata_df['ZBINSTPREM'] + poldata_df['EXTR']) * poldata_df['BILLFREQ'], 2)

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'ANNUAL_PREM', condition, value, default)
    
    return poldata_df

# Add 'SINGLE_PREM' column to the poldata_df
@comm_func.timer_func
def add_col_single_prem(poldata_df):

    condition = [(poldata_df['BILLFREQ'] == 0)]
    value = [poldata_df['ZBINSTPREM'] + poldata_df['EXTR']]
    default = 0.0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'SINGLE_PREM', condition, value, default)
    poldata_df['SINGLE_PREM'] = poldata_df['SINGLE_PREM'].round(2)

    return poldata_df

# Return a filtered dataframe (for the subsequent 4 methods: add_col_mpf_ind, add_col_basic_psm_ind, add_col_tot_ap and upd_col_sumins)
@comm_func.timer_func
def mpf_indicator_filter(poldata_df):
    filtered_df = poldata_df[
        ((poldata_df['FILE'] == const_var.IF) |
         ((poldata_df['FILE'] == const_var.ET) & (poldata_df['ZMOVECDE'] == 2))) &

        (
         ((poldata_df['CNTTYPE'].isin(["5T2", "5SM"])) &
          (~poldata_df['CRTABLE'].isin(["EWA2", "EWA5"]))) | # 4/10 WC: rewrote for efficiency
          
         ((poldata_df['CNTTYPE'].isin(["5MA", "5MU"]))  &
          (poldata_df['CRTABLE'].isin(["5MAB", "5MCA", "5MUB", "5MCD"]))) # 4/10 WC: rewrote for efficiency

        )
    ]
    
    return filtered_df

# Add 'MPF_IND' column to the poldata_df
@comm_func.timer_func
def add_col_mpf_ind(poldata_df):
    filtered_df = mpf_indicator_filter(poldata_df)
    filtered_df = filtered_df.copy()

    conditions = [
        (filtered_df['BENPLN'] == "G1") & (filtered_df['CNTTYPE'].isin(["5T2", "5SM"])) # 7/10 WC: rewrote for efficiency
    ]
    results = [1]

    default = 0

    filtered_df['MPF_IND'] = np.select(conditions, results, default)
    
    groupby_key_list = ['FILE','CHDRNUM']
    
    filtered_df = comm_func.add_groupby_col(filtered_df, groupby_key_list, 'MPF_IND', 'MPF_IND_average', "mean")
    filtered_df = filtered_df.drop_duplicates(['FILE', 'CHDRNUM', 'MPF_IND_average'])
    
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, ['MPF_IND_average'], groupby_key_list, groupby_key_list, "left")

    poldata_df = comm_func.default_col_null_to_zero(poldata_df, 'MPF_IND_average') 
    poldata_df['MPF_IND'] = comm_func.apply_rounding_on_col(poldata_df,'MPF_IND_average', 0) 
    poldata_df = comm_func.drop_col(poldata_df, 'MPF_IND_average')
    
    del filtered_df
    return poldata_df

# Add 'BASIC_PSM_IND' column to the poldata_df
@comm_func.timer_func
def add_col_basic_psm_ind(poldata_df):
    filtered_df = mpf_indicator_filter(poldata_df)
    filtered_df = filtered_df.copy()

    conditions = [
        (filtered_df['BENPLN'] != "") & (filtered_df['CNTTYPE'].isin(["5T2", "5SM"])),
        True  
    ]
    results = [1, 0] 

    filtered_df['BASIC_PSM_IND'] = np.select(conditions, results)

    groupby_key_list = ['FILE','CHDRNUM']
    
    filtered_df = comm_func.add_groupby_col(filtered_df, groupby_key_list, 'BASIC_PSM_IND', "BASIC_PSM_IND_average", "mean")
    filtered_df = filtered_df.drop_duplicates(["FILE", "CHDRNUM", "BASIC_PSM_IND_average"])
    
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, ["BASIC_PSM_IND_average"], groupby_key_list, groupby_key_list, "left")
    
    poldata_df = comm_func.default_col_null_to_zero(poldata_df, 'BASIC_PSM_IND_average') 
    poldata_df['BASIC_PSM_IND'] = comm_func.apply_rounding_on_col(poldata_df,'BASIC_PSM_IND_average', 0) 
    poldata_df = comm_func.drop_col(poldata_df, 'BASIC_PSM_IND_average')

    del filtered_df
    return poldata_df

# Add 'POLYEAR' column to the poldata_df
@comm_func.timer_func
def add_col_polyear(poldata_df, val_date_const):
    val_month = val_date_const.month

    conditions = [
        (((poldata_df['ZYYMMINF'] - val_month) / 100) <= 35),
        (((poldata_df['ZYYMMINF'] - val_month) / 100) <= 65)
    ]

    results = [
        ((poldata_df['ZYYMMINF'] - val_month) // 100) + (((poldata_df['ZYYMMINF'] - val_month) % 100) > 0),
        36
    ]

    default = 66
    
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'POLYEAR', conditions, results, default)
    
    return poldata_df

# Add 'RECORD_MPF' column with all 0s to the poldata_df
@comm_func.timer_func
def add_col_recordmpf(poldata_df):    
    condition = [poldata_df['FILE'].isin([const_var.IF, const_var.ET])]
    value = [1]
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'RECORD_MPF', condition, value, default)

    return poldata_df

# Add 'TOT_AP' column to the poldata_df
@comm_func.timer_func
def add_col_tot_ap(poldata_df):
    filtered_df = mpf_indicator_filter(poldata_df)
    filtered_df = filtered_df.copy()

    filtered_df['TOT_AP'] = filtered_df['ANNUAL_PREM']
    
    groupby_key_list = ['FILE','CHDRNUM']
    
    filtered_df = comm_func.add_groupby_col(filtered_df, groupby_key_list, 'TOT_AP', 'TOT_AP_sum', "sum")
    filtered_df = filtered_df.drop_duplicates(['FILE', 'CHDRNUM', 'TOT_AP_sum'])

    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, ['TOT_AP_sum'], groupby_key_list, groupby_key_list, "left")
    
    poldata_df = poldata_df.rename(columns={
        'TOT_AP_sum': 'TOT_AP'
    })

    poldata_df['TOT_AP'] = poldata_df['TOT_AP'].round(2)
    del filtered_df
    return poldata_df

# Output TOTAP_TAB E2E Table
@comm_func.timer_func
def output_totap_tab(poldata_df):
    filtered_df = mpf_indicator_filter(poldata_df)
    totap_df = filtered_df.copy()

    groupby_key_list = ['CHDRNUM', 'FILE', 'MPF_IND']

    totap_df = comm_func.add_groupby_col(totap_df, groupby_key_list, 'SUMINS', 'SUMINS', 'sum')
    totap_df = totap_df[['CHDRNUM', 'FILE', 'MPF_IND', 'BASIC_PSM_IND', 'SUMINS', 'TOT_AP']]
    totap_df = totap_df.sort_values(by = ['FILE', 'CHDRNUM', 'MPF_IND', 'BASIC_PSM_IND'])
    totap_df = totap_df.drop_duplicates(['CHDRNUM', 'FILE', 'MPF_IND', 'BASIC_PSM_IND', 'SUMINS', 'TOT_AP'])

    del filtered_df
    return totap_df

# Add 'TOTAP_SUMINS' column in the poldata_df
@comm_func.timer_func
def add_col_totap_sumins(poldata_df):
    filtered_df = mpf_indicator_filter(poldata_df)
    filtered_df = filtered_df.copy()

    groupby_key_list = ['FILE', 'CHDRNUM']
    
    filtered_df = comm_func.add_groupby_col(filtered_df, groupby_key_list, 'SUMINS', 'TOTAP_SUMINS', "sum")
    filtered_df = filtered_df.drop_duplicates(['FILE', 'CHDRNUM', 'TOTAP_SUMINS'])
    filtered_df = filtered_df.copy()
    
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, ['TOTAP_SUMINS'], groupby_key_list, groupby_key_list, "left")
    
    del filtered_df
    return poldata_df

# Add 'HISSDTE_Temp' column to the poldata_df
@comm_func.timer_func
def add_col_hissdte_temp(poldata_df, target_col_name):

    conditions = [
        (poldata_df['HISSDTE'] >= pd.to_datetime("2020-05-04")),
        (poldata_df['HISSDTE'] >= pd.to_datetime("2019-11-18")),
        (poldata_df['HISSDTE'] >= pd.to_datetime("2019-08-26")), 
        (poldata_df['HISSDTE'] >= pd.to_datetime("2019-03-18")),
        (poldata_df['HISSDTE'] >= pd.to_datetime("2019-01-07")),
        (poldata_df['HISSDTE'] >= pd.to_datetime("2018-10-08")),
        (poldata_df['HISSDTE'] >= pd.to_datetime("2016-03-21")),
        (poldata_df['HISSDTE'] >= pd.to_datetime("2015-08-24")),
        ((poldata_df['HISSDTE'] >= pd.to_datetime("2014-02-01")) &
         ((poldata_df['ISSUE_MONTH'] == 7) | (poldata_df['ISSUE_MONTH'] == 8))),
        (poldata_df['HISSDTE'] >= pd.to_datetime("2014-02-01")),
        (poldata_df['HISSDTE'] >= pd.to_datetime("2013-01-01"))
    ]

    results = [
        13,
        12, 
        11,
        10,
        9,
        8,
        7,
        6,
        4,
        5,
        3
    ]

    default = 0

    poldata_df[target_col_name] = np.select(conditions, results, default)
    
    return poldata_df

# Add supporting working for 'ENHANCED_IND'
@comm_func.timer_func
def compare_dur(poldata_df, target_col_name):
    conditions = [(poldata_df['ZYYMMINF'] <= poldata_df['DUR2']) | (poldata_df['ZYYMMINF'] <= poldata_df['DUR1'])]
    results = [1]
    default = 0

    poldata_df[target_col_name] = np.select(conditions, results, default)

    return poldata_df

# Supporting working for 'ENHANCED_IND'
@comm_func.timer_func
def compare_dur2(poldata_df, target_col_name):
    conditions = [
        (poldata_df['ZYYMMINF'] <= poldata_df['DUR2']),
        (poldata_df['ZYYMMINF'] <= poldata_df['DUR1'])    
    ]

    results = [
        2,
        3
    ]

    default = 0

    poldata_df[target_col_name] = np.select(conditions, results, default)
    
    return poldata_df

#  Add 'ENHANCED_IND' column to the poldata_df
@comm_func.timer_func
def add_col_enhanced_ind(poldata_df):   
    CNTTYPE_list = const_var.cnttype_enhanced_ind_list()
    CRTABLE_list = const_var.crtable_enhanced_ind_list()
    CRTABLE_list_2 = const_var.crtable2_enhanced_ind_list()
    CRTABLE_list_3 = const_var.crtable3_enhanced_ind_list()

    add_col_hissdte_temp(poldata_df, 'HISSDTE_TEMP')
    compare_dur(poldata_df, 'DUR_TEMP')
    compare_dur2(poldata_df, 'DUR_TEMP2')

    conditions = [
        (((poldata_df['CNTTYPE'] == "HYB") & (poldata_df['CRTABLE'] == "HY02")) & 
         (poldata_df['HISSDTE'] < pd.to_datetime("2019-06-17"))),

        (((poldata_df['CNTTYPE'] == "HYB") & (poldata_df['CRTABLE'] == "HY02")) & 
         (poldata_df['HISSDTE'] >= pd.to_datetime("2019-06-17"))),

        ((poldata_df['CNTTYPE'] == "5PD") & (poldata_df['CRTABLE'] == "5PDB") & 
         (poldata_df['ENTRY_YEAR'] >= 2017) & (poldata_df['ENTRY_YEAR'] <= 2019)) ,

        ((poldata_df['CNTTYPE'].isin(["GIU", "GIP"])) & 
         (poldata_df['CRTABLE'].isin(["TR06", "TR04"]))),

        ((poldata_df['CNTTYPE'] == "GIC") &
         (poldata_df['CRTABLE'] == "TR25") &
         (poldata_df['HISSDTE'] >= pd.to_datetime("2023-01-16"))),

        ((poldata_df['CNTTYPE'] == "GIC") &
         (poldata_df['CRTABLE'] == "TR25")),

        ((poldata_df['CNTTYPE'].isin(CNTTYPE_list)) &  # 24/9 WC: updated from CRTTABLE to CNTTYPE to reflect fix identified from XLT testing
         (poldata_df['CRTABLE'].isin(CRTABLE_list)) &
         (poldata_df['ENTRY_YEAR'] >= 2018)),

        ((poldata_df['CNTTYPE'].isin(CNTTYPE_list)) & 
         (poldata_df['CRTABLE'].isin(CRTABLE_list))), 

        ((poldata_df['AGNTNUM'].str[:1] == "9") &
         (poldata_df['AGNTNUM'].str[:2] != "9U") & 
         (poldata_df['CRTABLE'].isin(CRTABLE_list_2))),

        ((poldata_df['AGNTNUM'].str[:1] == "9") & 
         (poldata_df['AGNTNUM'].str[:2] != "9U") & 
         (poldata_df['CRTABLE'].isin(CRTABLE_list_3))),

        ((poldata_df['AGNTNUM'].str[:1] == "9") & 
         (poldata_df['AGNTNUM'].str[:2] != "9U") & 
         (poldata_df['CNTTYPE'] == "5SM") & # 24/9 WC: updated from CRTTABLE to CNTTYPE to reflect fix identified from XLT testing
         (poldata_df['ZYYMMINF'] <= poldata_df['DUR1']))
    ]    
 
    results = [
        0,
        1,
        1, 
        poldata_df['HISSDTE_TEMP'],
        15,
        14,
        1,
        0,
        poldata_df['DUR_TEMP'],
        poldata_df['DUR_TEMP2'],
        2
    ]
    
    default = 0

    # poldata_df['ENHANCED_IND'] = np.select(conditions, results, default)
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'ENHANCED_IND', conditions, results, default) 

    poldata_df = comm_func.drop_col(poldata_df, 'DUR_TEMP')
    poldata_df = comm_func.drop_col(poldata_df, 'DUR_TEMP2')

    return poldata_df

# Update Enhanced Index column for DMTM policies
@comm_func.timer_func
def upd_col_enhanced_ind_dmtm(poldata_df):
    # Archive previous column value
    poldata_df = poldata_df.rename(columns={
        'ENHANCED_IND': 'ENHANCED_IND_x0', 
    })
    
    condition = [poldata_df['CNTTYPE'].isin(["5T2", "5SM"])]
    value = [(poldata_df['ENHANCED_IND_x0'] + poldata_df['MPF_IND'])]
    default = poldata_df['ENHANCED_IND_x0']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'ENHANCED_IND', condition, value, default)

    DMTM_list = const_var.dmtm_filter_list()
    
    filtered_df = filter_df(poldata_df, DMTM_list, 0)
    filtered_df = filtered_df.copy() # 23/9 WC - added to remove slice warning

    condition_filtered = [(((filtered_df['ENTRY_YEAR'] > 2009) | 
        ((filtered_df['ENTRY_YEAR'] == 2009) & (filtered_df['ENTRY_MONTH'] >= 7))))
    ]
    
    value_filtered = [1]
    default_filtered = 0

    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'ENHANCED_IND_DMTM', condition_filtered, value_filtered, default_filtered)
    
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, ['ENHANCED_IND_DMTM'], ['$RID'], ['$RID'], 'left' )

    poldata_df = poldata_df.rename(columns={
        'ENHANCED_IND': 'ENHANCED_IND_x1', 
    })

    condition_na = [poldata_df['ENHANCED_IND_DMTM'].isna()]
    value_na = [poldata_df['ENHANCED_IND_x1']]
    default_na = poldata_df['ENHANCED_IND_DMTM']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'ENHANCED_IND', condition_na, value_na, default_na)

    poldata_df = comm_func.drop_col(poldata_df, 'ENHANCED_IND_DMTM')

    del filtered_df
    return poldata_df

# Update BANCA column for DMTM policies
@comm_func.timer_func
def upd_col_banca_dmtm(poldata_df):
       
    DMTM_list = const_var.dmtm_filter_list()
    
    poldata_df = poldata_df.rename(columns={
        'BANCA': 'BANCA_x0', 
    })

    condition = [poldata_df['CNTTYPE'].isin(DMTM_list)]
    value = ["DUM"]
    default = poldata_df['BANCA_x0']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, "BANCA", condition, value, default)
    return poldata_df

# Add 'SB_S_YR' column to the poldata_df
@comm_func.timer_func
def add_col_sb_s_yr(poldata_df):
    DMTM_list = const_var.dmtm_filter_list() 
    filtered_dmtm_df = filter_df(poldata_df, DMTM_list, 0)

    condition = [(filtered_dmtm_df['CNTTYPE'] == "ACE")]
    value = [3]
    default = 0
    filtered_dmtm_df = filtered_dmtm_df.copy()
    filtered_dmtm_df = comm_func.add_conditional_column_to_df(filtered_dmtm_df, 'SB_S_YR', condition, value, default)

    poldata_df = comm_func.add_merge_col(poldata_df, filtered_dmtm_df, ['SB_S_YR'], ['$RID'], ['$RID'], 'left')

    del filtered_dmtm_df
    return poldata_df

# Merge the DMTM_CELLPC_IND table to the poldata_df
@comm_func.timer_func
def mrg_dmtm_cellpc_ind_tbl(poldata_df, dmtm_cellpc_ind_tbl):
    output_col =  ['CELLPC_IND']
    left_key = ['CHDRNUM']
    right_key = ['IL_Polno']
    join_type = 'left'
    
    DMTM_list = const_var.dmtm_filter_list()          
    
    poldata_df = comm_func.add_merge_col(poldata_df, dmtm_cellpc_ind_tbl, output_col, left_key, right_key, join_type)
    
    condition = [((poldata_df['CNTTYPE'].isin(DMTM_list)) & poldata_df['CELLPC_IND'].isna())]
    value = [0]
    default = poldata_df['CELLPC_IND']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'CELLPC_IND', condition, value, default)
        
    return poldata_df

# Merge the DMTM table to the poldata_df
@comm_func.timer_func
def mrg_dmtm_tbl(poldata_df, dmtm_tbl):
    output_col = ['SUM_ASSURED', 'HB_SUM_ASSD', 'SURR_FAC_1', 'CIC_BEN_PP', 'ACC_SUM_ASSD', 'CC_SUM_ASSD', 
                  'CC2_SUM_ASSD', 'AMR_SA_PP', 'ROP_MAT_PC', 'HC_SUM_ASSD', 'INCR_BEN', 'YI_DTH_TPD', 
                  'SB_BEN', 'ADEN_BEN', 'ECA_BEN1', 'ECA_BEN2', 'LS_SUM_ASSD']
    left_key = ['CNTTYPE', 'CRTABLE', 'BENPLN', 'ENHANCED_IND', 'CELLPC_IND']
    right_key = ['CNTTYPE', 'CRTABLE', 'BENPLN', 'ENHANCED_IND', 'CELLPC_IND']
    join_type = 'left'
    
    DMTM_list = const_var.dmtm_filter_list()
    
    filtered_dmtm_df = filter_df(poldata_df, DMTM_list, 0)
    
    filtered_df = comm_func.add_merge_col(filtered_dmtm_df, dmtm_tbl, output_col, left_key, right_key, join_type)
    filtered_df = comm_func.default_col_null_to_zero(filtered_df, 'CIC_BEN_PP')

    conditions = [
        filtered_df['SUM_ASSURED'].isna(),
        ~filtered_df['SUM_ASSURED'].isna()
    ]
    
    results = [
        0, 
        filtered_df['RECORD_TO_KEEP']
    ]

    filtered_df['RECORD_TO_KEEP_temp'] = np.select(conditions, results)

    
    output_col.append('RECORD_TO_KEEP_temp')
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, output_col, ['$RID'], ['$RID'], 'left' )

    conditions = [
        poldata_df['RECORD_TO_KEEP_temp'] == 0,
        poldata_df['RECORD_TO_KEEP_temp'] != 0     # The rest of the conditions
    ]

    results = [
        0, 
        poldata_df['RECORD_TO_KEEP']
    ]

    # poldata_df['RECORD_TO_KEEP'] = np.select(conditions, results)
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'RECORD_TO_KEEP', conditions, results) 

    poldata_df = comm_func.drop_col(poldata_df, 'RECORD_TO_KEEP_temp')

    del filtered_dmtm_df, filtered_df
    return poldata_df

# Add a 'min_$RID' column to the poldata_df
@comm_func.timer_func
def add_col_min_rid(poldata_df):
    DMTM_list = const_var.dmtm_filter_list()
    filtered_dmtm_df = filter_df(poldata_df, DMTM_list, 0)
    
    filtered_df = filtered_dmtm_df[(filtered_dmtm_df['CNTTYPE'].isin(["LIP", "LPC"])) 
                                & (filtered_dmtm_df['RECORD_TO_KEEP'] != 0)]
    filtered_df = filtered_df[['CHDRNUM', 'FILE', '$RID']]
    filtered_df = comm_func.add_groupby_col(filtered_df, ['CHDRNUM', 'FILE'], '$RID', 'min_$RID', 'min')
    filtered_df = filtered_df[['CHDRNUM', 'FILE', 'min_$RID']]
    filtered_df = filtered_df.drop_duplicates()
    
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, ['min_$RID'], ['CHDRNUM', 'FILE'], ['CHDRNUM', 'FILE'], 'left')
    poldata_df[poldata_df['CNTTYPE'].isin(["LIP", "LPC"])]

    del filtered_dmtm_df, filtered_df
    return poldata_df

# Define a generic function to zeroise values in a target_col, where the RID is greater than the minimum RID for the same record
@comm_func.timer_func
def zeroise_non_min_rid_values(poldata_df, source_col, target_col):
    conditions = [
        poldata_df['$RID'] > poldata_df['min_$RID'],
        ((poldata_df['min_$RID'].isna()) | (poldata_df['$RID'] <= poldata_df['min_$RID'])),
    ]

    results = [
        0, 
        poldata_df[source_col]
    ]

    poldata_df[target_col] = np.select(conditions, results)

    return poldata_df

# Update column 'SUM_ASSURED' in the poldata_df 
@comm_func.timer_func
def upd_col_sum_assured_min_rid(poldata_df):
    # Archive existing column
    poldata_df = poldata_df.rename(columns={
        'SUM_ASSURED': 'SUM_ASSURED_x0', 
    })
    
    poldata_df = zeroise_non_min_rid_values(poldata_df, 'SUM_ASSURED_x0', 'SUM_ASSURED')

    return poldata_df

# Update column 'ACC_SUM_ASSURED' in the poldata_df
@comm_func.timer_func
def upd_col_acc_sum_assured(poldata_df):
    # Archive existing column
    poldata_df = poldata_df.rename(columns={
        'ACC_SUM_ASSD': 'ACC_SUM_ASSD_x0'
    })

    # Define list of conditions
    rid_gt_min_rid = (poldata_df['$RID'] > poldata_df['min_$RID'])
    cnttype_is_LIP = (poldata_df["CNTTYPE"] == "LIP")
    cnttype_is_LPC = (poldata_df["CNTTYPE"] == "LPC")
    crttable_is_LIPF = (poldata_df["CRTABLE"] == "LIPF")
    crttable_is_LIPS = (poldata_df["CRTABLE"] == "LIPS")
    crttable_is_LPCF = (poldata_df["CRTABLE"] == "LPCF")
    crttable_is_LPCS = (poldata_df["CRTABLE"] == "LPCS")
    benplan_is_ZR = (poldata_df["BENPLN"] == "ZR")
    benplan_is_ZS = (poldata_df["BENPLN"] == "ZS")
    benplan_is_ZT = (poldata_df["BENPLN"] == "ZT")
    benplan_is_ZU = (poldata_df["BENPLN"] == "ZU")
    benplan_is_ZV = (poldata_df["BENPLN"] == "ZV")

    conditions = [
        (rid_gt_min_rid) & (cnttype_is_LIP) & (crttable_is_LIPF) & (benplan_is_ZR),
        (rid_gt_min_rid) & (cnttype_is_LIP) & (crttable_is_LIPF) & (benplan_is_ZS),

        (rid_gt_min_rid) & (cnttype_is_LIP) & (crttable_is_LIPS) & (benplan_is_ZR),
        (rid_gt_min_rid) & (cnttype_is_LIP) & (crttable_is_LIPS) & (benplan_is_ZS),  

        (rid_gt_min_rid) & (cnttype_is_LPC) & (crttable_is_LPCF) & (benplan_is_ZT),
        (rid_gt_min_rid) & (cnttype_is_LPC) & (crttable_is_LPCF) & (benplan_is_ZU),    
        (rid_gt_min_rid) & (cnttype_is_LPC) & (crttable_is_LPCF) & (benplan_is_ZV),

        (rid_gt_min_rid) & (cnttype_is_LPC) & (crttable_is_LPCS) & (benplan_is_ZT),
        (rid_gt_min_rid) & (cnttype_is_LPC) & (crttable_is_LPCS) & (benplan_is_ZU),    
        (rid_gt_min_rid) & (cnttype_is_LPC) & (crttable_is_LPCS) & (benplan_is_ZV)
    ]
 
    results = [
        375000 - 75000, 
        125000 - 25000,

        150000 - 75000,
        50000 - 25000, # 24/9 WC: Updated to reflect XLT testing, typo.
        
        375000 - 75000,
        250000 - 50000,
        175000 - 35000,
        
        150000 - 75000,
        100000 - 50000,
        70000 - 35000
    ]

    default = poldata_df['ACC_SUM_ASSD_x0']

    poldata_df['ACC_SUM_ASSD'] = np.select(conditions, results, default)

    return poldata_df

# Update column 'INCR_BEN' in the poldata_df
@comm_func.timer_func
def upd_col_incr_ben(poldata_df):
    # Archive existing column
    poldata_df = poldata_df.rename(columns={
        'INCR_BEN': 'INCR_BEN_x0'
    })

    # Call function to zeroise records that do not match thte minimum RID record
    poldata_df = zeroise_non_min_rid_values(poldata_df, 'INCR_BEN_x0', 'INCR_BEN')
    
    return poldata_df

# Update column 'YI_DTH_TPD' in the poldata_df
@comm_func.timer_func
def upd_col_yi_dth_tpd(poldata_df):
    # Archive existing column
    poldata_df = poldata_df.rename(columns={
        'YI_DTH_TPD': 'YI_DTH_TPD_x0'
    })
    
    # Call function to zeroise records that do not have minimum RID values
    poldata_df = zeroise_non_min_rid_values(poldata_df, 'YI_DTH_TPD_x0', 'YI_DTH_TPD')
    poldata_df = comm_func.drop_col(poldata_df, 'min_$RID')

    return poldata_df

# Define a generic filter function that either applies a specified filter to the dataframe
@comm_func.timer_func
def filter_df(poldata_df, filter_criteria, switch):
    criteria = poldata_df['CNTTYPE'].isin(filter_criteria)

    if switch == 0:
        filtered_df = poldata_df[criteria]
    else:
        filtered_df = poldata_df[~criteria]
    
    return filtered_df

# Update SUM_ASSURED column for non-DMTM policies by merging table 'GENERAL' to the poldata_df
@comm_func.timer_func
def upd_col_sum_assured_nondmtm(poldata_df, general_tbl):
   
    
    # Default null values in the General Table BANCA column to "NA"
    conditions = [general_tbl['BANCA'].isna()]
    results = ["NA"]
    default = general_tbl['BANCA']

    general_tbl = comm_func.add_conditional_column_to_df(general_tbl, 'BANCA', conditions, results, default)

    # Filter out DMTM records from poldata_df, and merge against General Table to bring in SUM_ASSURED information
    DMTM_list = const_var.dmtm_filter_list()
    filtered_nondmtm_df = filter_df(poldata_df, DMTM_list, 1)
    
    output_col = ['FUND_IND', 'CCB_IND', 'VAR_IND', 'SP_IND', 'LFORMDESC', 
                  'PREM_IND', 'RB_IND', 'LIFE_IND', 'PARBSC_IND', 'WAIV_IND']
    mapping_key = ['CNTTYPE', 'CRTABLE', 'ENHANCED_IND', 'BANCA']

    filtered_df = comm_func.add_merge_col(filtered_nondmtm_df, general_tbl, output_col, mapping_key, mapping_key, "left")
    
    # For 'RECORD_TO_KEEP_temp' column
    conditions = [filtered_df['FUND_IND'].isna()] 
    results = [0]
    default = filtered_df['RECORD_TO_KEEP']
    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'RECORD_TO_KEEP_temp', conditions, results, default)

    # For 'SUM_ASSURED_nonDMTM' column
    conditions = [filtered_df['CCB_IND'] == 12]  
    results = [filtered_df['SUMINS'] * filtered_df['BILLFREQ']]
    default = filtered_df['SUMINS']
    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'SUM_ASSURED_nonDMTM', conditions, results, default)

    # Merge SUM_ASSURED for non-DMTM policies back to the poldata_df
    temp_list = ['RECORD_TO_KEEP_temp', 'SUM_ASSURED_nonDMTM']
    output_col_append = output_col + temp_list
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, output_col_append, ['$RID'], ['$RID'], 'left' )
    
    conditions = [poldata_df['RECORD_TO_KEEP_temp'] == 0]
    results = [0]
    default = poldata_df['RECORD_TO_KEEP']

    # poldata_df['RECORD_TO_KEEP'] = np.select(conditions, results)
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'RECORD_TO_KEEP', conditions, results, default) 

    # Archive existing column
    poldata_df = poldata_df.rename(columns={
        'SUM_ASSURED': 'SUM_ASSURED_x1'
    })

    conditions = [
        poldata_df['RECORD_TO_KEEP'] == 0,
        poldata_df['SUM_ASSURED_nonDMTM'].isna()
    ]

    results = [
        np.nan,
        poldata_df['SUM_ASSURED_x1']
    ]

    default = poldata_df['SUM_ASSURED_nonDMTM']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'SUM_ASSURED', conditions, results, default) 
    poldata_df = comm_func.default_col_null_to_zero(poldata_df, 'SUM_ASSURED')

    # Apply rounding
    poldata_df['SUM_ASSURED'] = poldata_df['SUM_ASSURED'].astype(float).round(2)
    
    poldata_df = comm_func.drop_col(poldata_df, ['RECORD_TO_KEEP_temp', 'SUM_ASSURED_nonDMTM'])

    del filtered_nondmtm_df, filtered_df

    return poldata_df

# Update 'LFORMDESC' column in the poldata_df
@comm_func.timer_func
def upd_col_lformdesc(poldata_df):
    DMTM_list = const_var.dmtm_filter_list()
    
    poldata_df = poldata_df.rename(columns={
        'LFORMDESC': 'LFORMDESC_x0'
    })

    condition = [poldata_df['CNTTYPE'].isin(DMTM_list)]
    value = ["YODA"]
    default = poldata_df['LFORMDESC_x0']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'LFORMDESC', condition, value, default)          # Not adding a new column but editing it. Consider renaming the function
    return poldata_df

# Add 'SUMINS_FREQ' column in the poldata_df
@comm_func.timer_func
def add_col_sumins_freq(poldata_df):
    DMTM_list = const_var.dmtm_filter_list()
    
    filtered_nondmtm_df = filter_df(poldata_df, DMTM_list, 1)
    filtered_nondmtm_df = filtered_nondmtm_df.copy()
    
    condition = [(filtered_nondmtm_df['CCB_IND'] == 12)]
    value = [filtered_nondmtm_df['SUM_ASSURED']]
    default = 0.0

    filtered_nondmtm_df = comm_func.add_conditional_column_to_df(filtered_nondmtm_df, 'SUMINS_FREQ', condition, value, default)
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_nondmtm_df, ['SUMINS_FREQ'], ['$RID'], ['$RID'], 'left' )
    
    del filtered_nondmtm_df
    return poldata_df

# Merge table 'EDP_POL_IND' to the poldata_df
@comm_func.timer_func
def mrg_edp_polno_ind_tbl(poldata_df, edp_polno_ind_tbl):
    output_col = ['PROD_NAME', 'POLNO_IND']
    mapping_key = ['CHDRNUM', 'CNTTYPE', 'CRTABLE']

    edp_polno_ind_tbl['POLNO_IND'].astype(int)
    poldata_df = comm_func.add_merge_col(poldata_df, edp_polno_ind_tbl, output_col, mapping_key, mapping_key, "left")

    condition = [poldata_df['POLNO_IND'].isna()]
    value = [0]
    default = poldata_df['POLNO_IND']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'POLNO_IND', condition, value, default)          # Not adding a new column but editing it. Consider renaming the function

    poldata_df = poldata_df.rename(columns={
        'PROD_NAME': 'EDP_PROD_NAME'
    })
    return poldata_df

# Add 'FUNDTYPE' column to the poldata_df
@comm_func.timer_func
def add_col_fundtype(poldata_df):
    conditions = [
        (poldata_df['POLNO_IND'] == 1) | (poldata_df['FUND_IND'] == "PAR"),
        (poldata_df['FUND_IND'].isin(["NPAR", "LINK"]))
    ]

    results = [
        1,
        2
    ]

    default = 0

    # poldata_df['FUNDTYPE'] = np.select(conditions, results, default)
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'FUNDTYPE', conditions, results, default)

    return poldata_df

# Merge valuation extract with the 'RB' table
@comm_func.timer_func
def mrg_rb_tbl(poldata_df, rb_tbl):
    
    rb_tbl = rb_tbl.rename(columns={
        'polyear': 'POLYEAR', 
    })

    output_col = ['rb', 'rb1', 'rb2', 'rb3', 'rb4', 'rb5', 'rb6', 'rb7']
    left_key = ['POLYEAR']
    right_key = ['POLYEAR']
    join_type = 'left'
    
    poldata_df = comm_func.add_merge_col(poldata_df, rb_tbl, output_col, left_key, right_key, join_type)
    
    return poldata_df

# Add a series flag indicator to distinguish between old and new series products (by contract type)
@comm_func.timer_func
def add_col_old_new_series_product_ind(poldata_df):
    # Conditions 
    conditions = [
        (poldata_df['CNTTYPE'].str[:1].isin(["1", "2", "3"])),  # if 'CNTTYPE' starts with 1,2 or 3
        (poldata_df['CNTTYPE'] == "WL7"),  # if 'CNTTYPE' is 'WL7'
        (poldata_df['CNTTYPE'].str[:1] == "P")  # if 'CNTTYPE' starts with 'P'
    ]

    choices = [
        "old_series", 
        "old_series", 
        "old_series"
    ]

    default = 'new_series'
    
    poldata_df['OLD_SERIES_IND_temp'] = np.select(conditions, choices, default) 

    return poldata_df

# Add PRE_EDP_FUNDTYPE indicator in the valuation extract
@comm_func.timer_func
def add_col_old_series_pre_edp_fundtype(poldata_df):
    
    filtered_df = poldata_df[(poldata_df['OLD_SERIES_IND_temp'] == "new_series")]
    filtered_df = filtered_df.copy() # 23/9 WC - added to remove slice warning

    conditions = [
        (filtered_df['FUND_IND'] == "PAR"),
        (filtered_df['FUND_IND'] == "NPAR"),
        (filtered_df['FUND_IND'] == "LINK"),                
    ]
    
    results = [
        1,
        2,
        2
    ]
    
    default = 0

    filtered_df['PRE_EDP_FUNDTYPE'] = np.select(conditions, results, default)
    
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, ['PRE_EDP_FUNDTYPE'], ['$RID'], ['$RID'], 'left')

    del filtered_df
    return poldata_df

# Create two summary tables - based on the Trad Waiver Type reference table for with fund and without fund mapping
@comm_func.timer_func
def split_trad_wv_type_tbl(trad_wv_type_tbl):

    groupby_key = ['CNTTYPE', 'CHDRNUM', 'FUND']
   
    groupby_agg_funcs = {
        'SUM_PREM': 'sum',
        'WAIV_TYPE': 'min',
        'POL_TERM_Y': 'max',
        'WIF_IND': 'max'
    }
   
    groupby_rename_list = {
        'SUM_PREM': 'WLIST_SUM_PREM',
        'WAIV_TYPE': 'WLIST_W_IND',
        'POL_TERM_Y': 'WLIST_TERM',
        'WIF_IND': 'WIF_IND'
    }

    w_fund_groupby_df = comm_func.return_groupby_df(trad_wv_type_tbl, groupby_key, groupby_agg_funcs, groupby_rename_list)

    groupby_key = ['CNTTYPE', 'CHDRNUM']
    
    groupby_rename_list = {
        'SUM_PREM': 'WLIST_SUM_PREM_woFUND',
        'WAIV_TYPE': 'WLIST_W_IND_woFUND',
        'POL_TERM_Y': 'WLIST_TERM_woFUND',
        'WIF_IND': 'WIF_IND_woFUND'
    }

    wo_fund_groupby_df = comm_func.return_groupby_df(trad_wv_type_tbl, groupby_key, groupby_agg_funcs, groupby_rename_list)

    return w_fund_groupby_df, wo_fund_groupby_df

# Add the PRE_EDP_FUNDTYPE column in the "with fund" group by table (as created in method 'split_trad_wv_type_tbl') 
@comm_func.timer_func
def add_col_pre_edp_fundtype_wfund(w_fund_groupby_df):
    conditions = [
        (w_fund_groupby_df['FUND'] == "A"),
        (w_fund_groupby_df['FUND'] == "N")              
    ]
    
    results = [
        1,
        2
    ]
    
    default = 0

    w_fund_groupby_df['PRE_EDP_FUNDTYPE']  = np.select(conditions, results, default)
    
    return w_fund_groupby_df

# Merge valuation extract with the trad waiver type table for both with fund and without fund policies
@comm_func.timer_func
def mrg_trad_wv_type_tbl(poldata_df, trad_wv_type_tbl):
    w_fund_output_col = ['WLIST_SUM_PREM', 'WLIST_W_IND', 'WLIST_TERM', 'WIF_IND']
    w_fund_left_key = ['CHDRNUM', 'CNTTYPE', 'PRE_EDP_FUNDTYPE']
    w_fund_right_key = ['CHDRNUM', 'CNTTYPE', 'PRE_EDP_FUNDTYPE']
    w_fund_join_type = 'left'

    wo_fund_output_col = ['WLIST_SUM_PREM_woFUND', 'WLIST_W_IND_woFUND', 'WLIST_TERM_woFUND', 'WIF_IND_woFUND']
    wo_fund_left_key = ['CHDRNUM', 'CNTTYPE', 'OLD_SERIES_IND_temp']
    wo_fund_right_key = ['CHDRNUM', 'CNTTYPE', 'OLD_SERIES_IND_temp']
    wo_fund_join_type = 'left'

    w_fund_groupby_df, wo_fund_groupby_df = split_trad_wv_type_tbl(trad_wv_type_tbl)

    w_fund_groupby_df = add_col_pre_edp_fundtype_wfund(w_fund_groupby_df)
    wo_fund_groupby_df['OLD_SERIES_IND_temp'] = 'old_series'

    poldata_df = comm_func.add_merge_col(poldata_df, w_fund_groupby_df, w_fund_output_col, w_fund_left_key, w_fund_right_key, w_fund_join_type)
    poldata_df = comm_func.add_merge_col(poldata_df, wo_fund_groupby_df, wo_fund_output_col, wo_fund_left_key, wo_fund_right_key, wo_fund_join_type)

    conditions = [poldata_df['WLIST_SUM_PREM_woFUND'].isna(), ~poldata_df['WLIST_SUM_PREM_woFUND'].isna()]
    results = [poldata_df['WLIST_SUM_PREM'], poldata_df['WLIST_SUM_PREM_woFUND']]
    poldata_df['WLIST_SUM_PREM'] = np.select(conditions, results)

    conditions = [poldata_df['WLIST_W_IND_woFUND'].isna(), ~poldata_df['WLIST_W_IND_woFUND'].isna()]
    results = [poldata_df['WLIST_W_IND'], poldata_df['WLIST_W_IND_woFUND']]
    poldata_df['WLIST_W_IND'] = np.select(conditions, results)

    conditions = [poldata_df['WLIST_TERM_woFUND'].isna(), ~poldata_df['WLIST_TERM_woFUND'].isna()]
    results = [poldata_df['WLIST_TERM'], poldata_df['WLIST_TERM_woFUND']]
    poldata_df['WLIST_TERM'] = np.select(conditions, results)

    conditions = [poldata_df['WIF_IND_woFUND'].isna(), ~poldata_df['WIF_IND_woFUND'].isna()]
    results = [poldata_df['WIF_IND'], poldata_df['WIF_IND_woFUND']]
    poldata_df['WIF_IND'] = np.select(conditions, results)

    poldata_df = poldata_df.drop(wo_fund_output_col, axis = 1)

    poldata_df = comm_func.add_merge_col(poldata_df, wo_fund_groupby_df, ['WLIST_SUM_PREM_woFUND'], ['CHDRNUM', 'CNTTYPE'], ['CHDRNUM', 'CNTTYPE'], 'left')
    poldata_df = poldata_df.rename(columns={
        'WLIST_SUM_PREM_woFUND': 'TOTAL_WAIVED_PREM'
    })

    data_cleaning_col = ['WLIST_SUM_PREM', 'WLIST_W_IND', 'WLIST_TERM', 'TOTAL_WAIVED_PREM']
    for x in data_cleaning_col:
        poldata_df = comm_func.default_col_null_to_zero(poldata_df, x)

    return poldata_df

# Merge the RI_CED_TRAD table to the poldata_df
@comm_func.timer_func
def mrg_ri_ced_trad_tbl(poldata_df, ri_ced_trad_tbl):
    output_col = ['RI_CED_COMP_BEN', 'RI_CED_ACHL1', 'RI_CED_ACHL2', 'RI_CED_ADTL1', 'RI_CED_ADTL2', 
                  'RI_CED_ADTL3', 'RI_CED_ADTL4', 'RI_CED_CIC1', 'RI_CED_CIC2', 'RI_CED_DEATH']
    left_key = ['CHDRNUM', 'LIFE', 'COVERAGE', 'RIDER']
    right_key = ['CHDRNUM', 'LIFE', 'COVERAGE', 'RIDER']
    join_type = 'left'
    
    poldata_df = comm_func.add_merge_col(poldata_df, ri_ced_trad_tbl, output_col, left_key, right_key, join_type)
    
    for i in output_col:
        poldata_df = comm_func.default_col_null_to_zero(poldata_df, i)

    return poldata_df

# Add a 'PM_IND' column to the poldata_df
@comm_func.timer_func
def add_col_pm_ind(poldata_df):
    conditions = [
        poldata_df['CCB_IND'] == 4,
        poldata_df['CCB_IND'] == 5
    ]

    results = [
        1,
        2
    ]

    default = 0

    poldata_df['PM_IND'] = np.select(conditions, results, default)

    return poldata_df

# Add 'CCB_IND' column to the poldata_df
@comm_func.timer_func
def gen_ccb_ind(poldata_df):
    condition = [((poldata_df['FILE'] == const_var.IF) & (poldata_df['CCB_IND'].isin([1, 2, 3])))] # 24/9 WC: Updated to reflect XLT testing, typo.
    value = [0]         # 0 is true, 1 is false
    default = 1

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'CCB_TAB_IND', condition, value, default)

    return poldata_df

# Add interim 'CC_ANN_PREM' column for the calculation of 'CCB_CC_AN_PREM'
@comm_func.timer_func
def add_col_filtered_cc_ann_prem(poldata_df):
    filtered_df = gen_ccb_ind(poldata_df)
    filtered_ccb_df = filtered_df[filtered_df['CCB_TAB_IND'] == 0]
    filtered_ccb_df = filtered_ccb_df.copy()
    filtered_ccb_df['CC_ANN_PREM'] = filtered_ccb_df['ANNPREM']
    
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_ccb_df, ['CC_ANN_PREM'], ['$RID'], ['$RID'], 'left')

    del filtered_df, filtered_ccb_df
    return poldata_df

# Add 'CCB_CC_ANN_PREM' column in the poldata_df
@comm_func.timer_func
def add_col_ccb_cc_ann_prem(poldata_df):

    filtered_ccb_df = poldata_df[(poldata_df['CCB_TAB_IND'] == 0) & (poldata_df['RECORD_TO_KEEP'] != 0)]
    filtered_ccb_df = filtered_ccb_df.copy()
    condition = [(filtered_ccb_df['CCB_IND'] == 2)]
    value = [2]
    default = 1

    filtered_ccb_df = comm_func.add_conditional_column_to_df(filtered_ccb_df, 'PM_IND', condition, value, default)

    filtered_ccb_df = comm_func.add_groupby_col(filtered_ccb_df, ['CHDRNUM', 'PM_IND'], 'CC_ANN_PREM', 'CCB_CC_ANN_PREM_temp', 'mean')

    poldata_df = comm_func.add_merge_col(poldata_df, filtered_ccb_df, ['CCB_CC_ANN_PREM_temp'], ['CHDRNUM', 'PM_IND'], ['CHDRNUM', 'PM_IND'], 'left')   # CC 27/9: updated to reflect XL's testing

    poldata_df['CCB_CC_ANN_PREM'] = comm_func.apply_rounding_on_col(poldata_df,'CCB_CC_ANN_PREM_temp', 0) # 24/9 WC: updated to reflect XLT testing 
    poldata_df = comm_func.default_col_null_to_zero(poldata_df, 'CCB_CC_ANN_PREM')
    poldata_df = comm_func.drop_col(poldata_df, ['CCB_CC_ANN_PREM_temp', 'CC_ANN_PREM'])

    del filtered_ccb_df
    return poldata_df

# Add 'CCB_CC_SUM_ASSURED' column in the poldata_df 
@comm_func.timer_func
def add_col_ccb_cc_sum_assd(poldata_df):

    filtered_df = poldata_df[poldata_df['CCB_TAB_IND'] == 0]        # CC 27/9: updated to reflect XL's testing result
    filtered_df = filtered_df.copy()
    filtered_df['CC_SUM_ASSD_temp'] = filtered_df['SUMINS']

    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, ['CC_SUM_ASSD_temp'], ['$RID'], ['$RID'], 'left')
    
    filtered_ccb_df = poldata_df[(poldata_df['CCB_TAB_IND'] == 0) & (poldata_df['RECORD_TO_KEEP'] != 0)]

    filtered_ccb_df = filtered_ccb_df.copy()

    condition = [(filtered_ccb_df['CCB_IND'] == 2)]
    value = [2]
    default = 1

    filtered_ccb_df = comm_func.add_conditional_column_to_df(filtered_ccb_df, 'PM_IND', condition, value, default)

    filtered_ccb_df = comm_func.add_groupby_col(filtered_ccb_df, ['CHDRNUM', 'PM_IND'], 'CC_SUM_ASSD_temp', 'CCB_CC_SUM_ASSD', 'mean')

    poldata_df = comm_func.add_merge_col(poldata_df, filtered_ccb_df, ['CCB_CC_SUM_ASSD'], ['CHDRNUM', 'PM_IND'], ['CHDRNUM', 'PM_IND'], 'left')
    poldata_df = comm_func.default_col_null_to_zero(poldata_df, 'CCB_CC_SUM_ASSD')
    poldata_df = comm_func.drop_col(poldata_df, 'CC_SUM_ASSD_temp') 

    del filtered_df, filtered_ccb_df
    return poldata_df

# Add interim 'CC_TERM_Y' column for calculation of 'CCB_CC_TERM_Y' column below
@comm_func.timer_func
def add_col_cc_term_y(poldata_df):

    filtered_ccb_df = poldata_df[(poldata_df['CCB_TAB_IND'] == 0) & (poldata_df['RECORD_TO_KEEP'] != 0)]
    filtered_ccb_df = filtered_ccb_df.copy()
    filtered_ccb_df = comm_func.add_groupby_col(filtered_ccb_df, ['CHDRNUM'], '$RID', 'min_$RID', 'min')

    condition = [(filtered_ccb_df['CCB_IND'] == 2)]
    value = [2]
    default = 1

    filtered_ccb_df = comm_func.add_conditional_column_to_df(filtered_ccb_df, 'PM_IND', condition, value, default)

    conditions = [
        ((filtered_ccb_df['$RID'] == filtered_ccb_df['min_$RID']) | (filtered_ccb_df['PM_IND'] == 2)) & (filtered_ccb_df['CCB_IND'] == 3),
        ((filtered_ccb_df['$RID'] == filtered_ccb_df['min_$RID']) | (filtered_ccb_df['PM_IND'] == 2)),
        (filtered_ccb_df['$RID'] != filtered_ccb_df['min_$RID']) & (filtered_ccb_df['PM_IND'] != 2)
    ]
    results = [
        100 - filtered_ccb_df['AGE'], 
        filtered_ccb_df['RCESTRM'],
        0
    ]

    filtered_ccb_df['CC_TERM_Y'] = np.select(conditions, results)

    poldata_df = comm_func.add_merge_col(poldata_df, filtered_ccb_df, ['CC_TERM_Y'], ['$RID'], ['$RID'], 'left')

    del filtered_ccb_df
    return poldata_df

# Add 'CCB_CC_TERM_Y' column in the poldata_df
@comm_func.timer_func
def add_col_ccb_cc_term_y(poldata_df):
    
    poldata_df = add_col_cc_term_y(poldata_df)
    
    filtered_ccb_df = poldata_df[(poldata_df['CCB_TAB_IND'] == 0) & (poldata_df['RECORD_TO_KEEP'] != 0)]
    filtered_ccb_df = filtered_ccb_df.copy()
    
    condition = [(filtered_ccb_df['CCB_IND'] == 2)]
    value = [2]
    default = 1

    filtered_ccb_df = comm_func.add_conditional_column_to_df(filtered_ccb_df, 'PM_IND', condition, value, default)

    filtered_ccb_df = comm_func.add_groupby_col(filtered_ccb_df, ['CHDRNUM', 'PM_IND'], 'CC_TERM_Y', 'CCB_CC_TERM_Y', 'mean')

    poldata_df = comm_func.add_merge_col(poldata_df, filtered_ccb_df, ['CCB_CC_TERM_Y'], ['CHDRNUM', 'PM_IND'], ['CHDRNUM', 'PM_IND'], 'left')  # CC 27/9: updated to reflect XL's testing
    poldata_df = comm_func.default_col_null_to_zero(poldata_df, 'CCB_CC_TERM_Y')
    poldata_df = comm_func.drop_col(poldata_df, 'CC_TERM_Y')

    del filtered_ccb_df
    return poldata_df

# Add 'MRTA_' columns in the poldata_df
@comm_func.timer_func
def add_col_mrta_fields(poldata_df):
    mrta_filter_list = const_var.mrta_filter_list()
    
    filtered_df = poldata_df[(poldata_df['CRTABLE'].isin(mrta_filter_list)) & (poldata_df['RECORD_TO_KEEP'] != 0)]
    filtered_df = filtered_df.copy()
    
    filtered_df['CC_SUM_ASSD'] = poldata_df['SUMINS']
    filtered_df['CC_ANN_PREM'] = 0
    filtered_df['CC_TERM_Y'] = poldata_df['RCESTRM'] 

    groupby_df = filtered_df.groupby(['CHDRNUM']).agg({
        'CC_SUM_ASSD': 'mean',
        'CC_ANN_PREM': 'mean',
        'CC_TERM_Y': 'mean'
    }).reset_index() 

    groupby_df = groupby_df.rename(columns={
        'CC_SUM_ASSD': 'MRTA_CC_SUM_ASSD', 
        'CC_ANN_PREM': 'MRTA_CC_ANN_PREM',
        'CC_TERM_Y': 'MRTA_CC_TERM_Y'
    })

    output_col_list = ['MRTA_CC_SUM_ASSD', 'MRTA_CC_ANN_PREM', 'MRTA_CC_TERM_Y']

    poldata_df = comm_func.add_merge_col(poldata_df, groupby_df, output_col_list, ['CHDRNUM'], ['CHDRNUM'], 'left')

    for x in output_col_list:
        poldata_df = comm_func.default_col_null_to_zero(poldata_df, x)

    del filtered_df, groupby_df
    return poldata_df


# Add 'POLFEE_ANN_PREM' column in the poldata_df
@comm_func.timer_func
def add_col_polfee_annprem(poldata_df):
    polfee_filter_list = const_var.polfee_filter_list()
    
    filtered_df = poldata_df[(poldata_df['CRTABLE'].isin(polfee_filter_list)) & (poldata_df['RECORD_TO_KEEP'] != 0)]
    filtered_df = filtered_df.copy()

    groupby_df = filtered_df.groupby(['CHDRNUM', 'FILE']).agg({
        'ANNPREM': 'mean'
    }).reset_index() 

    groupby_df = groupby_df.rename(columns={
        'ANNPREM': 'POLFEE_ANNPREM'
    })

    output_col_list = ['POLFEE_ANNPREM']

    poldata_df = comm_func.add_merge_col(poldata_df, groupby_df, output_col_list, ['CHDRNUM', 'FILE'], ['CHDRNUM', 'FILE'], 'left')

    for x in output_col_list:
        poldata_df = comm_func.default_col_null_to_zero(poldata_df, x)

    del filtered_df, groupby_df
    return poldata_df

# Add 'COL_MME_PRM' column in the poldata_df
@comm_func.timer_func
def add_col_mme_prm(poldata_df):
    pmle_filter_list = const_var.pmle_filter_list() 
    
    filtered_df = poldata_df[(poldata_df['CRTABLE'].isin(pmle_filter_list)) & (poldata_df['RECORD_TO_KEEP'] != 0)]
    filtered_df = filtered_df.copy()

    groupby_df = filtered_df.groupby(['CHDRNUM', 'FILE']).agg({
        'ANNPREM': 'mean'
    }).reset_index() 

    groupby_df = groupby_df.rename(columns={
        'ANNPREM': 'MME_PRM'
    })

    output_col_list = ['MME_PRM']

    poldata_df = comm_func.add_merge_col(poldata_df, groupby_df, output_col_list, ['CHDRNUM', 'FILE'], ['CHDRNUM', 'FILE'], 'left')

    for x in output_col_list:
        poldata_df = comm_func.default_col_null_to_zero(poldata_df, x)

    del filtered_df, groupby_df
    return poldata_df

# Add 'TEMP_SA' colum in the poldata_df
@comm_func.timer_func
def add_col_temp_sa(poldata_df):
    cnttype_filter_list1 = const_var.cnttype_list1_blanksa()
    cnttype_filter_list2 = const_var.cnttype_list2_blanksa()
    cnttype_filter_list3 = const_var.cnttype_list3_blanksa()
    
    crtable_filter_list1 = const_var.crtable_list1_blanksa()
    crtable_filter_list2 = const_var.crtable_list2_blanksa()
    crtable_filter_list3 = const_var.crtable_list3_blanksa()
    
    filtered_df = poldata_df[(poldata_df['CNTTYPE'].isin(cnttype_filter_list1 + cnttype_filter_list2 + cnttype_filter_list3)) & (poldata_df['RECORD_TO_KEEP'] != 0)]
    filtered_df = filtered_df.copy()

    filtered_df['TEMP_SA'] = np.nan
    filtered_df.loc[filtered_df['CNTTYPE'].isin(cnttype_filter_list1 + cnttype_filter_list2 + cnttype_filter_list3), 'TEMP_SA'] = 0
    # filtered_df['TEMP_SA'] = filtered_df['TEMP_SA'].astype(float).round(2)

    filtered_df.loc[filtered_df['CNTTYPE'].isin(cnttype_filter_list1) & (filtered_df['CRTABLE'].isin(crtable_filter_list1)), 'TEMP_SA'] = filtered_df['SUMINS']
    filtered_df.loc[filtered_df['CNTTYPE'].isin(cnttype_filter_list2) & (filtered_df['CRTABLE'].isin(crtable_filter_list2)), 'TEMP_SA'] = filtered_df['ZBINSTPREM'] + filtered_df['EXTR']
    filtered_df.loc[filtered_df['CNTTYPE'].isin(cnttype_filter_list3) & (filtered_df['CRTABLE'].isin(crtable_filter_list3)), 'TEMP_SA'] = filtered_df['ZBINSTPREM'] + filtered_df['EXTR']

    groupby_key = ['CHDRNUM', 'FILE', 'ZMOVECDE', 'CNTTYPE', 'LIFE_IND']

    groupby_df = filtered_df.groupby(groupby_key).agg({
        'TEMP_SA': 'sum'
    }).reset_index() 

    blanksa_tab_df = groupby_df[['FILE', 'CHDRNUM', 'ZMOVECDE', 'CNTTYPE', 'LIFE_IND', 'TEMP_SA']]
    blanksa_tab_df = blanksa_tab_df.sort_values(by = ['FILE', 'CHDRNUM', 'ZMOVECDE', 'CNTTYPE', 'LIFE_IND'])

    output_col_list = ['TEMP_SA']

    poldata_df = comm_func.add_merge_col(poldata_df, groupby_df, output_col_list, groupby_key, groupby_key, 'left')
    poldata_df['TEMP_SA'] = poldata_df['TEMP_SA'].astype(float).round(2)

    del filtered_df, groupby_df
    return poldata_df, blanksa_tab_df

# Output PRUTERM_TAB_STAGE0 E2E table
@comm_func.timer_func
def output_pruterm_tab_stage0_table(poldata_df):
    cnttype_filter_list = const_var.cnttype_pruterm_basic_list()
    
    crtable_filter_basic_pruterm = const_var.crtable_pruterm_list()
    crtable_filter_basic_prumortgage = const_var.crtable_prumortgage_list()
    crtable_filter_basic_ccp = const_var.crtable_ccp_list()
    crtable_filter_basic_ccpa = const_var.crtable_ccpa_list()

    filtered_df = poldata_df[(poldata_df['FILE'] == const_var.IF) & (poldata_df['CNTTYPE'].isin(cnttype_filter_list))]
    filtered_df = filtered_df.copy()

    condition_pruterm = [((filtered_df['CSTATCODE'] == "IF") & filtered_df['CRTABLE'].isin(crtable_filter_basic_pruterm))]
    value_pruterm = [1]
    default_pruterm = 5

    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'BASIC_PRUTERM', condition_pruterm, value_pruterm, default_pruterm)
   
    condition_prumortgage = [((filtered_df['CSTATCODE'] == "IF") & filtered_df['CRTABLE'].isin(crtable_filter_basic_prumortgage))]
    value_prumortgage = [2]
    default_prumortgage = 5

    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'BASIC_PRUMORTGAGE', condition_prumortgage, value_prumortgage, default_prumortgage)
    
    condition_ccp = [((filtered_df['CSTATCODE'] == "IF") & filtered_df['CRTABLE'].isin(crtable_filter_basic_ccp))]
    value_ccp = [3]
    default_ccp = 5

    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'BASIC_CCP', condition_ccp, value_ccp, default_ccp)
    
    condition_ccpa = [((filtered_df['CSTATCODE'] == "IF") & filtered_df['CRTABLE'].isin(crtable_filter_basic_ccpa))]
    value_ccpa = [4]
    default_ccpa = 5

    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'BASIC_CCPA', condition_ccpa, value_ccpa, default_ccpa)

    filtered_df['BASIC'] = filtered_df[['BASIC_PRUTERM','BASIC_PRUMORTGAGE', 'BASIC_CCP', 'BASIC_CCPA']].min(axis = 1)

    filtered_df = filtered_df[(filtered_df['BASIC'] != 5) & (filtered_df['RECORD_TO_KEEP'] != 0)]

    pruterm_tab_df = filtered_df.groupby('CHDRNUM').agg({
            'BASIC': 'min'
    }).reset_index() 

    pruterm_tab_df = pruterm_tab_df.rename(columns={
        'BASIC': 'PRUTERM_BASIC'
    })

    pruterm_tab_df_stage0 = pruterm_tab_df[['CHDRNUM', 'PRUTERM_BASIC']]
    pruterm_tab_df_stage0 = pruterm_tab_df_stage0.sort_values(by = ['CHDRNUM', 'PRUTERM_BASIC'])

    del filtered_df, pruterm_tab_df
    return pruterm_tab_df_stage0

# Add 'PRUTERM_BASIC' column in the poldata_df
@comm_func.timer_func
def add_col_pruterm_basic(poldata_df):
    cnttype_filter_list = const_var.cnttype_pruterm_basic_list()
    
    crtable_filter_basic_pruterm = const_var.crtable_pruterm_list()
    crtable_filter_basic_prumortgage = const_var.crtable_prumortgage_list()
    crtable_filter_basic_ccp = const_var.crtable_ccp_list()
    crtable_filter_basic_ccpa = const_var.crtable_ccpa_list()

    filtered_df = poldata_df[(poldata_df['FILE'] == const_var.IF) & (poldata_df['CNTTYPE'].isin(cnttype_filter_list))]
    filtered_df = filtered_df.copy()

    condition_pruterm = [((filtered_df['CSTATCODE'] == "IF") & filtered_df['CRTABLE'].isin(crtable_filter_basic_pruterm))]
    value_pruterm = [1]
    default_pruterm = 5

    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'BASIC_PRUTERM', condition_pruterm, value_pruterm, default_pruterm)
    
    condition_prumortgage = [((filtered_df['CSTATCODE'] == "IF") & filtered_df['CRTABLE'].isin(crtable_filter_basic_prumortgage))]
    value_prumortgage = [2]
    default_prumortgage = 5

    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'BASIC_PRUMORTGAGE', condition_prumortgage, value_prumortgage, default_prumortgage)
    
    condition_ccp = [((filtered_df['CSTATCODE'] == "IF") & filtered_df['CRTABLE'].isin(crtable_filter_basic_ccp))]
    value_ccp = [3]
    default_ccp = 5

    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'BASIC_CCP', condition_ccp, value_ccp, default_ccp)
    
    condition_ccpa = [((filtered_df['CSTATCODE'] == "IF") & filtered_df['CRTABLE'].isin(crtable_filter_basic_ccpa))]
    value_ccpa = [4]
    default_ccpa = 5

    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'BASIC_CCPA', condition_ccpa, value_ccpa, default_ccpa)

    filtered_df['BASIC'] = filtered_df[['BASIC_PRUTERM','BASIC_PRUMORTGAGE', 'BASIC_CCP', 'BASIC_CCPA']].min(axis = 1)

    filtered_df = filtered_df[(filtered_df['BASIC'] != 5) & (filtered_df['RECORD_TO_KEEP'] != 0)]

    groupby_df = filtered_df.groupby('CHDRNUM').agg({
            'BASIC': 'min'
    }).reset_index() 

    groupby_df = groupby_df.rename(columns={
        'BASIC': 'PRUTERM_BASIC'
    })

    pruterm_tab_df = groupby_df[['CHDRNUM', 'PRUTERM_BASIC']]
    pruterm_tab_df.sort_values(by = ['CHDRNUM', 'PRUTERM_BASIC'])

    poldata_df = comm_func.add_merge_col(poldata_df, groupby_df, ['PRUTERM_BASIC'], ['CHDRNUM'], ['CHDRNUM'], 'left')

    poldata_df = comm_func.default_col_null_to_zero(poldata_df, ['PRUTERM_BASIC'])

    del filtered_df, groupby_df
    return poldata_df, pruterm_tab_df

# Updated 'ENHANCED_IND' column in the poldata_df
@comm_func.timer_func
def upd_col_enhanced_ind(poldata_df):
    crtable_filter_1 = const_var.crtable1_enhanced_ind_pruterm()
    crtable_filter_2 = const_var.crtable2_enhanced_ind_pruterm()
    crtable_filter_3 = const_var.crtable3_enhanced_ind_pruterm()
    crtable_filter_4 = const_var.crtable4_enhanced_ind_pruterm()

    poldata_df = poldata_df.rename(columns={
        'ENHANCED_IND': 'ENHANCED_IND_x2'
    })

    poldata_df['ENHANCED_IND'] = np.where(
        ((poldata_df['PRUTERM_BASIC'] == 1) & poldata_df['CRTABLE'].isin(crtable_filter_1)) | 
        ((poldata_df['PRUTERM_BASIC'] == 2) & poldata_df['CRTABLE'].isin(crtable_filter_2)) | 
        ((poldata_df['PRUTERM_BASIC'] == 3) & poldata_df['CRTABLE'].isin(crtable_filter_3)) | 
        ((poldata_df['PRUTERM_BASIC'] == 4) & poldata_df['CRTABLE'].isin(crtable_filter_4)),
        np.where(poldata_df['ENHANCED_IND_x2'] == 0, 1, poldata_df['ENHANCED_IND_x2']),
        poldata_df['ENHANCED_IND_x2']
    )

    return poldata_df

# Add 'PROD_NAME' column in the poldata_df
@comm_func.timer_func
def add_col_prod_name(poldata_df, dmtm_tbl, general_tbl):

    poldata_df = poldata_df.rename(columns={
        'WAIV_IND': 'Left_WAIV_IND'
    })

    DMTM_list = const_var.dmtm_filter_list()

    filtered_df_dmtm = poldata_df[poldata_df['CNTTYPE'].isin(DMTM_list)].copy()
    filtered_df_non_dmtm = poldata_df[~poldata_df['CNTTYPE'].isin(DMTM_list)].copy()

    dmtm_key = ['CNTTYPE', 'CRTABLE', 'BENPLN', 'ENHANCED_IND', 'CELLPC_IND']
    non_dmtm_key = ['CNTTYPE', 'CRTABLE', 'ENHANCED_IND', 'BANCA']

    filtered_df_dmtm = comm_func.add_merge_col(filtered_df_dmtm, dmtm_tbl, ['MPF_NAME'], dmtm_key, dmtm_key, 'left')

    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df_dmtm, ['MPF_NAME'], ['$RID'], ['$RID'], 'left')
    
    conditions = [poldata_df['CNTTYPE'].isin(DMTM_list), ~poldata_df['CNTTYPE'].isin(DMTM_list)]
    results = ["", poldata_df['EDP_PROD_NAME']]

    # Apply the conditions and results
    poldata_df['EDP_PROD_NAME'] = np.select(conditions, results)

    
    filtered_df_non_dmtm = comm_func.add_merge_col(filtered_df_non_dmtm, general_tbl, ['PRODUCTNAME', 'WAIV_IND'], non_dmtm_key, non_dmtm_key, 'left')

    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df_non_dmtm, ['PRODUCTNAME', 'WAIV_IND'], ['$RID'], ['$RID'], 'left')
    
    conditions = [poldata_df['CNTTYPE'].isin(DMTM_list), ~poldata_df['CNTTYPE'].isin(DMTM_list)]
    results = [poldata_df['MPF_NAME'], poldata_df['PRODUCTNAME']]

    poldata_df['PRODUCT_NAME'] = np.select(conditions, results)

    
    poldata_df = poldata_df.rename(columns={
        'Left_WAIV_IND': 'WAIV_IND',
        'WAIV_IND': 'Right_WAIV_IND'
    })

    condition = [((poldata_df['FUNDTYPE'] == 1) & (poldata_df['POLNO_IND'] == 1))]
    value = [poldata_df['EDP_PROD_NAME']]
    default = poldata_df['PRODUCT_NAME']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'PRODUCT_NAME', condition, value, default)

    PRODUCT_NAME_list = const_var.product_name_update_list()
    CRTABLE_list = const_var.crtable_product_name_update_list()

    poldata_df = poldata_df.copy()

    condition_CRTABLE = [(
        (poldata_df['FILE'].isin([const_var.IF, const_var.ET])) & 
        (poldata_df['PRODUCT_NAME'].isin(PRODUCT_NAME_list)) &
        ((poldata_df['CRTABLE'].isin(CRTABLE_list)) & (poldata_df['BASIC_PSM_IND'] != 0))
    )]
    value_CRTABLE = ["XXXXXX"]
    default_CRTABLE = poldata_df['PRODUCT_NAME']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'PRODUCT_NAME', condition_CRTABLE, value_CRTABLE, default_CRTABLE)
    
    poldata_df = comm_func.drop_col(poldata_df, 'PRODUCTNAME')
    poldata_df = comm_func.drop_col(poldata_df, 'MPF_NAME')

    del filtered_df_dmtm, filtered_df_non_dmtm
    return poldata_df
