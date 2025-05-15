import pandas as pd
import numpy as np
import common_functions as comm_func
from common_classes import Conditional
import constant_variables as const_var
from mpf_trad_schema import prophet_output_trad_schema
from time import time

# Add 'AGE_AT_ENTRY' column to poldata_df
@comm_func.timer_func
def add_col_age_at_entry(poldata_df):

    poldata_df = comm_func.add_col_valex(poldata_df, 'AGE_AT_ENTRY', 'AGE_AT_ENTRY_2')

    return poldata_df

# Add 'AS_CURR_MTH' column to poldata_df
@comm_func.timer_func
def add_col_as_curr_mth(poldata_df):

    poldata_df = comm_func.add_col_valex(poldata_df, 'AS_CURR_MTH', 'AS_CURR_MTH_2')

    return poldata_df

# Add 'AS_CURR_YEAR' column to poldata_df
@comm_func.timer_func
def add_col_as_curr_year(poldata_df):

    poldata_df = comm_func.add_col_valex(poldata_df, 'AS_CURR_YEAR', 'AS_CURR_YEAR_2')

    return poldata_df

# Add 'MAT_BEN_PP' column to poldata_df
@comm_func.timer_func
def add_col_mat_ben_pp(poldata_df):

    poldata_df = comm_func.add_col_valex(poldata_df, 'MAT_BEN_PP', 'MAT_BEN_PP_2')
    poldata_df['MAT_BEN_PP'] = poldata_df['MAT_BEN_PP'].round(3)

    return poldata_df

# Add 'PREM_PAYBL_Y' column to poldata_df
@comm_func.timer_func
def add_col_prem_payabl_y(poldata_df):

    poldata_df = comm_func.add_col_valex(poldata_df, 'PREM_PAYBL_Y', 'PREM_PAYBL_Y_2')

    return poldata_df

# Add 'BFT_PAYBL_Y' column to poldata_df
@comm_func.timer_func
def add_col_bft_paybl_y(poldata_df):

    product_list = const_var.bft_paybl_y_list()

    conditions = [
        (poldata_df['PRODUCT_NAME'].isin(product_list) & (poldata_df['VAR_IND'] == 2)),
        (poldata_df['PRODUCT_NAME'].isin(product_list) & (poldata_df['VAR_IND'] != 2))
    ] 

    results = [
        85 - poldata_df['AGE'],
        poldata_df['RCESTRM']
    ]

    poldata_df['BFT_PAYBL_Y'] = np.select(conditions, results, default = 0)

    return poldata_df

# Update ANNUAL_PREM column to ANNUAL_PREM_2
@comm_func.timer_func
def upd_col_annual_prem(poldata_df):

    poldata_df = poldata_df.rename(columns={
        'ANNUAL_PREM': 'ANNUAL_PREM_x0'
    })

    poldata_df = comm_func.add_col_valex(poldata_df, 'ANNUAL_PREM', 'ANNUAL_PREM_2')

    return poldata_df

# Update SUM_ASSURED column by product name
@comm_func.timer_func
def upd_col_sum_assured_prod(poldata_df):

    poldata_df = poldata_df.rename(columns={
        'SUM_ASSURED': 'SUM_ASSURED_x3'
    })
    product_list1 = const_var.sum_assured_prod_list1()
    product_list2 = const_var.sum_assured_prod_list2()

    conditions = [
        poldata_df['PRODUCT_NAME'].isin(product_list1),
        poldata_df['PRODUCT_NAME'].isin(product_list2),
        (poldata_df['PRODUCT_NAME'] == "C5WIC_")
    ]

    results = [
        (poldata_df['SUMINS'] / 50),
        (poldata_df['SUMINS'] / 10),
        (poldata_df['SUMINS'] * 100)
    ]

    poldata_df['SUM_ASSURED'] = np.select(conditions, results, default = poldata_df['SUM_ASSURED_x3'])

    return poldata_df

# Add 'POLT' column to filtered data
@comm_func.timer_func
def add_col_polt(poldata_df):

    filtered_df = poldata_df[(poldata_df['FILE'] == "ET") | ((poldata_df['FILE'] == "EX") & (poldata_df['STATCODE'] == "CF") & (poldata_df['ZMOVECDE'] == 2))]
    filtered_df = filtered_df.copy()
    filtered_df['POLT'] = filtered_df['CHDRNUM'].astype(str) + " | " + filtered_df['CNTTYPE'].astype(str) + " | " + filtered_df['CRTABLE'].astype(str)

    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, ['POLT'], ['$RID'], ['$RID'], 'left')
    

    del filtered_df
    return poldata_df

# Add 'CFSTAT' column to poldata_df
@comm_func.timer_func
def add_col_cfstat(poldata_df):

    filtered_df = poldata_df[((poldata_df['FILE'] == "EX") & (poldata_df['STATCODE'] == "CF") & (poldata_df['ZMOVECDE'] == 2))]
    polt_list = filtered_df['POLT'].tolist()
    
    condition = [poldata_df['POLT'].isin(polt_list)]
    value = ["CF"]
    default = ""
    poldata_df = poldata_df.copy()
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'CFSTAT', condition, value, default)

    return poldata_df

# Add 'SPCODE' column to poldata_df
@comm_func.timer_func
def add_col_spcode(poldata_df):

    product_list = const_var.spcode_prod_list()
    CNTTYPE_list = const_var.cnttype_prod_list()

    conditions = [
        ((poldata_df['FILE'] == const_var.ET) & 
         ((poldata_df['CNTTYPE'].isin(CNTTYPE_list)) | (poldata_df['AGNTNUM'].str[:1] == "9"))),

        ((poldata_df['FILE'] == const_var.ET) & (poldata_df['PRODUCT_NAME'].isin(product_list)) & ((poldata_df['ZMOVECDE'] == 2) & (poldata_df['CFSTAT'] != "CF"))),

        ((poldata_df['FILE'] == const_var.ET)),

        ((poldata_df['FILE'] == const_var.IF) &
         ((poldata_df['CNTTYPE'].isin(CNTTYPE_list)) | (poldata_df['AGNTNUM'].str[:1] == "9"))),
        
        ((poldata_df['FILE'] == const_var.IF) & (poldata_df['AGNTNUM'].str[:1] == "6")),

        (poldata_df['FILE'] == const_var.IF)
    ]

    results = [
        71,
        71,
        51,
        21,
        41,
        1
    ]

    poldata_df['SPCODE'] = np.select(conditions, results, default = 0)

    return poldata_df

# Update 'SPCODE' column based on SP_IND
@comm_func.timer_func
def upd_col_spcode_spind(poldata_df):

    poldata_df = poldata_df.rename(columns={
        'SPCODE': 'SPCODE_x0'
    })

    conditions = [
        ((poldata_df['FILE'] == const_var.IF) & (poldata_df['FUNDTYPE'] == 1) &
         ((poldata_df['SPCODE_x0'] > 50) | (poldata_df['PRODUCT_NAME'].str[:2] == "C6") |
          (poldata_df['SP_IND'] < 3) | (poldata_df['SP_IND'] > 100))),
        
        ((poldata_df['FILE'] == const_var.IF) & (poldata_df['FUNDTYPE'] == 1))
    ]

    results = [
        poldata_df['SPCODE_x0'],
        poldata_df['SP_IND']
    ]

    poldata_df['SPCODE'] = np.select(conditions, results, default = poldata_df['SPCODE_x0'])

    return poldata_df

# Update 'SPCODE' column based on DMTM
@comm_func.timer_func
def upd_col_spcode_dmtm(poldata_df):

    dmtm_list = const_var.dmtm_spcode_list()

    condition = [((poldata_df['PRODUCT_NAME'].isin(dmtm_list)) & ((poldata_df['CSTATCODE'] == "IF") | (poldata_df['CSTATCODE'] == "PU")) & (poldata_df['FILE'] == const_var.IF))]
    value = [21]
    default = poldata_df['SPCODE']
    poldata_df = poldata_df.copy()
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'SPCODE', condition, value, default)

    return poldata_df

# Update 'SPCODE' column based on ENTRY_YEAR
@comm_func.timer_func
def upd_col_spcode_entryyear(poldata_df, val_date_const):
    val_year = val_date_const.year

    condition = [((poldata_df['FILE'] == const_var.IF) & (poldata_df['ENTRY_YEAR'] == val_year))]
    value = [(poldata_df['SPCODE'] + 1)]
    default = poldata_df['SPCODE']
    poldata_df = poldata_df.copy()
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'SPCODE', condition, value, default)

    return poldata_df

# Call the above functions to update SPCODE
@comm_func.timer_func
def upd_col_spcode(poldata_df, val_date_const):

    poldata_df = upd_col_spcode_spind(poldata_df)
    poldata_df = upd_col_spcode_dmtm(poldata_df)
    poldata_df = upd_col_spcode_entryyear(poldata_df, val_date_const)

    return poldata_df

# Update 'ANNUAL_PREM', 'SUM_ASSURED', 'PRODUCT_NAME' and 'RECORD_TO_KEEP' column for Compassionate Allowance policies in poldata_df
@comm_func.timer_func
def upd_col_for_ca(poldata_df):

    poldata_df = poldata_df.rename(columns={
        'ANNUAL_PREM': 'ANNUAL_PREM_x1',
        'SUM_ASSURED': 'SUM_ASSURED_x4',
        'PRODUCT_NAME': 'PRODUCT_NAME_x1'
    })

    filtered_base_df = poldata_df[(poldata_df['FILE'] == const_var.IF) & (poldata_df['VAR_IND'].isin([3, 4]))]
    filtered_nonCA_df = poldata_df[~((poldata_df['FILE'] == const_var.IF) & (poldata_df['VAR_IND'].isin([3, 4])))]

    filtered_ca_df = filtered_base_df.copy()
    
    # Update for CA policies
    filtered_ca_df['ANNUAL_PREM'] = 7.3
    filtered_ca_df['SUM_ASSURED'] = 2000
    filtered_ca_df['RECORD_TO_KEEP'] = 0.5

    conditions = [
        ((filtered_ca_df['VAR_IND'] == 3) & (filtered_ca_df['FUNDTYPE'] == 1)),
        (filtered_ca_df['VAR_IND'] == 3),
        ((filtered_ca_df['VAR_IND'] == 4) & (filtered_ca_df['FUNDTYPE'] == 1)),
        (filtered_ca_df['VAR_IND'] == 4)
    ]

    results = [
        "C3LTRM",
        "C3LTRN",
        "C4LTAR",
        "C4LTNR"
    ]

    filtered_ca_df['PRODUCT_NAME'] = np.select(conditions, results, default = "")

    # Update for base policies
    condition = [(filtered_base_df['ANNUAL_PREM_x1'] != 0)]
    value = [(filtered_base_df['ANNUAL_PREM_x1'] - 7.3)]
    default = filtered_base_df['ANNUAL_PREM_x1']
    filtered_base_df = filtered_base_df.copy()
    filtered_base_df = comm_func.add_conditional_column_to_df(filtered_base_df, 'ANNUAL_PREM', condition, value, default)

    filtered_base_df['SUM_ASSURED'] = filtered_base_df['SUM_ASSURED_x4']
    filtered_base_df['RECORD_TO_KEEP'] = 0.5
    filtered_base_df['PRODUCT_NAME'] = filtered_base_df['PRODUCT_NAME_x1']

    # Copy for the additional columns for the nonCA set of data
    filtered_nonCA_df = filtered_nonCA_df.copy()
    filtered_nonCA_df['ANNUAL_PREM'] = filtered_nonCA_df['ANNUAL_PREM_x1']
    filtered_nonCA_df['SUM_ASSURED'] = filtered_nonCA_df['SUM_ASSURED_x4']
    filtered_nonCA_df['PRODUCT_NAME'] = filtered_nonCA_df['PRODUCT_NAME_x1']

    poldata_df = pd.concat([filtered_ca_df, filtered_base_df, filtered_nonCA_df])
    poldata_df['ANNUAL_PREM'] = comm_func.apply_rounding_on_col(poldata_df, 'ANNUAL_PREM', 2)
    del filtered_ca_df, filtered_base_df, filtered_nonCA_df
    return poldata_df


# Add 'WLIST_TOTAL_PREM' column to poldata_df
@comm_func.timer_func
def add_col_wlist_total_prem(poldata_df):

    filtered_df = poldata_df[(((poldata_df['FILE'] == const_var.IF) & (poldata_df['PRODUCT_NAME'].str[:1] == "C")) &
                              ((poldata_df['CSTATCODE'] == "IF") | (poldata_df['CSTATCODE'] == "PU")) &
                              ((poldata_df['WIF_IND'] == 1) & (poldata_df['FUNDTYPE'] == 1))) &
                              
                              (poldata_df['RECORD_TO_KEEP'] != 0)]
    filtered_df = filtered_df.copy()
    
    conditions = [filtered_df['WAIV_IND'] == 1, filtered_df['WAIV_IND'] != 1]
    results = [filtered_df['ANNUAL_PREM'] + filtered_df['CC_ANN_PREM'], 0]
    
    # 24/9 WC: Updated to reflect testing from XLT
    filtered_df['WLIST_TOTAL_PREM'] = np.select(conditions, results)

    groupby_key_list = ['CHDRNUM', 'CNTTYPE']

    grouped_df = comm_func.add_groupby_col(filtered_df, groupby_key_list, 'WLIST_TOTAL_PREM', 'WLIST_TOTAL_PREM_sum', "sum")
    grouped_df = grouped_df[['$RID', 'WLIST_TOTAL_PREM_sum']]
    grouped_df = grouped_df.drop_duplicates()

    poldata_df = comm_func.add_merge_col(poldata_df, grouped_df, ['WLIST_TOTAL_PREM_sum'], ['$RID'], ['$RID'], 'left')      
    
    poldata_df = poldata_df.rename(columns={
        'WLIST_TOTAL_PREM_sum': 'WLIST_TOTAL_PREM'
    })

    del filtered_df
    return poldata_df

# Add 'ADD_WPREM' column to poldata_df
@comm_func.timer_func
def add_col_add_wprem(poldata_df):

    poldata_df['ADD_WPREM'] = (poldata_df['TOTAL_WAIVED_PREM'] - poldata_df['WLIST_TOTAL_PREM'])

    return poldata_df

# Update 'WAIVED_PREM' column to poldata_df
@comm_func.timer_func
def upd_col_waived_prem(poldata_df):

    poldata_df = poldata_df.rename(columns={
        'WAIVED_PREM': 'WAIVED_PREM_x0'
    })

    condition = [((poldata_df['WAIV_IND'] == 1) & (poldata_df['WIF_IND'] == 1))]
    value = [(poldata_df['ANNUAL_PREM'] + poldata_df['CC_ANN_PREM'])]
    default = 0.0

    poldata_df = poldata_df.copy()
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'WAIVED_PREM', condition, value, default)

    condition_2 = [(poldata_df['BASIC_IND'] == 1)]
    value_2 = [np.maximum(0, (poldata_df['WAIVED_PREM'] + poldata_df['ADD_WPREM']))]
    default_2 = poldata_df['WAIVED_PREM']

    poldata_df = poldata_df.copy()
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'WAIVED_PREM', condition_2, value_2, default_2)

    condition_3 = [((poldata_df['FUNDTYPE'] == 1) & 
                   ((poldata_df['WAIV_IND'] == 1) & (poldata_df['WIF_IND'] == 1)))]
    value_3 = [poldata_df['WAIVED_PREM']]
    default_3 = 0.0

    poldata_df = poldata_df.copy()
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'WAIVED_PREM', condition_3, value_3, default_3)

    return poldata_df

# Update 'RI_CED_CIC1' and 'RI_CED_CIC2' columns in poldata_df
@comm_func.timer_func
def upd_col_ri_ced(poldata_df, ri_ced_trad_tbl):

    poldata_df = poldata_df.rename(columns={
        'RI_CED_CIC1': 'RI_CED_CIC1_x0',
        'RI_CED_CIC2': 'RI_CED_CIC2_x0'
    })

    ri_ced_trad_tbl = comm_func.add_groupby_col(ri_ced_trad_tbl, 'CHDRNUM', 'RI_CED_CIC1', 'RI_CED_CIC1_sum', 'sum')
    ri_ced_trad_tbl = comm_func.add_groupby_col(ri_ced_trad_tbl, 'CHDRNUM', 'RI_CED_CIC2', 'RI_CED_CIC2_sum', 'sum')
    ri_ced_trad_tbl = ri_ced_trad_tbl[['CHDRNUM', 'RI_CED_CIC1_sum', 'RI_CED_CIC2_sum']]
    ri_ced_trad_tbl = ri_ced_trad_tbl.drop_duplicates()

    filtered_df = poldata_df[(poldata_df['PRODUCT_NAME'] == "CNPLTR") & (poldata_df['RECORD_TO_KEEP'] != 0)]

    filtered_df = comm_func.add_merge_col(filtered_df, ri_ced_trad_tbl, ['RI_CED_CIC1_sum', 'RI_CED_CIC2_sum'], ['CHDRNUM'], ['CHDRNUM'], 'left')

    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, ['RI_CED_CIC1_sum', 'RI_CED_CIC2_sum'], ['$RID'], ['$RID'], 'left')

    poldata_df = comm_func.default_col_null_to_string_or_col(poldata_df, 'RI_CED_CIC1_sum', poldata_df['RI_CED_CIC1_x0'])
    poldata_df = comm_func.default_col_null_to_string_or_col(poldata_df, 'RI_CED_CIC2_sum', poldata_df['RI_CED_CIC2_x0'])
    
    poldata_df = poldata_df.rename(columns={
        'RI_CED_CIC1_sum': 'RI_CED_CIC1',
        'RI_CED_CIC2_sum': 'RI_CED_CIC2'
    })

    del filtered_df
    return poldata_df

# Output of CA E2E Table
@comm_func.timer_func
def output_ca_summary(poldata_df):

    filtered_df = poldata_df[poldata_df['VAR_IND'].isin([3, 4, 5])]
    filtered_df = filtered_df.copy()
    
    condition = [(filtered_df['ANNUAL_PREM'] != 0)]
    value = [1]
    default = 0
    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'CA_SPLIT_IND', condition, value, default)

    condition_annual_prem = [filtered_df['VAR_IND'].isin([3, 4])]
    value_annual_prem = [7.3]
    default_annual_prem = filtered_df['ANNUAL_PREM']
    # filtered_df = filtered_df.copy()
    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'ANNUAL_PREM', condition_annual_prem, value_annual_prem, default_annual_prem)

    condition_sum_assured = [filtered_df['VAR_IND'].isin([3, 4])]
    value_sum_assured = [2000]
    default_sum_assured = filtered_df['COMP_BEN_SA']
    # filtered_df = filtered_df.copy()
    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'SUM_ASSURED', condition_sum_assured, value_sum_assured, default_sum_assured)

    filtered_df['RECORD_MPF'] = 1

    groupby_key_list = ['FILE','CA_SPLIT_IND', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE']

    filtered_df = comm_func.add_groupby_col(filtered_df, groupby_key_list, 'RECORD_MPF', "Sum_RECORD_MPF", "sum")
    filtered_df = comm_func.add_groupby_col(filtered_df, groupby_key_list, 'ANNUAL_PREM', "Sum_ANNUAL_PREM", "sum")
    filtered_df = comm_func.add_groupby_col(filtered_df, groupby_key_list, 'SUM_ASSURED', "Sum_SUM_ASSURED", "sum")
    filtered_df = filtered_df.drop_duplicates(['FILE', 'CA_SPLIT_IND', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE', 'Sum_RECORD_MPF', 'Sum_ANNUAL_PREM', 'Sum_SUM_ASSURED'])

    filtered_df = filtered_df[['FILE', 'CA_SPLIT_IND', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE', 'Sum_RECORD_MPF', 'Sum_ANNUAL_PREM', 'Sum_SUM_ASSURED']]
    filtered_df = filtered_df.sort_values(by = ['FILE', 'CA_SPLIT_IND', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE'])

    return filtered_df

# Update SPCODE, DURATIONIF_M and INIT_DECB_IF column for PAR CM 
@comm_func.timer_func
def upd_col_par_cm(poldata_df):
   
    # Archive existing column
    filtered_df = poldata_df.rename(columns={
        'SPCODE': 'SPCODE_x1',
        'DURATIONIF_M': 'DURATIONIF_M_x0',
        'INIT_DECB_IF': 'INIT_DECB_IF_x0'
    })

    filtered_df = filtered_df.copy()

    filtered_df = filtered_df[
        (((filtered_df['FILE'] == const_var.ET) & (filtered_df['ZMOVECDE'] == 2) &
                   (filtered_df['CFSTAT'] != "CF") & (filtered_df['PRODUCT_NAME'].str[:1] == "C")) |
                
                   ((filtered_df['FILE'] == const_var.IF) & (filtered_df['PRODUCT_NAME'].str[:1] == "C") &
                   ((filtered_df['CSTATCODE'] == "IF") | (filtered_df['CSTATCODE'] == "PU"))))]

    par_cm_crtable_list = ['6CBB', '6DEB', 'CM01']
    
    condition_par_cm_df = filtered_df[(filtered_df['SPCODE_x1'] >= 51) &
                                   ((filtered_df['FUND_IND'] == 'PAR') & 
                                   ((filtered_df['BASIC_IND'] == 1) | (filtered_df['CRTABLE'].isin(par_cm_crtable_list))))]

    for i in range(0, 12):
       
        if i == 0:
            temp_df = condition_par_cm_df.copy()
            temp_df['SPCODE_2'] = temp_df['SPCODE_x1']
            par_cm_df = temp_df
            
        else:
            temp2_df = condition_par_cm_df.copy()
            temp2_df['SPCODE_2'] = temp2_df['SPCODE_x1'] + i
            par_cm_df = pd.concat([par_cm_df, temp2_df])
    
    else:
        pass

    condition_durationif = [(par_cm_df['SPCODE_x1'] != par_cm_df['SPCODE_2'])]
    value_durationif = [0]
    default_durationif = par_cm_df['DURATIONIF_M_x0']
    par_cm_df = par_cm_df.copy()
    par_cm_df = comm_func.add_conditional_column_to_df(par_cm_df, 'DURATIONIF_M_temp', condition_durationif, value_durationif, default_durationif)

    condition_init_decb = [(par_cm_df['SPCODE_x1'] != par_cm_df['SPCODE_2'])]
    value_init_decb = [0.0]
    default_init_decb = par_cm_df['INIT_DECB_IF_x0']
    par_cm_df = par_cm_df.copy()
    par_cm_df = comm_func.add_conditional_column_to_df(par_cm_df, 'INIT_DECB_IF_temp', condition_init_decb, value_init_decb, default_init_decb)

    output_col = ['DURATIONIF_M_temp', 'INIT_DECB_IF_temp', 'SPCODE_2']
    filtered_df = comm_func.add_merge_col(filtered_df, par_cm_df, output_col, ['$RID'], ['$RID'], 'left')

    filtered_duration = [(filtered_df['DURATIONIF_M_temp'].notnull() & (filtered_df['DURATIONIF_M_temp'] != filtered_df['DURATIONIF_M_x0']))]
    value_duration = [filtered_df['DURATIONIF_M_temp']]
    default_duration = filtered_df['DURATIONIF_M_x0']
    filtered_df = filtered_df.copy()
    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'DURATIONIF_M', filtered_duration, value_duration, default_duration)

    filtered_value_init = [(filtered_df['INIT_DECB_IF_temp'].notnull() & (filtered_df['INIT_DECB_IF_temp'] != filtered_df['INIT_DECB_IF_x0']))]
    value_init = [filtered_df['INIT_DECB_IF_temp']]
    default_init = filtered_df['INIT_DECB_IF_x0']
    filtered_df = filtered_df.copy()
    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'INIT_DECB_IF', filtered_value_init, value_init, default_init)

    filtered_spcode = [(filtered_df['SPCODE_2'].notnull() & (filtered_df['SPCODE_x1'] != filtered_df['SPCODE_2']))]
    value_spcode = [filtered_df['SPCODE_2']]
    default_spcode = filtered_df['SPCODE_x1']
    filtered_df = filtered_df.copy()
    filtered_df = comm_func.add_conditional_column_to_df(filtered_df, 'SPCODE', filtered_spcode, value_spcode, default_spcode)

    filtered_df = comm_func.drop_col(filtered_df, ['SPCODE_2', 'DURATIONIF_M_temp', 'INIT_DECB_IF_temp'])

    del condition_par_cm_df, par_cm_df
    return filtered_df

# Add VAL_TYPE column for NPAR, PAR_CM and PAR_PROJ
@comm_func.timer_func
def add_col_val_type_proj(poldata_df):

    filtered_df = upd_col_par_cm(poldata_df)

    filtered_df = filtered_df.copy()
    filtered_df = filtered_df[(filtered_df['FUNDTYPE'] == 1)]
    filtered_df['VAL_TYPE'] = "PAR_CM"

    proj_df = poldata_df[(((poldata_df['FILE'] == const_var.ET) & (poldata_df['ZMOVECDE'] == 2) &
                   (poldata_df['CFSTAT'] != "CF") & (poldata_df['PRODUCT_NAME'].str[:1] == "C")) |
                
                   ((poldata_df['FILE'] == const_var.IF) & (poldata_df['PRODUCT_NAME'].str[:1] == "C") &
                   ((poldata_df['CSTATCODE'] == "IF") | (poldata_df['CSTATCODE'] == "PU"))))] 


    conditions = [
        proj_df['FUNDTYPE'].isin([2, 3]),
        proj_df['FUNDTYPE'] == 1
    ]


    results = [
        "NPAR",
        "PAR_PROJ"
    ]

    proj_df = proj_df.copy()
    proj_df['VAL_TYPE'] = np.select(conditions, results, default = "")
    proj_df= comm_func.add_col_valex(proj_df, 'DURATIONIF_M_x0', 'DURATIONIF_M')
    proj_df= comm_func.add_col_valex(proj_df, 'INIT_DECB_IF_x0', 'INIT_DECB_IF')
    proj_df= comm_func.add_col_valex(proj_df, 'SPCODE_x1', 'SPCODE')
    
    poldata_df = pd.concat([proj_df, filtered_df])
    poldata_df = poldata_df.sort_values(['PRODUCT_NAME', 'SPCODE', 'POL_NUMBER'])
    
    del proj_df, filtered_df
    return poldata_df

# Add 3 IFRS17 columns
@comm_func.timer_func
def add_ifrs17_columns(poldata_df, benefit_code_tbl):

    # Creating POL_NO_IFRS17
    # Converting POL_NUMBER to string
    # 24/9 WC: Updated to remove dataframe fragmentation error
    pol_no_ifrs17_series = poldata_df['POL_NUMBER'].astype(str).str.zfill(8).rename('POL_NO_IFRS17')
    basic_entry_year_series = poldata_df['ISSUE_YEAR'].rename('BASIC_ENTRY_YEAR')
    basic_entry_month_series = poldata_df['ISSUE_MONTH'].rename('BASIC_ENTRY_MONTH')

    poldata_df = pd.concat([poldata_df] + [pol_no_ifrs17_series, basic_entry_year_series, basic_entry_month_series], axis =1)

    output_col = ['PROD_CD', 'BENEFIT_CODE']
    left_key = ['CNTTYPE', 'CRTABLE']
    right_key = ['CNTTYPE', 'CRTABLE']

    poldata_df= comm_func.add_merge_col(poldata_df, benefit_code_tbl, output_col, left_key, right_key, 'left')
    
    return poldata_df

# Read C_Lib_B
@comm_func.timer_func
def groupby_clib_b(poldata_df, clib_list_tbl):
    
    # Obtain actions
    GroupByList = clib_list_tbl[((clib_list_tbl['Cat'] == "C_Lib_B") & (clib_list_tbl['Action'] == "GroupBy"))]['Field'].tolist()
    
    # Dropping of unused records to ensure groupby function is only taking into account the policies needed
    poldata_df = poldata_df[poldata_df['RECORD_TO_KEEP'] != 0]
    poldata_df = poldata_df.groupby(GroupByList, dropna = False).agg({
        'AGE_AT_ENTRY': 'mean',
        'SEX': 'mean',
        'SMOKER_STAT': 'mean',
        'ENTRY_MONTH': 'mean',
        'ENTRY_YEAR': 'mean',
        'POL_TERM_Y': 'mean',
        'BFT_PAYBL_Y': 'mean',
        'PREM_PAYBL_Y': 'mean',
        'PREM_MODE': 'mean',
        'ANNUAL_PREM': 'sum',
        'SINGLE_PREM': 'sum',
        'SUBSTD_PREM': 'mean',
        'SUM_ASSURED': 'sum',
        'DEATH_BEN': 'mean',
        'DURATIONIF_M': 'mean',
        'INIT_POLS_IF': 'mean',
        'INIT_DECB_IF': 'sum',
        'SN_LOAN': 'mean',
        'PLAN_CODE': 'mean',
        'CC_ANN_PREM': 'sum',
        'CC_SUM_ASSD': 'sum',
        'CC_TERM_Y': 'mean',
        'GN_INTRIM_RB': 'mean',
        'GN_REVBON_LY': 'mean',
        'PUPINDICATOR': 'mean',
        'INIT_AS_IF': 'mean',
        'INIT_AS_SURD': 'mean',
        'AS_CURR_MTH': 'mean',
        'AS_CURR_YEAR': 'mean',
        'INIT_AGC_IF': 'mean',
        'AGE2_ATENTRY': 'mean',
        'SEX2': 'mean',
        'SMOKER2_STAT': 'mean',
        'FUNDTYPE': 'mean',
        'DEFER_PER_Y': 'mean',
        'MORT_INT_PC': 'mean',
        'OCC_CLASS': 'mean',
        'NONPAR_PREM': 'sum',
        'LINK_PREM': 'sum',
        'RIDER_BEN_PP': 'mean',
        'MAT_BEN_PP': 'sum',
        'G_MAT_BEN_PP': 'mean',
        'SA_IND': 'mean',
        'IND_PC': 'mean',
        'WAIVED_STAT': 'mean',
        'CI_DURIF_M': 'mean',
        'NO_LS_CLAIM': 'mean',
        'CIS_I': 'mean',
        'RECV_I': 'mean',
        'PREG_I': 'mean',
        'BABY_I': 'mean',
        'OVA_I': 'mean',
        'LOST_I': 'mean',
        'MOM_I': 'mean',
        'DEL_I': 'mean',
        'ISSUE_MONTH': 'mean',
        'ISSUE_YEAR': 'mean',
        'MME_PREM': 'sum',
        'SERIES_IND': 'mean',
        'RETIREMENT_AGE': 'mean',
        'YTD_COST_TB': 'mean',
        'WAIV_TYPE': 'mean',
        'COMM_IND': 'mean',
        'COMP_BEN_SA': 'sum',
        'INCSA_PCT': 'mean',
        'HB_SUM_ASSD': 'sum',
        'SURR_FAC_1': 'sum',
        'CIC_BEN_PP': 'sum',
        'ACC_SUM_ASSD': 'sum',
        'CC2_SUM_ASSD': 'sum',
        'LS_SUM_ASSD': 'sum',
        'AMR_SA_PP': 'sum',
        'ROP_MAT_PC': 'sum',
        'HC_SUM_ASSD': 'sum',
        'INCR_BEN': 'sum',
        'YI_DTH_TPD': 'sum',
        'SB_BEN': 'sum',
        'ADEN_BEN': 'sum',
        'ECA_BEN1': 'sum',
        'ECA_BEN2': 'sum',
        'SB_S_YR': 'mean',
        'WAIVED_IND': 'min',
        'WAIVED_PREM': 'sum',
        'WAIVED_TERM': 'max',
        'RI_CED_ACHL1': 'sum',
        'RI_CED_ADTL1': 'sum',
        'RI_CED_ADTL2': 'sum',
        'RI_CED_ADTL3': 'sum',
        'RI_CED_ADTL4': 'sum',
        'RI_CED_CIC1': 'sum',
        'RI_CED_CIC2': 'sum',
        'RI_CED_DEATH': 'sum',
        'RI_CED_COMP_BEN': 'sum',
        'BASIC_ENTRY_MONTH': 'mean',
        'BASIC_ENTRY_YEAR': 'mean'
    }).reset_index()

    # sort the result in ascending order
    poldata_df = poldata_df.sort_values(by = ['PRODUCT_NAME', 'SPCODE', 'POL_NUMBER'])

    round_integer_list = ['POL_TERM_Y', 'AS_CURR_YEAR', 'AS_CURR_MTH', 'G_MAT_BEN_PP', 'PREM_PAYBL_Y', 'AGE_AT_ENTRY', 'ENTRY_YEAR', 'BFT_PAYBL_Y', 'BASIC_ENTRY_YEAR', 'AS_CURR_YEAR']

    for i in round_integer_list:
        # poldata_df[i] = poldata_df[i].round(0)
        poldata_df[i] = comm_func.apply_rounding_on_col(poldata_df, i, 0)
    
    poldata_df = poldata_df.rename(columns={
        'RI_CED_ACHL1': 'RI_CED_ACHL1_x0',
        'RI_CED_ADTL1': 'RI_CED_ADTL1_x0',
        'RI_CED_ADTL2': 'RI_CED_ADTL2_x0',
        'RI_CED_ADTL3': 'RI_CED_ADTL3_x0',
        'RI_CED_ADTL4': 'RI_CED_ADTL4_x0',
        'RI_CED_CIC1': 'RI_CED_CIC1_x0',
        'RI_CED_CIC2': 'RI_CED_CIC2_x0',
        'RI_CED_DEATH': 'RI_CED_DEATH_x0'
    })

    fund_list = ['RI_CED_ACHL1', 'RI_CED_ADTL1', 'RI_CED_ADTL2', 'RI_CED_ADTL3', 'RI_CED_ADTL4', 
                 'RI_CED_CIC1', 'RI_CED_CIC2', 'RI_CED_DEATH']
    
    for i in fund_list:
        
        condition = [(poldata_df['FUNDTYPE'] == 1)]
        value = [0.0]
        default = poldata_df[i + "_x0"]
        poldata_df = poldata_df.copy()
        poldata_df = comm_func.add_conditional_column_to_df(poldata_df, i, condition, value, default)

    poldata_df = poldata_df.drop_duplicates()
    replace_list = const_var.data_cleansing_list()
    
    for i in replace_list:

        if (poldata_df[i].dtype == int):
            conditions = [poldata_df[i].isnull()]
            results = [0]
            default = poldata_df[i]
            poldata_df[i] = np.select(conditions, results, default)

        elif (poldata_df[i].dtype == float):
            conditions = [poldata_df[i].isnull()]
            results = [0.0]
            default = poldata_df[i]
            poldata_df[i] = np.select(conditions, results, default)

        elif (poldata_df[i].dtype == str):
            conditions = [poldata_df[i].isnull()]
            results = [""]
            default = poldata_df[i]
            poldata_df[i] = np.select(conditions, results)

        else:
            pass       
            
    return poldata_df

# Map PAR_CHANNEL column to poldata_df
@comm_func.timer_func
def add_col_par_channel(poldata_df, par_channel_tbl):

    output_col = ['PAR_CHANNEL']
    left_key = ['SPCODE']
    right_key = ['SPCode']
    join_type = 'left'

    poldata_df = comm_func.add_merge_col(poldata_df, par_channel_tbl, output_col, left_key, right_key, join_type)
    poldata_df = comm_func.drop_col(poldata_df, 'SPCode')

    return poldata_df

# Map PAR_Table and CNTTYPE column to poldata_df
@comm_func.timer_func
def add_col_par_table(poldata_df, par_table_category_tbl):

    conditions = [par_table_category_tbl['CNTTYPE'].isna()]
    results = ["N/A"]
    default = par_table_category_tbl['CNTTYPE']
    par_table_category_tbl['CNTTYPE'] = np.select(conditions, results, default)
    
    output_col = ['CNTTYPE', 'PAR_Table']
    left_key = ['PRODUCT_NAME', 'PAR_CHANNEL']
    right_key = ['PRODUCT_NAME', 'PAR_CHANNEL']
    join_type = 'left'

    poldata_df = comm_func.add_merge_col(poldata_df, par_table_category_tbl, output_col, left_key, right_key, join_type)

    conditions = [(poldata_df['PAR_Table'].isnull())]
    results = [0]
    default = poldata_df['RECORD_TO_KEEP']

    poldata_df = poldata_df.copy()
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'RECORD_TO_KEEP', conditions, results, default)

    return poldata_df

# Add RETIREYR and PAYOUT column to poldata_df
@comm_func.timer_func
def add_col_retireyr_payout(poldata_df):

    poldata_df['RETIREYR'] = poldata_df['ENTRY_YEAR'] + poldata_df['RETIREMENT_AGE'] - poldata_df['AGE_AT_ENTRY']
    poldata_df['PAYOUT'] = poldata_df['POL_TERM_Y'] - poldata_df['RETIREMENT_AGE'] + poldata_df['AGE_AT_ENTRY']

    return poldata_df

# Map PAR_COHORT_INDEX column to poldata_df
@comm_func.timer_func
def add_col_par_cohort_index(poldata_df, par_table_tbl, par_channel_tbl, par_table_category_tbl):
    poldata_df = add_col_par_channel(poldata_df, par_channel_tbl)     # Only for mapping of PAR_COHORT_INDEX, column will be dropped
    poldata_df = add_col_par_table(poldata_df, par_table_category_tbl)    # Only for mapping of PAR_COHORT_INDEX, column will be dropped
    poldata_df = add_col_retireyr_payout(poldata_df) # Only for mapping of PAR_COHORT_INDEX, column will be dropped

    par_table_df = par_table_tbl[(par_table_tbl['PAR_Table'] == "PAR_Table")]
    par_table_pht_df = par_table_tbl[(par_table_tbl['PAR_Table'] == "PAR_Table_PHT")]
    par_table_psr_df = par_table_tbl[(par_table_tbl['PAR_Table'] == "PAR_Table_PSR")]
    par_table_pst_df = par_table_tbl[(par_table_tbl['PAR_Table'] == "PAR_Table_PST")]

    poldata_par_table_df = poldata_df[poldata_df['PAR_Table'] == "PAR_Table"]
    poldata_par_table_pht_df = poldata_df[poldata_df['PAR_Table'] == "PAR_Table_PHT"]
    poldata_par_table_psr_df = poldata_df[poldata_df['PAR_Table'] == "PAR_Table_PSR"]
    poldata_par_table_pst_df = poldata_df[poldata_df['PAR_Table'] == "PAR_Table_PST"]
 
    # For PAR_TABLE
    output_col = ['PAR_COHORT_INDEX']
    left_key = ['CNTTYPE', 'ENTRY_YEAR', 'POL_TERM_Y', 'PREM_PAYBL_Y']
    right_key = ['CNTTYPE', 'ENTRY_YEAR', 'POL_TERM_Y', 'PREM_PAYBL_Y']
    join_type = 'left'

    poldata_par_table_df = comm_func.add_merge_col(poldata_par_table_df, par_table_df, output_col, left_key, right_key, join_type)

    # For PAR_TABLE_PHT
    left_key_pht = ['ENTRY_YEAR', 'AGE_AT_ENTRY', 'SEX', 'SMOKER_STAT']
    right_key_pht = ['ENTRY_YEAR', 'AGE_AT_ENTRY', 'SEX', 'SMOKER_STAT']

    poldata_par_table_pht_df = comm_func.add_merge_col(poldata_par_table_pht_df, par_table_pht_df, output_col, left_key_pht, right_key_pht, join_type)

    # For PAR_TABLE_PSR
    left_key_psr = ['CNTTYPE', 'ENTRY_YEAR', 'RETIREYR', 'PREM_PAYBL_Y', 'PAYOUT']
    right_key_psr = ['CNTTYPE', 'ENTRY_YEAR', 'RETIREYR', 'PREM_PAYBL_Y', 'PAYOUT']

    poldata_par_table_psr_df = comm_func.add_merge_col(poldata_par_table_psr_df, par_table_psr_df, output_col, left_key_psr, right_key_psr, join_type)

    # For PAR_Table_PST
    left_key_pst = ['ENTRY_YEAR', 'PREM_PAYBL_Y']
    right_key_pst = ['ENTRY_YEAR', 'PREM_PAYBL_Y']

    poldata_par_table_pst_df = comm_func.add_merge_col(poldata_par_table_pst_df, par_table_pst_df, output_col, left_key_pst, right_key_pst, join_type)

    for df in (poldata_par_table_df, poldata_par_table_pht_df, poldata_par_table_psr_df, poldata_par_table_pst_df):

        if ((not df.empty) & (df['PAR_COHORT_INDEX'].isnull().sum() > 0)):
            print("There are policies not assigned for PAR_COHORT_INDEX.")
        else:
            pass

    poldata_df = pd.concat([poldata_par_table_df, poldata_par_table_pht_df, poldata_par_table_psr_df, poldata_par_table_pst_df])

    condition = [(poldata_df['PAR_COHORT_INDEX'].isnull())]
    value = [0]
    default = poldata_df['RECORD_TO_KEEP']

    poldata_df = poldata_df.copy()
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'RECORD_TO_KEEP', condition, value, default)

    # Drop RECORD_TO_KEEP column from poldata_df
    poldata_df = poldata_df[(poldata_df['RECORD_TO_KEEP'] != 0)]
    poldata_df = comm_func.drop_col(poldata_df, 'RECORD_TO_KEEP')
    poldata_df = poldata_df.sort_values(by = ['VAL_TYPE', 'PRODUCT_NAME', 'SPCODE', 'POL_NUMBER'])

    # Output warning message if there is NPAR policies assigned for PAR_COHORT_INDEX
    if (poldata_df['VAL_TYPE'].isin(["NPAR"]).sum() > 0):
        print("There are NPAR policies assigned for PAR_COHORT_INDEX")
    else:
        pass

    del poldata_par_table_df, poldata_par_table_pht_df, poldata_par_table_psr_df, poldata_par_table_pst_df
    return poldata_df

# Map AS column to poldata_df
@comm_func.timer_func
def add_col_as(poldata_df_mpf, par_table_category_tbl, asrf_tbl):

    par_table_category_tbl = par_table_category_tbl.drop_duplicates(['PRODUCT_NAME'])
    par_table_list = par_table_category_tbl['PRODUCT_NAME'].tolist()
    
    poldata_df_withAS = poldata_df_mpf[poldata_df_mpf['PRODUCT_NAME'].isin(par_table_list)]
    poldata_df_withAS = poldata_df_withAS.copy()
    poldata_df_withAS['RECORD_TO_KEEP'] = 1

    poldata_df_others = poldata_df_mpf[~poldata_df_mpf['PRODUCT_NAME'].isin(par_table_list)]
    poldata_df_others = poldata_df_others.copy()
    poldata_df_others['RECORD_TO_KEEP'] = 0

    poldata_df_mpf = pd.concat([poldata_df_withAS, poldata_df_others])

    poldata_df_mpf = poldata_df_mpf.rename(columns={
        'INIT_AS_IF': 'INIT_AS_IF_x0',
        'INIT_AGC_IF': 'INIT_AGC_IF_x0',
        'YTD_COST_TB': 'YTD_COST_TB_x0'
    })

    filtered_df_spcode = poldata_df_mpf[(poldata_df_mpf['SPCODE'] < 51) & poldata_df_mpf['RECORD_TO_KEEP'] == 1]

    output_col = ['INIT_AS_IF', 'INIT_AGC_IF', 'YTD_COST_TB']
    left_key = ['POL_NO_IFRS17', 'PROD_CD', 'BENEFIT_CODE']
    right_key = ['Polno', 'CNTTYPE', 'CRTABLE']
    join_type = 'left'

    filtered_df = comm_func.add_merge_col(filtered_df_spcode, asrf_tbl, output_col, left_key, right_key, join_type)

    poldata_output_col = ['INIT_AS_IF', 'INIT_AGC_IF', 'YTD_COST_TB']
    poldata_left_key = ['POL_NO_IFRS17', 'PROD_CD', 'BENEFIT_CODE', 'SPCODE', 'PRODUCT_NAME']
    poldata_right_key = ['POL_NO_IFRS17', 'PROD_CD', 'BENEFIT_CODE', 'SPCODE', 'PRODUCT_NAME']
 
    poldata_df_mpf = comm_func.add_merge_col(poldata_df_mpf, filtered_df, poldata_output_col, poldata_left_key, poldata_right_key, join_type)
    poldata_df_mpf = poldata_df_mpf.drop_duplicates()

    # Default null values
    poldata_df_mpf = comm_func.default_col_null_to_string_or_col(poldata_df_mpf, 'INIT_AS_IF', poldata_df_mpf['INIT_AS_IF_x0'])
    poldata_df_mpf = comm_func.default_col_null_to_string_or_col(poldata_df_mpf, 'INIT_AGC_IF', poldata_df_mpf['INIT_AGC_IF_x0'])
    poldata_df_mpf = comm_func.default_col_null_to_string_or_col(poldata_df_mpf, 'YTD_COST_TB', poldata_df_mpf['YTD_COST_TB_x0'])

    # Output warning message if there is NPAR policies assigned for PAR ETL2A
    if (poldata_df_mpf['VAL_TYPE'].isin(["NPAR"]).sum() > 0):
        print("There are NPAR policies assigned for PAR ETL2A.")
    else:
        pass

    del filtered_df, filtered_df_spcode
    return poldata_df_mpf

# Update AS_CURR_YEAR and AS_CURR_MTH column 
@comm_func.timer_func
def upd_col_as_curr_year_mth(poldata_df, val_date_const):
    val_month = val_date_const.month
    val_year = val_date_const.year

    poldata_df['AS_CURR_YEAR'] = val_year
    poldata_df['AS_CURR_MTH'] = val_month

    return poldata_df

# Sort the columns in order
@comm_func.timer_func
def sort_col_as(poldata_df):

    poldata_df = poldata_df[poldata_df['RECORD_TO_KEEP'] == 1]

    sort_list = const_var.as_sort_list()
    poldata_df = poldata_df[sort_list]

    poldata_df = poldata_df.sort_values(by = ['VAL_TYPE', 'PRODUCT_NAME', 'SPCODE', 'POL_NUMBER'])
    
    return poldata_df

