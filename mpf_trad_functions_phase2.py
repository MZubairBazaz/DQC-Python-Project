import pandas as pd
import numpy as np
import common_functions as comm_func
from common_classes import Conditional
import constant_variables as const_var
from time import time

# Generic function to look up against the grouping table.
def lookup_grouping_col(poldata_df, grouping_table_df, target_col_name):
    # Updates the poldata_df by mapping the relevant "grouping" on each target column name
    conditions = [grouping_table_df['BANCA'].isna(), ~grouping_table_df['BANCA'].isna()]
    results = ["NA", grouping_table_df['BANCA']]
    grouping_table_df['BANCA'] = np.select(conditions, results)

    grouping_table_df_filtered = grouping_table_df[['CNTTYPE', 'CRTABLE', 'BANCA', 'ENHANCED_IND', target_col_name]].copy()
    grouping_table_df_filtered = grouping_table_df_filtered.rename(columns={target_col_name: "GF_" + target_col_name})
    grouping_table_df_filtered = grouping_table_df_filtered.drop_duplicates()
    poldata_df = poldata_df.merge(
        grouping_table_df_filtered[['CNTTYPE','CRTABLE', 'BANCA', 'ENHANCED_IND', "GF_" + target_col_name]], 
        on = ['CNTTYPE', 'CRTABLE', 'BANCA', 'ENHANCED_IND'], 
        how = "left"
    )
    return poldata_df

# Generic function to condition
def add_ifnb_conditional_col(poldata_df, grouping_table_df, target_col_name, conditional_col, default_col):
    # Adds a new column 'target_col_name' which takes on the 'default_col' if the record is an IF and or ET record
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, target_col_name)
    
    conditions = [
        (poldata_df["GF_" + target_col_name] == "G0"),
        (poldata_df[conditional_col].isin([const_var.IF,const_var.ET]))
    ]
 
    results = [
        0, 
        poldata_df[default_col]
    ]
    
    poldata_df[target_col_name] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + target_col_name)
    return poldata_df

# --------------------------------------------------------------------
# Rank 1
# --------------------------------------------------------------------
@comm_func.timer_func
def add_col_age2_atentry(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'AGE2_ATENTRY')
    
    conditions = [
        (poldata_df["GF_" + 'AGE2_ATENTRY'] == "G0"),
        (poldata_df["GF_" + 'AGE2_ATENTRY'] == "G1") & (poldata_df['FILE'].isin({const_var.ET, const_var.IF}))
    ]
 
    results = [
        0, 
        poldata_df["FLAGE"], 
    ]
    
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'AGE2_ATENTRY', conditions, results, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'AGE2_ATENTRY')
    return poldata_df
 
@comm_func.timer_func
def add_col_annual_prem_1(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'ANNUAL_PREM_1')
    
    conditions = [
        (poldata_df["GF_" + 'ANNUAL_PREM_1'] == "G0") |
        ((poldata_df["GF_" + 'ANNUAL_PREM_1'] == "G1") & ((poldata_df['CSTATCODE'] == "PU") | (poldata_df['CPSTATCODE'] == 'PU')))
    ]
 
    results = [
        0 
    ]
    
    default = poldata_df['ANNUAL_PREM']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'ANNUAL_PREM_1', conditions, results, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'ANNUAL_PREM_1')
    return poldata_df
 
@comm_func.timer_func
def add_col_as_curr_mth_1(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'AS_CURR_MTH_1')
    
    FIRST_MONTH = 1
    LAST_MONTH = 12
 
    conditions = [
        (poldata_df["GF_" + 'AS_CURR_MTH_1'] == "G0"),
        (poldata_df["GF_" + 'AS_CURR_MTH_1'] == "G1") & (poldata_df['ENTRY_MONTH'] != FIRST_MONTH)
    ]
 
    results = [
        0, 
        poldata_df['ENTRY_MONTH'] -1
    ]
    
    default = LAST_MONTH
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'AS_CURR_MTH_1', conditions, results, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'AS_CURR_MTH_1')
    return poldata_df
 
@comm_func.timer_func
def add_col_as_curr_year_1(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'AS_CURR_YEAR_1')
    
    FIRST_MONTH = 1
    LAST_MONTH = 12
 
    conditions = [
        (poldata_df["GF_" + 'AS_CURR_YEAR_1'] == "G0"),
        (poldata_df["GF_" + 'AS_CURR_YEAR_1'] == "G1") & (poldata_df['ENTRY_MONTH'] == FIRST_MONTH)
    ]
 
    results = [
        0, 
        poldata_df['ENTRY_YEAR'] - 1
    ]
    
    poldata_df['AS_CURR_YEAR_1'] = np.select(conditions, results, default = poldata_df['ENTRY_YEAR'])
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'AS_CURR_YEAR_1')
    return poldata_df
 
@comm_func.timer_func
def add_col_basic(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'BASIC')
    
    conditions = [
        (poldata_df["GF_" + 'BASIC'] == "G0"),
        (poldata_df["GF_" + 'BASIC'] == "G1")
    ]
 
    results = [
        0, 
        poldata_df['PRUTERM_BASIC'] 
    ]
    
    poldata_df['BASIC'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'BASIC')
    return poldata_df

@comm_func.timer_func
def add_col_blanksa(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'BLANKSA')
    
    conditions = [
        (poldata_df["GF_" + 'BLANKSA'] == "G0"),
        (poldata_df["GF_" + 'BLANKSA'] == "G1")
    ]
 
    results = [
        0, 
        poldata_df['TEMP_SA'] 
    ]
    
    default = 0.0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'BLANKSA', conditions, results, default)
    poldata_df['BLANKSA'] = comm_func.apply_rounding_on_col(poldata_df, 'BLANKSA', 2)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'BLANKSA')
    return poldata_df

@comm_func.timer_func
def add_col_cc_ann_prem(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'CC_ANN_PREM')
    
    conditions = [
        (poldata_df["GF_" + 'CC_ANN_PREM'] == "G0"),
        (poldata_df["GF_" + 'CC_ANN_PREM'] == "G1")
    ]
 
    results = [
        0, 
        poldata_df['CCB_CC_ANN_PREM'] 
    ]
    
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'CC_ANN_PREM', conditions, results, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'CC_ANN_PREM')
    return poldata_df

@comm_func.timer_func
def add_col_comm_ind(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'COMM_IND')
    
    CUTOFF_YEAR = 2020
 
    conditions = [
        (poldata_df["GF_" + 'COMM_IND'] == "G0"),
        (poldata_df["GF_" + 'COMM_IND'] == "G1") & (poldata_df['PROPRCDT'].dt.year > CUTOFF_YEAR)
    ]
 
    results = [
        0, 
        10
    ]
    
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'COMM_IND', conditions, results, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'COMM_IND')
    return poldata_df

@comm_func.timer_func
def add_col_comp_ben_sa(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'COMP_BEN_SA')
    
    conditions = [
        (poldata_df["GF_" + 'COMP_BEN_SA'] == "G0"),
        (poldata_df["GF_" + 'COMP_BEN_SA'] == "G1")
    ]
 
    results = [
        0, 
        poldata_df['SUMINS'] 
    ]
    
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'COMP_BEN_SA', conditions, results, default)

    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'COMP_BEN_SA')
    return poldata_df

@comm_func.timer_func
def add_col_defer_per_y(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'DEFER_PER_Y')
    
    conditions = [
        (poldata_df["GF_" + 'DEFER_PER_Y'] == "G0"),
        (poldata_df["GF_" + 'DEFER_PER_Y'] == "G1")
    ]
 
    results = [
        0, 
        poldata_df['CONSTPRD'] 
    ]
    
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'DEFER_PER_Y', conditions, results, default)
    # poldata_df['DEFER_PER_Y'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'DEFER_PER_Y')
    return poldata_df

@comm_func.timer_func
def add_col_gstprem(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'GSTPREM')
    
    conditions = [
        (poldata_df["GF_" + 'GSTPREM'] == "G0"),
        (poldata_df["GF_" + 'GSTPREM'] == "G1") & poldata_df['FILE'].isin([const_var.IF,const_var.ET]) & (poldata_df['BILLFREQ'] != 0),
        (poldata_df["GF_" + 'GSTPREM'] == "G1") & poldata_df['FILE'].isin([const_var.IF,const_var.ET]) & (poldata_df['BILLFREQ'] == 0)
    ]
 
    results = [
        0, 
        poldata_df['ZGSTPREM'] * poldata_df['BILLFREQ'], 
        poldata_df['ZGSTPREM']
    ]
    
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'GSTPREM', conditions, results, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'GSTPREM')
    return poldata_df

@comm_func.timer_func
def add_col_incsa_pct(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'INCSA_PCT')
    
    conditions = [
        (poldata_df["GF_" + 'INCSA_PCT'] == "G0"),
        (poldata_df["GF_" + 'INCSA_PCT'] == "G1")
    ]
 
    results = [
        0, 
        poldata_df['INCRATE'] 
    ]
    
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'INCSA_PCT', conditions, results, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'INCSA_PCT')
    return poldata_df

@comm_func.timer_func
def add_col_init_pols_if(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'INIT_POLS_IF')
    
    conditions = [
        (poldata_df["GF_" + 'INIT_POLS_IF'] == "G0"),
        (poldata_df["GF_" + 'INIT_POLS_IF'] == "G1")
    ]
 
    results = [
        0, 
        1
    ]
    
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'INIT_POLS_IF', conditions, results, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'INIT_POLS_IF')
    return poldata_df

@comm_func.timer_func
def add_col_mme_prem(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'MME_PREM')
    
    conditions = [
        (poldata_df["GF_" + 'MME_PREM'] == "G0"),
        (poldata_df["GF_" + 'MME_PREM'] == "G1")
    ]
 
    results = [
        0, 
        poldata_df['MME_PRM']
    ]
    
    poldata_df['MME_PREM'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'MME_PREM')
    return poldata_df

@comm_func.timer_func
def add_col_mom_i(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'MOM_I')
    
    conditions = [
        (poldata_df["GF_" + 'MOM_I'] == "G0"),
        (poldata_df["GF_" + 'MOM_I'] == "G1")
    ]
 
    results = [
        0, 
        1
    ]
    
    poldata_df['MOM_I'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'MOM_I')
    return poldata_df

@comm_func.timer_func
def add_col_mort_int_pc(poldata_df, grouping_table_df):
    '''
    Need to think about how to put this into functions better, implementing for now.
 
    Implementing the logic:
    IF [CRRCD] >= DateTimeParse("2009/04/01","%y-%m-%d") THEN IF [ZINTRATE] <= 3 THEN 3 ELSEIF [ZINTRATE] <= 4 THEN 4 ELSEIF [ZINTRATE] <= 5 THEN 5
      ELSEIF [ZINTRATE] <= 6 THEN 6 ELSEIF [ZINTRATE] <= 7 THEN 7 ELSEIF [ZINTRATE] <= 8 THEN 8 ELSEIF [ZINTRATE] <= 9 THEN 9 
      ELSEIF [ZINTRATE] <= 12 THEN 12 ELSEIF [ZINTRATE] <= 15 THEN 15 ELSEIF [ZINTRATE] <= 18 THEN 18 ELSE 0 ENDIF 
      ELSEIF [CRRCD] >= DateTimeParse("2005/10/01","%y-%m-%d") THEN IF [ZINTRATE] <= 7 THEN 7 ELSEIF [ZINTRATE] <= 9 THEN 9 
      ELSEIF [ZINTRATE] <= 12 THEN 12 ELSEIF [ZINTRATE] <= 15 THEN 15 ELSEIF [ZINTRATE] <= 18 THEN 18 ELSE 0 ENDIF ELSE IF [ZINTRATE] <= 9 THEN 9 
      ELSEIF [ZINTRATE] <= 12 THEN 12 ELSEIF [ZINTRATE] <= 15 THEN 15 ELSEIF [ZINTRATE] <= 18 THEN 18 ELSE 0 ENDIF ENDIF
    '''
    #Instantiate Column
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'MORT_INT_PC')
    poldata_df['MORT_INT_PC'] = 0
    
    date_comparison_1 = np.datetime64("2009-04-01")
    date_comaprison_2 = np.datetime64("2005-10-01")

    conditions = [
        ((poldata_df['GF_MORT_INT_PC'] == "G1") & (poldata_df['CRRCD'] >= date_comparison_1) & (poldata_df['ZINTRATE'] <= 3)),
        ((poldata_df['GF_MORT_INT_PC'] == "G1") & (poldata_df['CRRCD'] >= date_comparison_1) & (poldata_df['ZINTRATE'] <= 4)),
        ((poldata_df['GF_MORT_INT_PC'] == "G1") & (poldata_df['CRRCD'] >= date_comparison_1) & (poldata_df['ZINTRATE'] <= 5)),
        ((poldata_df['GF_MORT_INT_PC'] == "G1") & (poldata_df['CRRCD'] >= date_comparison_1) & (poldata_df['ZINTRATE'] <= 6)),
        ((poldata_df['GF_MORT_INT_PC'] == "G1") & ((poldata_df['CRRCD'] >= date_comparison_1) | (poldata_df['CRRCD'] >= date_comaprison_2)) & (poldata_df['ZINTRATE'] <= 7)),
        ((poldata_df['GF_MORT_INT_PC'] == "G1") & (poldata_df['CRRCD'] >= date_comparison_1) & (poldata_df['ZINTRATE'] <= 8)),
        ((poldata_df['GF_MORT_INT_PC'] == "G1") & (poldata_df['ZINTRATE'] <= 9)),
        ((poldata_df['GF_MORT_INT_PC'] == "G1") & (poldata_df['ZINTRATE'] <= 12)),
        ((poldata_df['GF_MORT_INT_PC'] == "G1") & (poldata_df['ZINTRATE'] <= 15)),
        ((poldata_df['GF_MORT_INT_PC'] == "G1") & (poldata_df['ZINTRATE'] <= 18))
        
    ]

    results = [
        3,
        4,
        5,
        6,
        7,
        8,
        9,
        12,
        15,
        18
    ]

    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'MORT_INT_PC', conditions, results, default)

    # drop redundant columns
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'MORT_INT_PC')
    return poldata_df

@comm_func.timer_func
def add_col_no_ls_claim(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'NO_LS_CLAIM')
    
    conditions = [
        (poldata_df["GF_" + 'NO_LS_CLAIM'] == "G0"),
        (poldata_df["GF_" + 'NO_LS_CLAIM'] == "G1")
    ]
 
    results = [
        0, 
        poldata_df['LSCI']
    ]
    
    poldata_df['NO_LS_CLAIM'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'NO_LS_CLAIM')
 
    return poldata_df

@comm_func.timer_func
def add_col_nonpar_prem(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'NONPAR_PREM')
    
    conditions = [
        (poldata_df["GF_" + 'NONPAR_PREM'] == "G0"),
        (poldata_df["GF_" + 'NONPAR_PREM'] == "G1")
    ]
 
    results = [
        0, 
        poldata_df['ANNUAL_PREM']
    ]
    
    poldata_df['NONPAR_PREM'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'NONPAR_PREM')
 
    return poldata_df

@comm_func.timer_func
def add_col_pol_term_y(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'POL_TERM_Y')
    
    conditions = [
        (poldata_df["GF_" + 'POL_TERM_Y'] == "G0"),
        (poldata_df["GF_" + 'POL_TERM_Y'] == "G1") & (poldata_df['FILE'].isin([const_var.IF,const_var.ET]) )
    ]
 
    results = [
        0, 
        poldata_df["RCESTRM"]
    ]
    
    poldata_df['POL_TERM_Y'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'POL_TERM_Y')
 
    return poldata_df

@comm_func.timer_func
def add_col_ppt_code(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'PPT_CODE')
    
    conditions = [
        (poldata_df["GF_" + 'PPT_CODE'] == "G0"),
        (poldata_df["GF_" + 'PPT_CODE'] == "G1") & (poldata_df['BENPLN'] == 'GA'),
        (poldata_df["GF_" + 'PPT_CODE'] == "G1") & (poldata_df['BENPLN'] == 'GB'),
        (poldata_df["GF_" + 'PPT_CODE'] == "G1") & (poldata_df['BENPLN'] == 'GC'),
        (poldata_df["GF_" + 'PPT_CODE'] == "G1") & (poldata_df['BENPLN'] == 'GD')
    ]
 
    results = [
        0, 
        1,
        2,
        3,
        4
    ]

    default = 0
    
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'PPT_CODE', conditions, results, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'PPT_CODE')
    return poldata_df

@comm_func.timer_func
def add_col_prem_freq(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'PREM_FREQ')
    
    conditions = [
        (poldata_df["GF_" + 'PREM_FREQ'] == "G0"),
        (poldata_df["GF_" + 'PREM_FREQ'] == "G1") & (poldata_df['BILLFREQ'] == 0)
    ]
 
    results = [
        0, 
        1
    ]
    
    poldata_df['PREM_FREQ'] = np.select(conditions, results, default = poldata_df['BILLFREQ'])
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'PREM_FREQ')
 
    return poldata_df

@comm_func.timer_func
def add_col_prem_months(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'PREM_MONTHS')
    
    conditions = [
        (poldata_df["GF_" + 'PREM_MONTHS'] == "G0"),
        (poldata_df["GF_" + 'PREM_MONTHS'] == "G1") & (poldata_df['FILE'] == const_var.IF)
    ]
 
    results = [
        0, 
        np.maximum(0, ((poldata_df['ZNPDD'].dt.year - poldata_df['CRRCD'].dt.year) * 12 + poldata_df['ZNPDD'].dt.month - poldata_df['CRRCD'].dt.month))
    ]
    
    poldata_df['PREM_MONTHS'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'PREM_MONTHS')
    return poldata_df

@comm_func.timer_func
def add_col_prem_paybl_y_1(poldata_df, grouping_table_df):
    # Returns age_at_entry_1 field
    poldata_df = add_ifnb_conditional_col(poldata_df, grouping_table_df, 'PREM_PAYBL_Y_1', 'FILE', 'PCESTRM')
    return poldata_df

@comm_func.timer_func
def add_col_retirement_age(poldata_df, grouping_table_df):

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, "RETIREMENT_AGE")

    condition = [(poldata_df["GF_RETIREMENT_AGE"] == "G1")]
    value = [poldata_df['RETIREAGE']]
    default = 0
    
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'RETIREMENT_AGE', condition, value, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_RETIREMENT_AGE")

    return poldata_df

@comm_func.timer_func
def add_col_sex(poldata_df, grouping_table_df):

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'SEX')

    condition = [((poldata_df['GF_SEX'] == "G0")|
                 ((poldata_df['GF_SEX'] == "G1") & 
                 (poldata_df['FILE'].isin([const_var.IF,const_var.ET])) & 
                 (poldata_df['CLTSEX'] == "M")))]
    value = [0]
    default = 1

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'SEX', condition, value, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_SEX")

    return poldata_df

@comm_func.timer_func
def add_col_sex2(poldata_df, grouping_table_df):

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'SEX2')

    condition = [((poldata_df['GF_SEX2'] == "G0")|
                 ((poldata_df['GF_SEX2'] == "G1") & 
                 (poldata_df['FILE'].isin([const_var.IF,const_var.ET])) & 
                 (poldata_df['FLSEX'] == "M")))]
    value = [0]
    default = 1

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'SEX2', condition, value, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_SEX2")

    return poldata_df

@comm_func.timer_func
def add_col_smoker_stat(poldata_df, grouping_table_df):

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'SMOKER_STAT')

    condition = [(((poldata_df['GF_' + 'SMOKER_STAT'] == "G1") & 
                 (poldata_df['FILE'].isin([const_var.IF,const_var.ET])) & 
                 (poldata_df['SMOKING'] == "S")))]
    value = [1]
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'SMOKER_STAT', condition, value, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + "SMOKER_STAT")

    return poldata_df

@comm_func.timer_func
def add_col_smoker2_stat(poldata_df, grouping_table_df):

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'SMOKER2_STAT')

    condition = [(((poldata_df['GF_SMOKER2_STAT'] == "G1") & 
                 (poldata_df['FILE'].isin([const_var.IF,const_var.ET])) & 
                 (poldata_df['FLSMOKING'] == "S")))]
    value = [1]
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'SMOKER2_STAT', condition, value, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + "SMOKER2_STAT")

    return poldata_df

@comm_func.timer_func
def add_col_substd_prem(poldata_df, grouping_table_df):

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'SUBSTD_PREM')

    condition = [(poldata_df["GF_" + 'SUBSTD_PREM'] == "G1")]
    value = [poldata_df['EXTR']]
    default = 0.0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'SUBSTD_PREM', condition, value, default)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + "SUBSTD_PREM")

    return poldata_df

@comm_func.timer_func
def add_col_treaty_id_treaty(poldata_df, grouping_table_df):

    ID_list = ["1", "2", "3", "4", "5"]

    for i in ID_list:
        poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'TREATY_ID_TREATY' + i)

        condition = [(poldata_df['GF_TREATY_ID_TREATY' + i] == "G1")]
        value = ["NA"]
        default = "0"

        poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'TREATY_ID_TREATY' + i, condition, value, default)
        poldata_df = comm_func.drop_col(poldata_df, "GF_" + "TREATY_ID_TREATY" + i)

    return poldata_df

@comm_func.timer_func
def add_col_age_at_entry_1(poldata_df, grouping_table_df):
    
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'AGE_AT_ENTRY_1')

    File_List = [const_var.IF,const_var.ET]
    poldata_df = comm_func.add_col_month(poldata_df, 'month_CRRCD', 'CRRCD')
    poldata_df = comm_func.add_col_month(poldata_df, 'month_CLTDOB', 'CLTDOB')
    poldata_df = comm_func.add_col_day(poldata_df, 'day_CRRCD', 'CRRCD')
    poldata_df = comm_func.add_col_day(poldata_df, 'day_CLTDOB', 'CLTDOB')
    poldata_df = comm_func.add_col_year(poldata_df, 'year_CRRCD', 'CRRCD')
    poldata_df = comm_func.add_col_year(poldata_df, 'year_CLTDOB', 'CLTDOB')

    conditions = [
        (poldata_df['GF_AGE_AT_ENTRY_1'] == "G1") & (poldata_df['FILE'].isin(File_List)),

        (poldata_df['GF_AGE_AT_ENTRY_1'] == "G2") & 
        (poldata_df['FILE'].isin(File_List)) & 
        (
         (
          (poldata_df['month_CRRCD'] == poldata_df['month_CLTDOB']) &
          (poldata_df['day_CRRCD'] > poldata_df['day_CLTDOB'])
          ) |
         (poldata_df['month_CRRCD'] > poldata_df['month_CLTDOB'])
        ),

        ((poldata_df['GF_AGE_AT_ENTRY_1'] == "G2") & (poldata_df['FILE'] == const_var.IF)),

        (poldata_df['GF_AGE_AT_ENTRY_1'] == "G2") & (poldata_df['FILE'] == const_var.ET)
    ]
    
    results = [
        poldata_df['AGE'],
        (poldata_df['year_CRRCD'] - poldata_df['year_CLTDOB'] + 1),
        np.maximum(1, (poldata_df['year_CRRCD'] - poldata_df['year_CLTDOB'])),
        (poldata_df['year_CRRCD'] - poldata_df['year_CLTDOB'])
    ]
    
    poldata_df['AGE_AT_ENTRY_1'] = np.select(conditions, results, default = 0)
    
    poldata_df = comm_func.drop_col(poldata_df, 'month_CRRCD')
    poldata_df = comm_func.drop_col(poldata_df, 'month_CLTDOB')
    poldata_df = comm_func.drop_col(poldata_df, 'day_CRRCD')
    poldata_df = comm_func.drop_col(poldata_df, 'day_CLTDOB')
    poldata_df = comm_func.drop_col(poldata_df, 'year_CRRCD')
    poldata_df = comm_func.drop_col(poldata_df, 'year_CLTDOB')
    poldata_df = comm_func.drop_col(poldata_df, 'GF_AGE_AT_ENTRY_1')
   
    return poldata_df

@comm_func.timer_func
def upd_col_fundtype(poldata_df, grouping_table_df):

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'FUNDTYPE')

    condition = [(
                 ((poldata_df['GF_FUNDTYPE'] == "G1") &
                  ((poldata_df['CSTATCODE'] == "PU") | (poldata_df['CPSTATCODE'] == "PU"))
                 ) |
                 (poldata_df['GF_FUNDTYPE'] == "G2")
                )]
    
    value = [2]
    default = poldata_df['FUNDTYPE']
    
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'FUNDTYPE', condition, value, default)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_FUNDTYPE')

    return poldata_df    

@comm_func.timer_func
def add_col_prem_mode(poldata_df, grouping_table_df):

    File_List = [const_var.IF,const_var.ET]
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'PREM_MODE')

    condition = [(
                  ((poldata_df['GF_PREM_MODE'] == "G1") &
                  (poldata_df['BILLCHNL'].isin(["C", "R", "H"])) &
                  (poldata_df['FILE'].isin(File_List))) |
        
                  ((poldata_df['GF_PREM_MODE'] == "G2") & 
                  (poldata_df['BILLCHNL'] == "C") & (poldata_df['FILE'].isin(File_List)))
                )]

    value = [1]
    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'PREM_MODE', condition, value, default)

    poldata_df = comm_func.drop_col(poldata_df, 'GF_PREM_MODE')

    return poldata_df

@comm_func.timer_func
def add_col_series_ind(poldata_df, grouping_table_df):

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'SERIES_IND')

    conditions = [
        (poldata_df['GF_SERIES_IND'] == "G1"),
        (poldata_df['GF_SERIES_IND'] == "G2")
    ]
    
    results = [
        2,
        1
    ]

    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'SERIES_IND', conditions, results, default)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_SERIES_IND')

    return poldata_df

@comm_func.timer_func
def upd_col_single_prem(poldata_df, grouping_table_df):

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'SINGLE_PREM')

    conditions = [
        (poldata_df['GF_SINGLE_PREM'] == "G0"),
        (poldata_df['GF_SINGLE_PREM'] == "G2")
    ]
    
    results = [
        0.0,
        poldata_df['ZBINSTPREM']
    ]

    poldata_df = poldata_df.rename(columns={
        'SINGLE_PREM': 'SINGLE_PREM_x0'
    })
    default = poldata_df['SINGLE_PREM_x0']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'SINGLE_PREM', conditions, results, default)
    poldata_df['SINGLE_PREM'] = comm_func.apply_rounding_on_col(poldata_df, 'SINGLE_PREM', 2)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_SINGLE_PREM')

    return poldata_df

@comm_func.timer_func
def add_col_cc_term_y(poldata_df, grouping_table_df):

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'CC_TERM_Y')

    conditions = [
        ((poldata_df['GF_CC_TERM_Y'] == "G1") & ((poldata_df['CSTATCODE'] == "PU") | (poldata_df['CPSTATCODE'] == "PU"))),
        ((poldata_df['GF_CC_TERM_Y'] == "G1") |
        ((poldata_df['GF_CC_TERM_Y'] == "G2") & (poldata_df['MRTA_CC_TERM_Y'] == 0))),
        (poldata_df['GF_CC_TERM_Y'] == "G2"),
        (poldata_df['GF_CC_TERM_Y'] == "G3")
    ]

    results = [
       0,
       poldata_df['RCESTRM'],
       poldata_df['MRTA_CC_TERM_Y'],
       poldata_df['CCB_CC_TERM_Y'] 
    ]

    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'CC_TERM_Y', conditions, results, default)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_CC_TERM_Y')

    return poldata_df

def rb_condition_filter(poldata_df, group_indicator, ZYYMMINF_switch, val_date_const):

    val_month = val_date_const.month
    val_year = val_date_const.year

    if ZYYMMINF_switch == 0:
        conditions = (
            (poldata_df['GF_ZREVBNS'] == group_indicator) &
            (poldata_df['ENTRY_MONTH'] > val_month) &
            (poldata_df['ENTRY_YEAR'] < val_year)
            )
    else:
        conditions = (
            (poldata_df['GF_ZREVBNS'] == group_indicator) &
            (poldata_df['ENTRY_MONTH'] > val_month) &
            (poldata_df['ENTRY_YEAR'] < val_year) &
            (poldata_df['ZYYMMINF'] >= (ZYYMMINF_switch + val_month))
            )

    return conditions

@comm_func.timer_func
def revbns_calc(poldata_df, sumins, zrevbns):
    
    calcs = sumins * poldata_df['SUMINS'] + zrevbns * poldata_df['ZREVBNS_x0']

    return calcs

@comm_func.timer_func
def upd_col_zrevbns(poldata_df, grouping_table_df, val_date_const):

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'ZREVBNS')

    poldata_df = poldata_df.rename(columns={
        'ZREVBNS': 'ZREVBNS_x0'
    })

    conditions = [
        (poldata_df['GF_ZREVBNS'] == "G0"),
        rb_condition_filter(poldata_df, "G2", 0, val_date_const),
        rb_condition_filter(poldata_df, "G3", 0, val_date_const),
        rb_condition_filter(poldata_df, "G4", 0, val_date_const),
        rb_condition_filter(poldata_df, "G5", 0, val_date_const),
        rb_condition_filter(poldata_df, "G6", 0, val_date_const),
        rb_condition_filter(poldata_df, "G7", 201, val_date_const),
        rb_condition_filter(poldata_df, "G8", 201, val_date_const),
        rb_condition_filter(poldata_df, "G9", 0, val_date_const),
        rb_condition_filter(poldata_df, "G10", 201, val_date_const),
        rb_condition_filter(poldata_df, "G11", 0, val_date_const),
        rb_condition_filter(poldata_df, "G12", 0, val_date_const),
        rb_condition_filter(poldata_df, "G13", 201, val_date_const),
        rb_condition_filter(poldata_df, "G14", 201, val_date_const),
        rb_condition_filter(poldata_df, "G15", 0, val_date_const),
        rb_condition_filter(poldata_df, "G16", 0, val_date_const),
        rb_condition_filter(poldata_df, "G17", 0, val_date_const),
        rb_condition_filter(poldata_df, "G18", 101, val_date_const),
        rb_condition_filter(poldata_df, "G19", 0, val_date_const),
        rb_condition_filter(poldata_df, "G20", 0, val_date_const),
        rb_condition_filter(poldata_df, "G21", 0, val_date_const),
        rb_condition_filter(poldata_df, "G22", 0, val_date_const),
        rb_condition_filter(poldata_df, "G23", 0, val_date_const),
    ]

    results = [
        0,
        revbns_calc(poldata_df, 0.0445, 1.0345),
        revbns_calc(poldata_df, 0.022, 1.029),
        revbns_calc(poldata_df, 0.01, 1.015),
        revbns_calc(poldata_df, 0.013, 1.0175),
        revbns_calc(poldata_df, 0.01, 1.015),
        revbns_calc(poldata_df, 0.015, 1.030),
        revbns_calc(poldata_df, 0.015, 1.030),
        revbns_calc(poldata_df, 0.015, 1.030),
        revbns_calc(poldata_df, 0.018, 1.036),
        revbns_calc(poldata_df, poldata_df['rb7'], 1.015),
        revbns_calc(poldata_df, poldata_df['rb2'], 1.028),
        revbns_calc(poldata_df, poldata_df['rb3'], 1.035),
        revbns_calc(poldata_df, poldata_df['rb1'], 1.034),
        revbns_calc(poldata_df, poldata_df['rb6'], 1.0265),
        revbns_calc(poldata_df, poldata_df['rb'], 1.034),
        revbns_calc(poldata_df, 0.03, 1.038),
        revbns_calc(poldata_df, poldata_df['rb4'], (1 + poldata_df['rb4'])),
        (0.02 * poldata_df['TEMP_SA'] + 1.038 * poldata_df['ZREVBNS_x0']),
        revbns_calc(poldata_df, 0.065, 1),
        revbns_calc(poldata_df, poldata_df['rb5'], (1 + poldata_df['rb5'])),
        poldata_df['ZREVBNS_x0'],
        revbns_calc(poldata_df, 0.01, 1.015)
    ]

    default = poldata_df['ZREVBNS_x0']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'ZREVBNS', conditions, results, default)
    poldata_df['ZREVBNS'] = comm_func.apply_rounding_on_col(poldata_df, 'ZREVBNS', 6)

    poldata_df = comm_func.drop_col(poldata_df, 'GF_ZREVBNS')
    return poldata_df

# For G0 only list
@comm_func.timer_func
def add_col_group_zero(poldata_df):

    list = ['GN_INTRIM_RB', 'GN_REVBON_LY', 'PUPINDICATOR', 'INIT_AS_IF', 'INIT_AS_SURD', 'INIT_AGC_IF', 
            'CI_DURIF_M', 'SA_IND', 'IND_PC', 'WAIVED_STAT', 'CIS_I', 'RECV_I', 'PREG_I', 'BABY_I', 'OVA_I',
            'LOST_I', 'DEL_I', 'YTD_COST_TB', 'WAIV_TYPE', 'SN_LOAN']
    
    for i in list:
        poldata_df[i] = 0

    return poldata_df

# Adjustment after Rank 1
# Add DTH_MULT column to poldata_df
@comm_func.timer_func
def add_col_dth_mult(poldata_df, lookup_list_tbl):

    filtered_prod_df = poldata_df[poldata_df['PRODUCT_NAME'] == "C6PHEB"]
    
    output_col = ['MNS', 'FNS', 'MS', 'FS']
    left_key = ['AGE_AT_ENTRY_1']
    right_key = ['AGE_AT_ENTRY']
    join_type = 'left'

    filtered_df = comm_func.add_merge_col(filtered_prod_df, lookup_list_tbl, output_col, left_key, right_key, join_type)
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, output_col, ['$RID'], ['$RID'], 'left')

    conditions = [(poldata_df['PRODUCT_NAME'] == "C6PHEB") & (poldata_df['MNS'].isna())]
    results = [0]
    default = poldata_df['RECORD_TO_KEEP']

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'RECORD_TO_KEEP', conditions, results, default)

    conditions = [
        ((poldata_df['SEX'] == 1) & (poldata_df['SMOKER_STAT'] == 0)),
        ((poldata_df['SEX'] == 0) & (poldata_df['SMOKER_STAT'] == 0)),
        ((poldata_df['SEX'] == 1) & (poldata_df['SMOKER_STAT'] == 1)),
        ((poldata_df['SEX'] == 0) & (poldata_df['SMOKER_STAT'] == 1))
    ]

    results = [
        poldata_df['FNS'],
        poldata_df['MNS'],
        poldata_df['FS'],
        poldata_df['MS']
    ]

    poldata_df['DTH_MULT'] = np.select(conditions, results, default = 0.0)

    del filtered_prod_df, filtered_df
    return poldata_df

# Adjustment of PPT_CODE in poldata_df
@comm_func.timer_func
def upd_col_ppt_code(poldata_df):

    CNTTYPE_list = const_var.cnttype_ppt_filter_list()
    CRTABLE_list = const_var.crtable_ppt_filter_list()

    poldata_df = poldata_df.rename(columns={
        'PPT_CODE': 'PPT_CODE_x0'
    })

    filtered_df = poldata_df[(poldata_df['CNTTYPE'].isin(CNTTYPE_list) & poldata_df['CRTABLE'].isin(CRTABLE_list)) & (poldata_df['RECORD_TO_KEEP'] != 0)]

    groupby_key_list = ['FILE','CHDRNUM']
    
    filtered_df = comm_func.add_groupby_col(filtered_df, groupby_key_list, 'PPT_CODE_x0', 'Right_PPT_CODE', "mean")
    
    poldata_df = comm_func.add_merge_col(poldata_df, filtered_df, ['Right_PPT_CODE'], ['FILE','CHDRNUM'], ['FILE','CHDRNUM'], 'left')
    
    condition = [(poldata_df['Right_PPT_CODE'].notnull())]
    value = [poldata_df['Right_PPT_CODE']]
    default = poldata_df['PPT_CODE_x0']
    
    poldata_df = poldata_df.copy()
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'PPT_CODE', condition, value, default)
    poldata_df = comm_func.drop_col(poldata_df, 'Right_PPT_CODE')

    del filtered_df
    return poldata_df

# --------------------------------------------------------------------
# Rank 2 Calculations
# --------------------------------------------------------------------
@comm_func.timer_func
def add_col_death_ben(poldata_df, grouping_table_df):
    # Adds a new column 'DEATH_BEN' based on defined definitions
    poldata_df = lookup_grouping_col(poldata_df,  grouping_table_df, 'DEATH_BEN')
    conditions = [
        (poldata_df["GF_" + 'DEATH_BEN'] == "G0"),
        (poldata_df["GF_" + 'DEATH_BEN'] == "G1")
    ]

    results = [
        0.0,
        poldata_df['SINGLE_PREM'] * poldata_df['DTH_MULT']
    ]

    poldata_df['DEATH_BEN'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_DEATH_BEN')

    return poldata_df

@comm_func.timer_func
def add_col_mat_ben_pp_1(poldata_df, grouping_table_df):
    # Adds a new column 'MAT_BEN_PP_1' based on defined definitions
    poldata_df = lookup_grouping_col(poldata_df,  grouping_table_df, 'MAT_BEN_PP_1')
    conditions = [
        (poldata_df["GF_" + 'MAT_BEN_PP_1'] == "G0"),
        (poldata_df["GF_" + 'MAT_BEN_PP_1'] == "G1"),
        (poldata_df["GF_" + 'MAT_BEN_PP_1'] == "G2"),
        (poldata_df["GF_" + 'MAT_BEN_PP_1'] == "G3"),
        (poldata_df["GF_" + 'MAT_BEN_PP_1'] == "G4")
    ]

    results = [
        0.0,
        poldata_df['ANNUAL_PREM'] * poldata_df['PREM_PAYBL_Y_1'] * poldata_df['SURR_FAC_1'],
        0.55 * poldata_df['SUM_ASSURED'],
        poldata_df['ANNUAL_PREM'] * poldata_df['POL_TERM_Y'] * poldata_df['SURR_FAC_1'],
        poldata_df['SUM_ASSURED']
    ]

    default = 0.0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'MAT_BEN_PP_1', conditions, results, default)
    poldata_df['MAT_BEN_PP_1'] = comm_func.apply_rounding_on_col(poldata_df, 'MAT_BEN_PP_1', 0)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_MAT_BEN_PP_1')

    return poldata_df

@comm_func.timer_func
def add_col_age_at_entry_2(poldata_df, grouping_table_df):
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'AGE_AT_ENTRY_2')

    conditions = [
        (poldata_df['GF_AGE_AT_ENTRY_2'] == "G0"),
        (poldata_df['GF_AGE_AT_ENTRY_2'] == "G1") |
        (poldata_df['GF_AGE_AT_ENTRY_2'] == "G2") & (poldata_df['AGE_AT_ENTRY_1'] != 15) |
        (poldata_df['GF_AGE_AT_ENTRY_2'] == "G3") & (poldata_df['AGE_AT_ENTRY_1'] != 16),
        (poldata_df['GF_AGE_AT_ENTRY_2'] == "G2") & (poldata_df['AGE_AT_ENTRY_1'] == 15),
        (poldata_df['GF_AGE_AT_ENTRY_2'] == "G3") & (poldata_df['AGE_AT_ENTRY_1'] == 16)
    ]

    results = [
        0,
        poldata_df['AGE_AT_ENTRY_1'],
        16,
        17
    ]

    poldata_df['AGE_AT_ENTRY_2'] = np.select(conditions, results, default = poldata_df['AGE_AT_ENTRY_1'])

    poldata_df = comm_func.drop_col(poldata_df, 'GF_AGE_AT_ENTRY_2')

    return poldata_df

@comm_func.timer_func
def add_col_init_decb_if(poldata_df, grouping_table_df):
    # Adds a new column 'INIT_DECB_IF' based on defined definitions
    poldata_df = lookup_grouping_col(poldata_df,  grouping_table_df, 'INIT_DECB_IF')
    conditions = [
        (poldata_df["GF_" + 'INIT_DECB_IF'] == "G0"),
        (poldata_df["GF_" + 'INIT_DECB_IF'] == "G1")
    ]

    results = [
        0.0,
        poldata_df['ZREVBNS']
    ]

    default = 0
    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'INIT_DECB_IF', conditions, results, default)
    poldata_df['INIT_DECB_IF'] = comm_func.apply_rounding_on_col(poldata_df, 'INIT_DECB_IF', 6)
    # poldata_df['INIT_DECB_IF'] = poldata_df['INIT_DECB_IF'].round(6)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_INIT_DECB_IF')

    return poldata_df

@comm_func.timer_func
def add_col_link_prem(poldata_df, grouping_table_df):
    # Adds a new column 'LINK_PREM' based on defined definitions
    poldata_df = lookup_grouping_col(poldata_df,  grouping_table_df, 'LINK_PREM')
    conditions = [
        (poldata_df["GF_" + 'LINK_PREM'] == "G0"),
        (poldata_df["GF_" + 'LINK_PREM'] == "G1")
    ]

    results = [
        0.0,
        poldata_df['NONPAR_PREM'] / 0.75 * 0.25
    ]

    poldata_df['LINK_PREM'] = np.select(conditions, results, default = 0)
    poldata_df['LINK_PREM'] = comm_func.apply_rounding_on_col(poldata_df, 'LINK_PREM', 10)
    # poldata_df['LINK_PREM'] = poldata_df['LINK_PREM'].round(10)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_LINK_PREM')

    return poldata_df

@comm_func.timer_func
def add_col_waived_ind(poldata_df, grouping_table_df):
    # Adds a new column 'WAIVED_IND' based on defined definitions
    poldata_df = lookup_grouping_col(poldata_df,  grouping_table_df, 'WAIVED_IND')
    conditions = [
        ((poldata_df["GF_" + 'WAIVED_IND'] == "G0") |
         ((poldata_df["GF_" + 'WAIVED_IND'] == "G1") &
         ((poldata_df['FILE'] == const_var.ET) | (poldata_df['FUNDTYPE'] == 2) | (poldata_df['WAIV_IND'] == 0)))),
        ((poldata_df["GF_" + 'WAIVED_IND'] == "G1") & (poldata_df['WLIST_W_IND'].notnull()))
    ]

    results = [
        0,
        poldata_df['WLIST_W_IND']
    ]

    default = 0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'WAIVED_IND', conditions, results, default)
    # poldata_df['WAIVED_IND'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_WAIVED_IND')

    return poldata_df

@comm_func.timer_func
def add_col_waived_prem(poldata_df, grouping_table_df):
    # Adds a new column 'WAIVED_PREM' based on defined definitions
    poldata_df = lookup_grouping_col(poldata_df,  grouping_table_df, 'WAIVED_PREM')

    conditions = [
        ((poldata_df["GF_" + 'WAIVED_PREM'] == "G0") | 
        ((poldata_df['GF_WAIVED_PREM'] == "G1") & ((poldata_df['FILE'] == const_var.ET) | (poldata_df['FUNDTYPE'] == 2) | (poldata_df['WAIV_IND'] == 0)))),
        ((poldata_df['GF_WAIVED_PREM'] == "G1") & poldata_df['WLIST_SUM_PREM'].notnull())
    ]

    results = [
        0.0,
        poldata_df['WLIST_SUM_PREM']
    ]

    default = 0.0

    poldata_df = comm_func.add_conditional_column_to_df(poldata_df, 'WAIVED_PREM', conditions, results, default)
 
    # poldata_df['WAIVED_PREM'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_WAIVED_PREM')

    return poldata_df

@comm_func.timer_func
def add_col_plan_code(poldata_df, grouping_table_df):
    # Adds a new column 'PLAN_CODE' based on defined definitions
    poldata_df = lookup_grouping_col(poldata_df,  grouping_table_df, 'PLAN_CODE')

    list1= ['H1', 'H5', 'HA', 'HE', 'HI', 'HM']
    list2 = ['H2', 'H6', 'HB', 'HF', 'HJ', 'HN']
    list3 = ['H3', 'H7', 'HC', 'HG', 'HK', 'HO']
    list4 = ['H4', 'H8', 'HD', 'HH', 'HL', 'HP']

    #Need to ammend the code for the switch G2.
    conditions = [
        (poldata_df["GF_" + 'PLAN_CODE'] == "G0"),
        (poldata_df["GF_" + 'PLAN_CODE'] == "G1"),
        (poldata_df["GF_" + 'PLAN_CODE'] == "G2") & (poldata_df['BENPLN'].isin(list1)),
        (poldata_df["GF_" + 'PLAN_CODE'] == "G2") & (poldata_df['BENPLN'].isin(list2)),
        (poldata_df["GF_" + 'PLAN_CODE'] == "G2") & (poldata_df['BENPLN'].isin(list3)),
        (poldata_df["GF_" + 'PLAN_CODE'] == "G2") & (poldata_df['BENPLN'].isin(list4)),
        (poldata_df["GF_" + 'PLAN_CODE'] == "G3"),
        (poldata_df["GF_" + 'PLAN_CODE'] == "G4") & (poldata_df['BASIC_PSM_IND'] == 0),
        (poldata_df["GF_" + 'PLAN_CODE'] == "G5")
    ]

    results = [
        0,
        1,
        1,
        2,
        3,
        4,
        poldata_df['PPT_CODE'],
        1,
        poldata_df['SUMINS'] / 25000
    ]

    poldata_df['PLAN_CODE'] = np.select(conditions, results, default = 0)
    poldata_df['PLAN_CODE'] = comm_func.apply_rounding_on_col(poldata_df, 'PLAN_CODE', 0)
    # poldata_df['PLAN_CODE'] = poldata_df['PLAN_CODE'].round(0)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_PLAN_CODE')

    return poldata_df

@comm_func.timer_func
def add_col_basic_ind(poldata_df, grouping_table_df):
    # Adds a new column 'PLAN_CODE' based on defined definitions
    poldata_df = lookup_grouping_col(poldata_df,  grouping_table_df, 'BASIC_IND')

    conditions = [
        (poldata_df["GF_" + 'BASIC_IND'] == "G0"),

        ((poldata_df["GF_" + 'BASIC_IND'] == "G1") & (poldata_df['ZBSCIND'] == "Y")) |
        (poldata_df["GF_" + 'BASIC_IND'] == "G2") |
        ((poldata_df["GF_" + 'BASIC_IND'] == "G3") & ((poldata_df['BASIC'] == 1) | (poldata_df['ZBSCIND'] == "Y"))) |
        ((poldata_df["GF_" + 'BASIC_IND'] == "G4") & ((poldata_df['BASIC'] == 3) | (poldata_df['ZBSCIND'] == "Y"))) |
        ((poldata_df["GF_" + 'BASIC_IND'] == "G5") & ((poldata_df['BASIC'] == 4) | (poldata_df['ZBSCIND'] == "Y"))) |
        ((poldata_df["GF_" + 'BASIC_IND'] == "G6") & ((poldata_df['BASIC'] == 2) | (poldata_df['ZBSCIND'] == "Y")))
    ]

    results = [
        0,
        1
    ]

    poldata_df['BASIC_IND'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, 'GF_BASIC_IND')

    return poldata_df

@comm_func.timer_func
def upd_col_cc_sum_assd(poldata_df, grouping_table_df):
    # Updates the column value for CC_SUM_ASSD based on defined definitions

    poldata_df = poldata_df.rename(columns={
        'CC_SUM_ASSD': 'CC_SUM_ASSD_x1'
    })
    poldata_df = lookup_grouping_col(poldata_df,  grouping_table_df, 'CC_SUM_ASSD')

    conditions = [
        (poldata_df["GF_" + 'CC_SUM_ASSD'] == "G1"),
        (poldata_df["GF_" + 'CC_SUM_ASSD'] == "G2"),
        (poldata_df["GF_" + 'CC_SUM_ASSD'] == "G3"),
        ((poldata_df["GF_" + 'CC_SUM_ASSD'] == "G4") & ((poldata_df['CSTATCODE'] == "PU") | (poldata_df['CPSTATCODE'] == "PU"))),
        (poldata_df["GF_" + 'CC_SUM_ASSD'] == "G4"),
        ((poldata_df["GF_" + 'CC_SUM_ASSD'] == "G5") & (poldata_df['FUNDSRCE'] == "B") & (poldata_df['MRTA_CC_SUM_ASSD'] > 0)),
        (poldata_df["GF_" + 'CC_SUM_ASSD'] == "G5"),
        ((poldata_df["GF_" + 'CC_SUM_ASSD'] == "G6") & (poldata_df['MRTA_CC_SUM_ASSD'] > 0))
    ]

    results = [
        poldata_df['CC_SUM_ASSD_x1'],
        poldata_df['MRTA_CC_SUM_ASSD'],
        poldata_df['CCB_CC_SUM_ASSD'],
        0.0,
        poldata_df['SUMINS'],
        (poldata_df['MRTA_CC_SUM_ASSD'] + poldata_df['BLANKSA']),
        poldata_df['MRTA_CC_SUM_ASSD'],
        poldata_df['BLANKSA']
    ]

    poldata_df['CC_SUM_ASSD'] = np.select(conditions, results, default = 0.0)
    poldata_df = comm_func.default_col_null_to_zero(poldata_df, 'CC_SUM_ASSD')
    poldata_df = comm_func.drop_col(poldata_df, 'GF_CC_SUM_ASSD')

    return poldata_df

@comm_func.timer_func
def upd_col_sum_assured_grouped(poldata_df, grouping_table_df):

    poldata_df = poldata_df.rename(columns={
        'SUM_ASSURED': 'SUM_ASSURED_x2'
    })
    poldata_df = lookup_grouping_col(poldata_df,  grouping_table_df, 'SUM_ASSURED')

    conditions = [
        (poldata_df["GF_" + 'SUM_ASSURED'] == "G1") & (poldata_df['FUNDSRCE'] == "B"),
        ((poldata_df["GF_" + 'SUM_ASSURED'] == "G1") & (poldata_df['FUNDSRCE'] != "B")) | (poldata_df["GF_" + 'SUM_ASSURED'] == "G2"),
        (poldata_df["GF_" + 'SUM_ASSURED'] == "G3"),
        (poldata_df["GF_" + 'SUM_ASSURED'] == "G4"),
        ((poldata_df["GF_" + 'SUM_ASSURED'] == "G5") & (poldata_df['BASIC_PSM_IND'] != 0)),
        ((poldata_df["GF_" + 'SUM_ASSURED'] == "G5") & (poldata_df['TOTAP_SUMINS'] == 230000)),
        ((poldata_df["GF_" + 'SUM_ASSURED'] == "G5") & (poldata_df['TOTAP_SUMINS'] != 230000)),
        ((poldata_df["GF_" + 'SUM_ASSURED'] == "G6") & (poldata_df['TOTAP_SUMINS'] == 230000)),
        (poldata_df["GF_" + 'SUM_ASSURED'] == "G6"),
        (poldata_df["GF_" + 'SUM_ASSURED'] == "G7"),
        (poldata_df["GF_" + 'SUM_ASSURED'] == "G8"),
        (poldata_df["GF_" + 'SUM_ASSURED'] == "G9"),
    ]

    results = [
        poldata_df['SUMINS'] + poldata_df['BLANKSA'],
        poldata_df['SUM_ASSURED_x2'],
        poldata_df['BLANKSA'],
        poldata_df['BLANKSA'] * poldata_df['BILLFREQ'] * poldata_df['RCESTRM'],
        poldata_df['SUM_ASSURED_x2'],
        225000,
        poldata_df['TOTAP_SUMINS'],
        225000,
        poldata_df['TOTAP_SUMINS'],
        poldata_df['TEMP_SA'],
        poldata_df['RETINCM'] * poldata_df['GCPFREQ'],
        poldata_df['VARSA'] * poldata_df['TNOUNITX']
    ]

    poldata_df['SUM_ASSURED'] = np.select(conditions, results, default = 0)
    # poldata_df['SUM_ASSURED'] = comm_func.apply_rounding_on_col(poldata_df, 'SUM_ASSURED', 2)
    poldata_df['SUM_ASSURED'] = poldata_df['SUM_ASSURED'].round(2)
    poldata_df = comm_func.default_col_null_to_zero(poldata_df, 'SUM_ASSURED')
    poldata_df = comm_func.drop_col(poldata_df, 'GF_SUM_ASSURED')

    return poldata_df

@comm_func.timer_func
def add_col_prem_paybl_y_2(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    # WC - consider extending this to a dynamic list (or may not be required if using a config table).
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'PREM_PAYBL_Y_2')
    
    conditions = [
        (poldata_df["GF_" + 'PREM_PAYBL_Y_2'] == "G0"),
        (poldata_df["GF_" + 'PREM_PAYBL_Y_2'] == "G1"),
        (poldata_df["GF_" + 'PREM_PAYBL_Y_2'] == "G2"),
        (poldata_df["GF_" + 'PREM_PAYBL_Y_2'] == "G3"),
    ]
 
    results = [
        0, 
        1, 
        poldata_df['PREM_PAYBL_Y_1'], 
        10
    ]
    
    poldata_df['PREM_PAYBL_Y_2'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'PREM_PAYBL_Y_2')
    return poldata_df

# --------------------------------------------------------------------
# Rank 3
# --------------------------------------------------------------------
@comm_func.timer_func
def add_col_profile(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    # WC - consider extending this to a dynamic list (or may not be required if using a config table).
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'POLFEE')
    
    conditions = [
        (poldata_df["GF_" + 'POLFEE'] == "G0"),
        ((poldata_df["GF_" + 'POLFEE'] == "G1") & (poldata_df["BASIC_IND"] == 1 ))
    ]
 
    results = [
        0, 
        poldata_df["POLFEE_ANNPREM"]
    ]
    
    poldata_df['POLFEE'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'POLFEE')

    return poldata_df

@comm_func.timer_func
def upd_col_cic_ben_pp(poldata_df, grouping_table_df):
    """
    Updates the 'CIC_BEN_PP' column in the `poldata_df` DataFrame based on defined conditions 
    and values from the `grouping_table_df` DataFrame.
 
    Args:
        poldata_df (pd.DataFrame): The DataFrame containing the 'CIC_BEN_PP' column to be updated.
        grouping_table_df (pd.DataFrame): The DataFrame containing the lookup information.
 
    Returns:
        pd.DataFrame: The updated `poldata_df` DataFrame with the modified 'CIC_BEN_PP' column.
    """
    poldata_df = poldata_df.rename(columns={
        'CIC_BEN_PP': 'CIC_BEN_PP_x0'
    })

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'CIC_BEN_PP')
    
    conditions = [
        (poldata_df["GF_" + 'CIC_BEN_PP'] == "G0"),
        (poldata_df["GF_" + 'CIC_BEN_PP'] == "G1"),
        (poldata_df["GF_" + 'CIC_BEN_PP'] == "G2"),
        (poldata_df["GF_" + 'CIC_BEN_PP'] == "G3"),
    ]
 
    results = [
        0, 
        poldata_df['CIC_BEN_PP_x0'], 
        poldata_df['CC_SUM_ASSD'], 
        poldata_df['SUM_ASSURED'] * 1.2
    ]
    
    poldata_df['CIC_BEN_PP'] = np.select(conditions, results, default = 0)
    poldata_df['CIC_BEN_PP'] = comm_func.apply_rounding_on_col(poldata_df, 'CIC_BEN_PP', 0)
    # poldata_df['CIC_BEN_PP'] = poldata_df['CIC_BEN_PP'].round(0)
    poldata_df = comm_func.default_col_null_to_zero(poldata_df, 'CIC_BEN_PP')
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'CIC_BEN_PP')
    return poldata_df

@comm_func.timer_func
def upd_col_product_name(poldata_df, grouping_table_df):
    """
    Updates the 'PRODUCT_NAME' column in the `poldata_df` DataFrame based on defined conditions 
    and values from the `grouping_table_df` DataFrame.
 
    Args:
        poldata_df (pd.DataFrame): The DataFrame containing the 'PRODUCT_NAME' column to be updated.
        grouping_table_df (pd.DataFrame): The DataFrame containing the lookup information.
 
    Returns:
        pd.DataFrame: The updated `poldata_df` DataFrame with the modified 'PRODUCT_NAME' column.
    """
 
    poldata_df = poldata_df.rename(columns={
        'PRODUCT_NAME': 'PRODUCT_NAME_x0'
    })

    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'PRODUCT_NAME')
    
    conditions = [
        (poldata_df["GF_" + 'PRODUCT_NAME'] == "G0"),
        (poldata_df["GF_" + 'PRODUCT_NAME'] == "G1") & (poldata_df['BASIC_IND'] == 1) & ((poldata_df['CSTATCODE'] == 'PU') | (poldata_df['CPSTATCODE'] == 'PU')),
        (poldata_df["GF_" + 'PRODUCT_NAME'] == "G1") & ((poldata_df['CSTATCODE'] == 'PU') | (poldata_df['CPSTATCODE'] == 'PU')),
        (poldata_df["GF_" + 'PRODUCT_NAME'] == "G2") & (poldata_df['BASIC_IND'] == 1) & ((poldata_df['CSTATCODE'] == 'PU') | (poldata_df['CPSTATCODE'] == 'PU')),
        (poldata_df["GF_" + 'PRODUCT_NAME'] == "G2") & ((poldata_df['CSTATCODE'] == 'PU') | (poldata_df['CPSTATCODE'] == 'PU'))
    ]
 
    results = [
        "", 
        "CNPEAL", 
        "XXXXXX", 
        "CNPWLL", 
        "XXXXXX"
    ]
    
    poldata_df['PRODUCT_NAME'] = np.select(conditions, results, default = poldata_df['PRODUCT_NAME_x0'])
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'PRODUCT_NAME')
    return poldata_df
 
@comm_func.timer_func
def add_col_waived_term(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    # WC - consider extending this to a dynamic list (or may not be required if using a config table).
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'WAIVED_TERM')
    
    conditions = [
        (poldata_df["GF_" + 'WAIVED_TERM'] == "G1") & ((poldata_df['FILE'] == const_var.ET) | (poldata_df['FUNDTYPE'] == 2) | (poldata_df['WAIV_IND'] == 0)),
        (poldata_df["GF_" + 'WAIVED_TERM'] == "G1") & (poldata_df['WAIVED_IND'] == 1),
        (poldata_df["GF_" + 'WAIVED_TERM'] == "G1") & (poldata_df['WAIVED_IND'] == 2),
        (poldata_df["GF_" + 'WAIVED_TERM'] == "G1") & (poldata_df['WAIVED_IND'] == 3)
    ]
 
    results = [ 
        0, 
        poldata_df['WLIST_TERM'], 
        poldata_df.apply(lambda row: min(25 - row['AGE_AT_ENTRY_2'], row['WLIST_TERM']), axis=1),
        poldata_df['WLIST_TERM']
    ]
    
    poldata_df['WAIVED_TERM'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'WAIVED_TERM')
    return poldata_df

# --------------------------------------------------------------------
# Rank 4
# --------------------------------------------------------------------
@comm_func.timer_func
def add_col_as_curr_mth_2(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    # WC - consider extending this to a dynamic list (or may not be required if using a config table).
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'AS_CURR_MTH_2')
    
    conditions = [
        (poldata_df["GF_" + 'AS_CURR_MTH_2'] == "G0"),
        (poldata_df["GF_" + 'AS_CURR_MTH_2'] == "G1"),
        (poldata_df["GF_" + 'AS_CURR_MTH_2'] == "G2") & (poldata_df['PRODUCT_NAME'].isin(['C6CPC_', 'C6CB__', 'C6PCDR', 'C6DE__', 'C6PWGU', 'C6PDRU', 'C6PWGS', 'C6PEDK', 'C6CPH_', 'C6PCE_']))
    ]
 
    results = [
        0, 
        poldata_df['AS_CURR_MTH_1'], 
        1
    ]
    
    poldata_df['AS_CURR_MTH_2'] = np.select(conditions, results, default = poldata_df['AS_CURR_MTH_1'])
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'AS_CURR_MTH_2')
    return poldata_df
 
@comm_func.timer_func
def add_col_as_curr_year_2(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    # WC - consider extending this to a dynamic list (or may not be required if using a config table).
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'AS_CURR_YEAR_2')
    
    conditions = [
        (poldata_df["GF_" + 'AS_CURR_YEAR_2'] == "G0"),
        (poldata_df["GF_" + 'AS_CURR_YEAR_2'] == "G1"),
        (poldata_df["GF_" + 'AS_CURR_YEAR_2'] == "G2") & (poldata_df['PRODUCT_NAME'].isin(['C6CPC_', 'C6CB__', 'C6PCDR', 'C6DE__', 'C6PWGU', 'C6PDRU', 'C6PWGS', 'C6PEDK', 'C6CPH_', 'C6PCE_']))
    ]
 
    results = [
        0, 
        poldata_df['AS_CURR_YEAR_1'], 
        2012
    ]
    
    poldata_df['AS_CURR_YEAR_2'] = np.select(conditions, results, default = poldata_df['AS_CURR_YEAR_1'])
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'AS_CURR_YEAR_2')
    return poldata_df

@comm_func.timer_func
def add_col_annual_prem_2(poldata_df, grouping_table_df):

    # Adds a new column 'target_col_name' based on defined conditions.
    # WC - consider extending this to a dynamic list (or may not be required if using a config table).
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'ANNUAL_PREM_2')
    
    conditions = [
        (poldata_df["GF_" + 'ANNUAL_PREM_2'] == "G0"),
        (poldata_df["GF_" + 'ANNUAL_PREM_2'] == "G1"),
        ((poldata_df["GF_" + 'ANNUAL_PREM_2'] == "G2") & (poldata_df['BASIC_IND'] == 1)),
        ((poldata_df["GF_" + 'ANNUAL_PREM_2'] == "G3") & (poldata_df['CSTATCODE'] != "PU") & (poldata_df['CPSTATCODE'] != "PU")),
        ((poldata_df["GF_" + 'ANNUAL_PREM_2'] == "G4") & (poldata_df['FILE'].isin([const_var.IF,const_var.ET])) & (poldata_df['BASIC_PSM_IND'] != 0)),
        ((poldata_df["GF_" + 'ANNUAL_PREM_2'] == "G4") & (poldata_df['FILE'].isin([const_var.IF,const_var.ET])) & (poldata_df['BASIC_PSM_IND'] == 0))
    ]
 
    results = [
        0, 
        (poldata_df['ANNUAL_PREM_1']), 
        (poldata_df['ANNUAL_PREM_1'] + poldata_df['POLFEE']),
        (poldata_df['TOT_AP']), 
        (poldata_df['ANNUAL_PREM_1']),
        (poldata_df['TOT_AP'])
    ]
    
    poldata_df['ANNUAL_PREM_2'] = np.select(conditions, results, default = poldata_df['ANNUAL_PREM_1'])
    poldata_df = comm_func.default_col_null_to_zero(poldata_df, 'ANNUAL_PREM_2')

    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'ANNUAL_PREM_2')

    return poldata_df

# --------------------------------------------------------------------
# Rank 5
# --------------------------------------------------------------------
@comm_func.timer_func
def add_col_mat_ben_pp_2(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    # WC - consider extending this to a dynamic list (or may not be required if using a config table).
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'MAT_BEN_PP_2')
    
    # fill the default value
    poldata_df['MAT_BEN_PP_2'] = poldata_df['MAT_BEN_PP_1']
    # Fill the value for G0
    poldata_df.loc[poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G0", 'MAT_BEN_PP_2'] = 0
    
    # set the value for G2
    poldata_df.loc[poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G2", 'MAT_BEN_PP_2'] = 1.4 * poldata_df['ANNUAL_PREM_2'] * poldata_df['PREM_PAYBL_Y_2']
 
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G2") & (poldata_df['POL_TERM_Y'] <= 40), 'MAT_BEN_PP_2'] = 1.2 * poldata_df['ANNUAL_PREM_2'] * poldata_df['PREM_PAYBL_Y_2']
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G2") & (poldata_df['POL_TERM_Y'] <= 30), 'MAT_BEN_PP_2'] = poldata_df['ANNUAL_PREM_2'] * poldata_df['PREM_PAYBL_Y_2']
    
    # Fill the value for G3 
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G3") & (poldata_df["SUM_ASSURED"] == 20000), 'MAT_BEN_PP_2'] = 8060
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G3") & (poldata_df["SUM_ASSURED"] == 12000), 'MAT_BEN_PP_2'] = 8060
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G3") & (poldata_df["SUM_ASSURED"] == 51500), 'MAT_BEN_PP_2'] = 17560
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G3") & (poldata_df["SUM_ASSURED"] == 27000), 'MAT_BEN_PP_2'] = 17560
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G3") & (poldata_df["SUM_ASSURED"] == 90000), 'MAT_BEN_PP_2'] = 27060
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G3") & (poldata_df["SUM_ASSURED"] == 43500), 'MAT_BEN_PP_2'] = 27060
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G3") & (poldata_df["SUM_ASSURED"] == 123000), 'MAT_BEN_PP_2'] = 36560
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G3") & (poldata_df["SUM_ASSURED"] == 60000), 'MAT_BEN_PP_2'] = 36560
 
    # Fill the value for G4
    poldata_df.loc[poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G4", 'MAT_BEN_PP_2'] = poldata_df['ANNUAL_PREM_2'] * poldata_df['PREM_PAYBL_Y_2']
 
    # Fill the value for G5
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G5") & (poldata_df["PRODUCT_NAME"].isin(['C6PSEU', 'C6PSES'])) & \
                   (poldata_df['ANNUAL_PREM_2'] == 0), 'MAT_BEN_PP_2'] = poldata_df['SUM_ASSURED']
    
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G5") & (poldata_df["PRODUCT_NAME"].isin(['C6PSEU', 'C6PSES'])) & \
                   (poldata_df['ANNUAL_PREM_2'] != 0), 'MAT_BEN_PP_2'] = poldata_df['ANNUAL_PREM_2'] * poldata_df['PREM_PAYBL_Y_2']
    
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G5") & (poldata_df["PRODUCT_NAME"].isin(['C6PSEU', 'C6PSES'])) & \
                   (poldata_df['ANNUAL_PREM_2'] != 0) & (poldata_df['POL_TERM_Y'] == 20) & (poldata_df['PREM_PAYBL_Y_2'] == 15), 
                   'MAT_BEN_PP_2'] = 1.08 * poldata_df['ANNUAL_PREM_2'] * poldata_df['PREM_PAYBL_Y_2']
    
    poldata_df.loc[(poldata_df["GF_" + 'MAT_BEN_PP_2'] == "G5") & (poldata_df["PRODUCT_NAME"].isin(['C6PSEU', 'C6PSES'])) & \
                   (poldata_df['ANNUAL_PREM_2'] != 0) & (poldata_df['POL_TERM_Y'] == 25) & (poldata_df['PREM_PAYBL_Y_2'] == 20), 
                   'MAT_BEN_PP_2'] = 1.18 * poldata_df['ANNUAL_PREM_2'] * poldata_df['PREM_PAYBL_Y_2']
 

    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'MAT_BEN_PP_2')
 
    return poldata_df
 
@comm_func.timer_func
def add_col_g_mat_ben_pp(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    # WC - consider extending this to a dynamic list (or may not be required if using a config table).
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'G_MAT_BEN_PP')
    
    conditions = [
        (poldata_df["GF_" + 'G_MAT_BEN_PP'] == "G0"),
        (poldata_df["GF_" + 'G_MAT_BEN_PP'] == "G1")
    ]
 
    results = [
        0, 
        poldata_df['ANNUAL_PREM_2'] * poldata_df['PREM_PAYBL_Y_2'] 
    ]
    
    poldata_df['G_MAT_BEN_PP'] = np.select(conditions, results, default = 0)
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'G_MAT_BEN_PP')
    return poldata_df

@comm_func.timer_func
def add_col_rider_ben_pp(poldata_df, grouping_table_df):
    # Adds a new column 'target_col_name' based on defined conditions.
    # WC - consider extending this to a dynamic list (or may not be required if using a config table).
    poldata_df = lookup_grouping_col(poldata_df, grouping_table_df, 'RIDER_BEN_PP')
    
    conditions = [
        (poldata_df["GF_" + 'RIDER_BEN_PP'] == "G0"),
        (poldata_df["GF_" + 'RIDER_BEN_PP'] == "G1") & ((poldata_df['CSTATCODE'] == 'PU') | (poldata_df['CPSTATCODE'] == 'PU'))
    ]
 
    results = [
        0, 
        0, 
    ]
    
    poldata_df['RIDER_BEN_PP'] = np.select(conditions, results, default = poldata_df['ANNUAL_PREM_2'])
    poldata_df = comm_func.default_col_null_to_zero(poldata_df, 'RIDER_BEN_PP')
    poldata_df = comm_func.drop_col(poldata_df, "GF_" + 'RIDER_BEN_PP')
    return poldata_df

# Output BASIC_POLFEE E2E Table after Rank 5 Calculation
@comm_func.timer_func
def output_basic_polfee(poldata_df):

    polfee_list = const_var.polfee_cnttype_list()

    basic_polfee_df = poldata_df[((poldata_df['CNTTYPE'].isin(polfee_list)) & (poldata_df['BASIC_IND'] == 1))]
    basic_polfee_df = basic_polfee_df[['FILE', 'CNTTYPE', 'CRTABLE', 'CHDRNUM', 'POLFEE_ANNPREM']]
    basic_polfee_df = basic_polfee_df.sort_values(by = ['FILE', 'CNTTYPE', 'CRTABLE', 'CHDRNUM'])

    return basic_polfee_df

# Output BLANKSA by Group E2E Table after Rank 5 Calculation
@comm_func.timer_func
def output_blanksa_summary(poldata_df):

    ELSP_ROP_list = const_var.elsp_list()

    filtered_elsp_df = poldata_df[poldata_df['CRTABLE'].isin(ELSP_ROP_list)]
    filtered_elsp_df = filtered_elsp_df[['FILE', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE', 'CHDRNUM', 'LIFE_IND', 'BLANKSA', 'BILLFREQ', 'RCESTRM', 'SUM_ASSURED']]
    filtered_elsp_df.sort_values(by = ['FILE', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE', 'CHDRNUM', 'LIFE_IND'])

    filtered_elrp_df = poldata_df[poldata_df['CRTABLE'] == "ROPR"]
    filtered_elrp_df = filtered_elrp_df[['FILE', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE', 'CHDRNUM', 'LIFE_IND', 'BLANKSA', 'BILLFREQ', 'RCESTRM', 'SUM_ASSURED']]
    filtered_elrp_df = filtered_elrp_df.sort_values(by = ['FILE', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE', 'CHDRNUM', 'LIFE_IND'])

    MRTA_ROP_list = const_var.mrta_rop_list()

    filtered_mrta_rop_df = poldata_df[poldata_df['CRTABLE'].isin(MRTA_ROP_list)]
    filtered_mrta_rop_df = filtered_mrta_rop_df[['FILE', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE', 'CHDRNUM', 'LIFE_IND', 'BLANKSA']]
    filtered_mrta_rop_df = filtered_mrta_rop_df.sort_values(by = ['FILE', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE', 'CHDRNUM', 'LIFE_IND'])

    MRTA_BLANKSA_list = const_var.mrta_blanksa_list()

    filtered_mrta_df = poldata_df[(poldata_df['CRTABLE'].isin(MRTA_BLANKSA_list)) & poldata_df['FUNDSRCE'] == "B"]
    filtered_mrta_df = filtered_mrta_df[['FILE', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE', 'CHDRNUM', 'LIFE_IND', 'BLANKSA']]
    filtered_mrta_df = filtered_mrta_df.sort_values(by = ['FILE', 'PRODUCT_NAME', 'CNTTYPE', 'CRTABLE', 'CHDRNUM', 'LIFE_IND'])

    return filtered_elsp_df, filtered_elrp_df, filtered_mrta_rop_df, filtered_mrta_df

@comm_func.timer_func
def bundled_function1(poldata_df, grouping_table_df):
       
    poldata_df = add_col_age2_atentry(poldata_df, grouping_table_df)
    poldata_df = add_col_annual_prem_1(poldata_df, grouping_table_df)
    poldata_df = add_col_as_curr_mth_1(poldata_df, grouping_table_df)
    poldata_df = add_col_as_curr_year_1(poldata_df, grouping_table_df)
    poldata_df = add_col_basic(poldata_df, grouping_table_df)
    poldata_df = add_col_blanksa(poldata_df, grouping_table_df)
    poldata_df = add_col_cc_ann_prem(poldata_df, grouping_table_df)
    poldata_df = add_col_comm_ind(poldata_df, grouping_table_df)
    poldata_df = add_col_comp_ben_sa(poldata_df, grouping_table_df)
    poldata_df = add_col_defer_per_y(poldata_df, grouping_table_df)
    poldata_df = add_col_gstprem(poldata_df, grouping_table_df)
    poldata_df = add_col_incsa_pct(poldata_df, grouping_table_df)
    poldata_df = add_col_init_pols_if(poldata_df, grouping_table_df)
    poldata_df = add_col_mme_prem(poldata_df, grouping_table_df)
    poldata_df = add_col_mom_i(poldata_df, grouping_table_df)
    poldata_df = add_col_mort_int_pc(poldata_df, grouping_table_df)
    poldata_df = add_col_no_ls_claim(poldata_df, grouping_table_df)
    poldata_df = add_col_nonpar_prem(poldata_df, grouping_table_df)
    poldata_df = add_col_pol_term_y(poldata_df, grouping_table_df)
    poldata_df = add_col_ppt_code(poldata_df, grouping_table_df)
    poldata_df = add_col_prem_freq(poldata_df, grouping_table_df)
    poldata_df = add_col_prem_months(poldata_df, grouping_table_df)
    poldata_df = add_col_prem_paybl_y_1(poldata_df, grouping_table_df)
 
    poldata_df = add_col_retirement_age(poldata_df, grouping_table_df)
    poldata_df = add_col_sex(poldata_df, grouping_table_df)
    poldata_df = add_col_sex2(poldata_df, grouping_table_df)
    poldata_df = add_col_smoker_stat(poldata_df, grouping_table_df)
    poldata_df = add_col_smoker2_stat(poldata_df, grouping_table_df)
    poldata_df = add_col_substd_prem(poldata_df, grouping_table_df)
    poldata_df = add_col_treaty_id_treaty(poldata_df, grouping_table_df)
    poldata_df = add_col_age_at_entry_1(poldata_df, grouping_table_df)
    poldata_df = upd_col_fundtype(poldata_df, grouping_table_df)
    poldata_df = add_col_prem_mode(poldata_df, grouping_table_df)
    poldata_df = add_col_series_ind(poldata_df, grouping_table_df)
    poldata_df = upd_col_single_prem(poldata_df, grouping_table_df)
    poldata_df = add_col_cc_term_y(poldata_df, grouping_table_df)
    poldata_df = upd_col_zrevbns(poldata_df, grouping_table_df)
 
    return poldata_df
 
@comm_func.timer_func
def bundled_function2(poldata_df, lookup_list_tbl):
       
    poldata_df = add_col_dth_mult(poldata_df, lookup_list_tbl)
    poldata_df = upd_col_ppt_code(poldata_df)
 
    poldata_df = add_col_group_zero(poldata_df)
   
    return poldata_df
 
@comm_func.timer_func
def bundled_function3(poldata_df, grouping_table_df):
       
    poldata_df = add_col_death_ben(poldata_df, grouping_table_df)
    poldata_df = add_col_init_decb_if(poldata_df, grouping_table_df)
    poldata_df = add_col_link_prem(poldata_df, grouping_table_df)
    poldata_df = add_col_waived_ind(poldata_df, grouping_table_df)
    poldata_df = add_col_waived_prem(poldata_df, grouping_table_df)
    poldata_df = add_col_age_at_entry_2(poldata_df, grouping_table_df)
    poldata_df = add_col_prem_paybl_y_2(poldata_df, grouping_table_df)
    poldata_df = add_col_mat_ben_pp_1(poldata_df, grouping_table_df)
    poldata_df = add_col_plan_code(poldata_df, grouping_table_df)
    poldata_df = add_col_basic_ind(poldata_df, grouping_table_df)
    poldata_df = upd_col_cc_sum_assd(poldata_df, grouping_table_df)
    poldata_df = upd_col_sum_assured(poldata_df, grouping_table_df)
   
    return poldata_df
 
@comm_func.timer_func
def bundled_function4(poldata_df, grouping_table_df):
       
    poldata_df = add_col_profile(poldata_df, grouping_table_df)
    poldata_df = add_col_waived_term(poldata_df, grouping_table_df)
    poldata_df = upd_col_product_name(poldata_df, grouping_table_df)
    poldata_df = upd_col_cic_ben_pp(poldata_df, grouping_table_df)
 
    return poldata_df
 
@comm_func.timer_func
def bundled_function5(poldata_df, grouping_table_df):
       
    poldata_df = add_col_as_curr_mth_2(poldata_df, grouping_table_df)
    poldata_df = add_col_as_curr_year_2(poldata_df, grouping_table_df)
    poldata_df = add_col_annual_prem_2(poldata_df, grouping_table_df)
 
    return poldata_df
 
@comm_func.timer_func
def bundled_function6(poldata_df, grouping_table_df):
       
    poldata_df = add_col_g_mat_ben_pp(poldata_df, grouping_table_df)
    poldata_df = add_col_rider_ben_pp(poldata_df, grouping_table_df)
    poldata_df = add_col_mat_ben_pp_2(poldata_df, grouping_table_df)
 
    return poldata_df


