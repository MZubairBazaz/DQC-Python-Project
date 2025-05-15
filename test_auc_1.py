def add_col_pruterm_basic(poldata_df):
    cnttype_filter_list = const_var.cnttype_pruterm_basic_list()
    
    crtable_filter_basic_pruterm = const_var.crtable_pruterm_list()
    crtable_filter_basic_prumortgage = const_var.crtable_prumortgage_list()
    crtable_filter_basic_ccp = const_var.crtable_ccp_list()
    crtable_filter_basic_ccpa = const_var.crtable_ccpa_list()
    #Step 1
    filtered_df = poldata_df.filter((poldata_df.FILE == const_var.IF) & (poldata_df.CNTTYPE.isin(cnttype_filter_list)))

    #Step 2
    filtered_df = filtered_df.withColumn("BASIC_PRUTERM",
    when((filtered_df.CSTATCODE == "IF") & (filtered_df.CRTABLE.isin(crtable_filter_basic_pruterm)), lit(1))
     .otherwise(lit(5)))

    #Step 3
    filtered_df = filtered_df.withColumn("BASIC_PRUMORTGAGE",
    when((filtered_df.CSTATCODE == "IF") & (filtered_df.CRTABLE.isin(crtable_filter_basic_prumortgage)), lit(2))
     .otherwise(lit(5)))
    
    #Step 4
    filtered_df = filtered_df.withColumn("BASIC_CCP",
    when((filtered_df.CSTATCODE == "IF") & (filtered_df.CRTABLE.isin(crtable_filter_basic_ccp)), lit(3))
     .otherwise(lit(5)))
    
    #Step 5
    filtered_df = filtered_df.withColumn("BASIC_CCPA",
    when((filtered_df.CSTATCODE == "IF") & (filtered_df.CRTABLE.isin(crtable_filter_basic_ccpa)), lit(4))
     .otherwise(lit(5)))
    
    #Step 6
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
