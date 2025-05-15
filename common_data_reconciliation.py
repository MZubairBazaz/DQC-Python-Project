import pandas as pd
import re

class Data_Reconciliation:
    def __init__(self):
        
        # Specify the column headers
        headers = ['Stage', 'Description', 'Field Type', 'NO_RECORDS','NO_RECORDS_UPDATED','SUM_ASSURED','ANNUAL_PREM']

        # Create an empty DataFrame with these column headers
        val_table = pd.DataFrame(columns=headers)
        self.val_table = val_table
        self.val_table_pl = val_table
 
    def add_row_mvmt(self, poldata_df_prior, poldata_df_curr):
        
        stage = len(self.val_table)
        no_records_diff = len(poldata_df_curr) - len(poldata_df_prior)

        col_sum = poldata_df_prior.get('SUM_ASSURED')
        sum_assured_prior = "NA" if col_sum is None else col_sum.sum()

        col_sum = poldata_df_prior.get('ANNUAL_PREM')
        annual_prem_prior = "NA" if col_sum is None else col_sum.sum()

        col_sum = poldata_df_curr.get('SUM_ASSURED')
        sum_assured_curr = "NA" if col_sum is None else col_sum.sum()

        col_sum = poldata_df_curr.get('ANNUAL_PREM')
        annual_prem_curr = "NA" if col_sum is None else col_sum.sum()

        if (sum_assured_curr == "NA") or (sum_assured_prior == "NA"):
            sa_diff = "NA"
        else:
            sa_diff = sum_assured_curr - sum_assured_prior

        if (annual_prem_curr == "NA") or (annual_prem_prior == "NA"):
            ap_diff = "NA"
        else:
            ap_diff = annual_prem_curr - annual_prem_prior

        # Find the new columns
        current_columns = set(poldata_df_curr.columns)
        prior_columns = set(poldata_df_prior.columns)
        new_columns = current_columns - prior_columns

        # Get new columns ending with _x followed by any number
        filtered_columns = [col for col in new_columns if re.search(r'_x\d+$', col)]

        # Create a new list of cleaned column names
        cleaned_columns = [re.sub("_x\d+$", "", col) for col in filtered_columns]

        # Remove duplicates
        cleaned_columns_no_duplicates = list(set(cleaned_columns))

        # Get common columns with prev poldata
        common_columns = prior_columns.intersection(cleaned_columns_no_duplicates)
        common_columns = list(common_columns)

        # Creating 2 new df for comparison
        new_current_df = poldata_df_curr[common_columns]
        new_prior_df = poldata_df_prior[common_columns]
        num_diff_rows = (~new_current_df.eq(new_prior_df).all(axis=1)).sum()

        # 2 Decimal places
        sa_diff = f"{sa_diff:.2f}" if sa_diff != "NA" else sa_diff
        ap_diff = f"{ap_diff:.2f}" if ap_diff != "NA" else ap_diff

        newrow = {'Stage': stage, 'Description': val_item_list[stage],'Field Type': 'Movement', 'NO_RECORDS': no_records_diff, 'NO_RECORDS_UPDATED': num_diff_rows, 'SUM_ASSURED': sa_diff, 'ANNUAL_PREM': ap_diff}
        self.val_table = pd.concat([self.val_table, pd.DataFrame([newrow])], ignore_index=True)

    def add_row_agg(self, poldata_df):

        stage = len(self.val_table)
        NO_RECORDS = len(poldata_df)

        col_sum = poldata_df.get('SUM_ASSURED')
        sum_assured = "NA" if col_sum is None else col_sum.sum()

        col_sum = poldata_df.get('ANNUAL_PREM')
        annual_prem = "NA" if col_sum is None else col_sum.sum()

        # 2 Decimal places
        sum_assured = f"{sum_assured:.2f}" if sum_assured != "NA" else sum_assured
        annual_prem = f"{annual_prem:.2f}" if annual_prem != "NA" else annual_prem

        newrow = {'Stage': stage, 'Description': val_item_list[stage], 'Field Type': 'Aggregate', 'NO_RECORDS': NO_RECORDS, 'NO_RECORDS_UPDATED': 'NA', 'SUM_ASSURED': sum_assured, 'ANNUAL_PREM': annual_prem}
        self.val_table = pd.concat([self.val_table, pd.DataFrame([newrow])], ignore_index=True)
    
    def add_row_agg_prod_lvl(self, poldata_df, prod_name, val_type):

        stage = len(self.val_table_pl)
        NO_RECORDS = len(poldata_df)
        
        col_sum = poldata_df.get('SUM_ASSURED').astype(float)
        sum_assured = "NA" if col_sum is None else col_sum.sum()

        col_sum = poldata_df.get('ANNUAL_PREM').astype(float)
        annual_prem = "NA" if col_sum is None else col_sum.sum()

        # 2 Decimal places
        sum_assured = f"{sum_assured:.2f}" if sum_assured != "NA" else sum_assured
        annual_prem = f"{annual_prem:.2f}" if annual_prem != "NA" else annual_prem

        newrow = {'Stage': stage, 'Description': "Output_prod_level: " + str(prod_name) + "_" + str(val_type), 'Field Type': 'Aggregate', 'NO_RECORDS': NO_RECORDS, 'NO_RECORDS_UPDATED': 'NA', 'SUM_ASSURED': sum_assured, 'ANNUAL_PREM': annual_prem}
        self.val_table_pl = pd.concat([self.val_table_pl, pd.DataFrame([newrow])], ignore_index=True)

    def output_val_table(self, filename):
        self.val_table.to_csv('./Outputs/' + filename, index=False)
    
    def output_val_table_append(self, filename):
        self.val_table_pl.to_csv('./Outputs/' + filename, mode = 'a', header = False, index=False)

    def clear_val_table(self):
       self.val_table_pl.drop(self.val_table_pl.index, inplace=True)


# Make sure the list is consistent with the occurences in the main function where the validation class is called
val_item_list =[
    'Valuation Extract',
    'Phase 1 Starting',
    '   Phase 1 method 1',
    '   Phase 1 method 2',
    'Phase 2 starting',
    '   Phase 2 rank 1',
    '   Phase 2 rank 1 adjustment',
    '   Phase 2 rank 2',
    '   Phase 2 rank 3',
    '   Phase 2 rank 4',
    '   Phase 2 rank 5',
    'Phase 3 Starting',
    '   Phase 3 method 1',
    '   Phase 3 method 2',
    'Output - before par cohort index',
    '   Par cohort index',
    'Output - before asset share',
    '   Asset share',
    'Output - after asset share'
]
