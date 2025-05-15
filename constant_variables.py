# Import Python Packages
import pandas as pd
import numpy as np
from dateutil.relativedelta import relativedelta as rd

def cnttype_enhanced_ind_list():

    list = ["7GM", "7MC", "7MG"]

    return list

def crtable_enhanced_ind_list():

    list = ["7GMB", "7G2B", "7MCB", "7M2B", "7MGB", "7M3B", "7GMI", "7G2I", "7MCI", 
                    "7M2I", "7MGI", "7M3I"]
    
    return list

def crtable2_enhanced_ind_list():

    list = ["5LYB", "4LDB", "6BCP", "ADDA", "ADDD", "CCPJ", "ADDL", "ADDN", "ADDR", 
                      "ADDP", "5PMS", "5PML", "5PTC", "5PTS", "5PTL", "CPPH", "CCPG", "CCPK", 
                      "CCPI", "CPCG", "CPPI", "CPPJ"]
    
    return list

def crtable3_enhanced_ind_list():

    list = ["5PTB", "5BCP", "5PMB", "5SWB", "5WTA", "5WTB", "5WTC", "5WTD", "PMMC"]

    return list

def dmtm_filter_list():

    list = ["EAC", "EHI", "HIP", "LCP", "LIP", "LPC", "LPR", "LPS", 
                 "LSP", "MPP", "OHC", "OHD", "OHJ", "OHI", "ACE", "PCC", 
                 "PIC", "PLV", "PMA", "PSC", "PVC", "PWC", "PCP"]
    
    return list

def mrta_filter_list():
    
    # 24/9 WC: fixed typo from TIO2 to TI02
    list =  ["TI02", "2M2I", "3A2I", "4M2I", "6MCI", "7GMI", "7MCI", "7MGI", "GMAI", 
                        "2MCI", "3AMI", "4MCI", "5GMI", "6M2I", "7G2I", "7M2I", "7M3I"]
    
    return list

def polfee_filter_list():

    list = ["2PFE", "BTFE", "NPFE", "PFEE", "POFE", "4PFE", "ELFE", "NPOF", "PLFE", "POLF"]

    return list

def pmle_filter_list():

    list = ["PMLE", "PME3", "PMEO", "PMEW", "PME2", "PMLA", "PMEP"]

    return list

def cnttype_list1_blanksa():

    list = ["6PD", "6PC"]

    return list

def cnttype_list2_blanksa():

    list = ["5ES", "5ER", "5EF", "5EL"]

    return list

def cnttype_list3_blanksa():

    list = ["1GM", "1LP", "2MC", "3AM", "4MC", "4SP", "5GM", "5SP", "6MC", "7GM", "7MC", "7MG", "GMA"]

    return list

def crtable_list1_blanksa():

    list = ["6PCB", "6PDB"]

    return list

def crtable_list2_blanksa():

    list = ["5ESB", "5ERB", "ROPS", "ROPR", "ELFE", "5EFB", "ROPF", "5ELB", "ROPM"]

    return list

def crtable_list3_blanksa():

    list = ["1GMB", "1G2B", "1G3B", "TM02", "2MCB", "2M2B", "3AMB", "3A2B", "4MCB", "4M2B", "4SPB", "5GMB", 
                            "5SPB", "6MCB", "6M2B", "7GMB", "7G2B", "7MCB", "7M2B", "7MGB", "7M3B", "1RP1", "1RP2", "2RP1", "2RP2", 
                            "3RP1", "3RP2", "4RP1", "6RP1", "7RP1", "7RP2", "7RP3", "7RP4", "7RP5", "GMAB", "RP07"]
    
    return list

def cnttype_pruterm_basic_list():

    list = ["5BT", "5PT", "4BS", "4BT", "4BU", "4BV", "6PD", "6HO", "6HR", "6PC", "5PC", "5PA", "4PC", "4PA", 
                           "5PS", "5SA", "4PS", "4SA", "4SC", "5PE", "5PB", "4PE", "4PB", "5PL", "5PJ", "4PL", "4PJ", "4PF", 
                           "4FS", "3CE", "3PL"]
    
    return list

def crtable_pruterm_list():

    list = ["5PTB", "4PTB", "6PTB", "6PTM", "6CPT", "5PTC", "4PTC", "5PTS", "4PTE", "5PTL", "4PTL", "4PTF"]

    return list

def crtable_prumortgage_list():

    list = ["5PMB", "4PMB", "6PLB", "6PLG", "6PLC", "6PCM", "4PMC", "5PMS", "4PMS", "4PMV", "5PML", "4PML", "4PMG"]

    return list

def crtable_ccp_list():

    list = ["5BCP", "CCPA", "6PCP", "CCPG", "CCPB", "CCPI", "CCPD", "CCPE", "CPCG", "CPCC", "CCPJ", "CCPH", "CCPF", "3CPC", "CPC3"]

    return list

def crtable_ccpa_list():

    list = ["CPPB", "CPPH", "CPPD", "CPPK", "CPPG", "CPPI", "CPPE", "CPPJ", "CPPF", "3CPP", "3CPR"]

    return list

def crtable1_enhanced_ind_pruterm():

    list = ["5PTB", "4PTB"]

    return list

def crtable2_enhanced_ind_pruterm():

    list = ["5PMB", "4PMC", "4PMS", "4PMV", "4PML", "4PMB", "4PMG"]

    return list

def crtable3_enhanced_ind_pruterm():

    list = ["5BCP", "CCPB", "CCPA", "CCPD", "CCPE", "CPCC", "CCPH", "CCPF"]

    return list

def crtable4_enhanced_ind_pruterm():

    list = ["CPPD", "CPPG", "CPPE", "CPPF"]

    return list

def product_name_update_list():

    list = ['C5PSM1', 'C5PSM2', 'C5PS1B', 'C5PS2B']

    return list

def crtable_product_name_update_list():

    list = ['5T2B', '5SMB']

    return list

def cnttype_ppt_filter_list():

    list = ['4PP', '4PR', '5PP', '5PR']

    return list

def crtable_ppt_filter_list():

    list = ['4PPB', '4PRB', '5PPB', '5PRB']

    return list

def bft_paybl_y_list():

    list = ['C3CWPR', 'C3CWPN', 'C4CWTR', 'C4CWNR', 'C6CWPC', 'C6CWPD', 'C6CWPH', 'C6CWPR', 'C6CWPU', 'C5CWTN', 'C6CWEZ', 'C6CWNU', 'C6CWTN', 'C4CWTN', 'C4PWPP',
            'C3CWP_', 'C3CWN_', 'C4CWTP', 'C5CWTP']
    
    return list

def sum_assured_prod_list1():

    list = ['C3HSPB', 'C3HSPN', 'C4HSPB', 'C4HSPN', 'C5HSPB']

    return list

def sum_assured_prod_list2():

    list = ['CEP10_', 'CEPD21', 'CEPDN1']

    return list

def spcode_prod_list():

    list = ["C5EHIB", "C5EHIP", "C5EPAC", "C5EPCB", "C5HHI2", "C5HHIP", "C5HIP2", "C5LCP_", "C5LPR_", 
            "C5LS2_", "C5LSP_", "C5MP__", "C5PAC2", "C5PAR_", "C5PARB", "C5PCC2", "C5PICA", "C5PLCG", 
            "C5PLCS", "C5PMYC", "C5PSC_", "C5PVC_", "C5PWC_", "C6PCP_"]
    
    return list

def cnttype_prod_list():

    list = ['1GM', '2MC', '7GM']
    
    return list

def dmtm_spcode_list():

    list = ['C5EHIB', 'C5EHIP', 'C5EPAC', 'C5EPCB', 'C5HHI2', 'C5HHIP', 'C5HIP2', 'C5LCP_', 'C5LPR_', 'C5LS2_', 'C5LSP_', 'C5MP__', 'C5PAC2', 'C5PAR_', 'C5PARB', 'C5PCC2', 'C5PICA',
            'C5PLCG', 'C5PLCS', 'C5PMYC', 'C5PSC_', 'C5PVC_', 'C5PWC_', 'C6PCP_']
    
    return list

def polfee_cnttype_list():

    list = ['1WL', '1WN', '1WP', '2BL', '2DO', '2EA', '2EP', '2JW', '2PI', '2PP', '2WL', '2WN', '2WP', '4BS', '4BT', '4BU', '4BV', '5BT', '5ER', '5PT']

    return list

def elsp_list():

    list = ['ROPS', 'ROPF', 'ROPM']

    return list

def mrta_rop_list():

    list = ['1RP1', '1RP2', '2RP1', '2RP2', '3RP1', '3RP2', '4RP1', '6RP1', '7RP1', '7RP2', '7RP3', '7RP4', '7RP5']

    return list

def mrta_blanksa_list():

    list = ["1GMB", "1G2B", "1G3B", "TM02", "2MCB", "2M2B", "3AMB", "3A2B", "4MCB", "4M2B", "4SPB", "5GMB", 
                            "5SPB", "6MCB", "6M2B", "7GMB", "7G2B", "7MCB", "7MGB", "7M3B"]
    
    return list

def data_cleansing_list():

    list = ['VAL_TYPE', 'PRODUCT_NAME', 'SPCODE', 'POL_NUMBER', 'AGE_AT_ENTRY', 'SEX', 'SMOKER_STAT', 'ENTRY_MONTH',
                    'ENTRY_YEAR', 'POL_TERM_Y', 'BFT_PAYBL_Y', 'PREM_PAYBL_Y', 'PREM_FREQ', 'PREM_MODE', 'ANNUAL_PREM',
                    'SINGLE_PREM', 'SUBSTD_PREM', 'SUM_ASSURED', 'DEATH_BEN', 'DURATIONIF_M', 'INIT_POLS_IF', 'INIT_DECB_IF',
                    'SN_LOAN', 'PLAN_CODE', 'CC_ANN_PREM', 'CC_SUM_ASSD', 'CC_TERM_Y', 'GN_INTRIM_RB', 'GN_REVBON_LY', 'PUPINDICATOR',
                    'INIT_AS_IF', 'INIT_AS_SURD', 'AS_CURR_MTH', 'AS_CURR_YEAR', 'INIT_AGC_IF', 'AGE2_ATENTRY', 'SEX2', 'SMOKER2_STAT',
                    'FUNDTYPE', 'DEFER_PER_Y', 'MORT_INT_PC', 'OCC_CLASS', 'NONPAR_PREM', 'LINK_PREM', 'RIDER_BEN_PP', 'MAT_BEN_PP',
                    'G_MAT_BEN_PP', 'SA_IND', 'IND_PC', 'WAIVED_STAT', 'CI_DURIF_M', 'NO_LS_CLAIM', 'CIS_I', 'RECV_I', 'PREG_I',
                    'BABY_I', 'OVA_I', 'LOST_I', 'MOM_I', 'DEL_I', 'ISSUE_MONTH', 'ISSUE_YEAR', 'MME_PREM', 'SERIES_IND', 
                    'RETIREMENT_AGE', 'YTD_COST_TB', 'WAIV_TYPE', 'COMM_IND', 'COMP_BEN_SA', 'INCSA_PCT', 'HB_SUM_ASSD', 'SURR_FAC_1',
                    'CIC_BEN_PP', 'ACC_SUM_ASSD', 'CC2_SUM_ASSD', 'LS_SUM_ASSD', 'AMR_SA_PP', 'ROP_MAT_PC', 'HC_SUM_ASSD', 'INCR_BEN',
                    'YI_DTH_TPD', 'SB_BEN', 'ADEN_BEN', 'ECA_BEN1', 'ECA_BEN2', 'SB_S_YR', 'TREATY_ID_TREATY1', 'TREATY_ID_TREATY2',
                    'TREATY_ID_TREATY3', 'TREATY_ID_TREATY4', 'TREATY_ID_TREATY5', 'PROD_CD', 'BENEFIT_CODE', 'POL_NO_IFRS17',
                    'BASIC_ENTRY_MONTH', 'BASIC_ENTRY_YEAR']
    
    return list

def summary_list():

    list = ['AGE_AT_ENTRY','SEX','SMOKER_STAT','ENTRY_MONTH','ENTRY_YEAR','POL_TERM_Y','BFT_PAYBL_Y','PREM_PAYBL_Y','PREM_FREQ','PREM_MODE',
            'ANNUAL_PREM','SINGLE_PREM','SUBSTD_PREM','SUM_ASSURED','DEATH_BEN','DURATIONIF_M','INIT_POLS_IF','INIT_DECB_IF','SN_LOAN','PLAN_CODE',
            'CC_ANN_PREM','CC_SUM_ASSD','CC_TERM_Y','GN_INTRIM_RB','GN_REVBON_LY','PUPINDICATOR','INIT_AS_IF','INIT_AS_SURD','AS_CURR_MTH','AS_CURR_YEAR',
            'INIT_AGC_IF','AGE2_ATENTRY','SEX2','SMOKER2_STAT','FUNDTYPE','DEFER_PER_Y','MORT_INT_PC','OCC_CLASS','NONPAR_PREM','LINK_PREM','RIDER_BEN_PP',
            'MAT_BEN_PP','G_MAT_BEN_PP','SA_IND','IND_PC','WAIVED_STAT','CI_DURIF_M','NO_LS_CLAIM','CIS_I','RECV_I','PREG_I','BABY_I','OVA_I','LOST_I','MOM_I',
            'DEL_I','ISSUE_MONTH','ISSUE_YEAR','MME_PREM','SERIES_IND','RETIREMENT_AGE','YTD_COST_TB','WAIV_TYPE','COMM_IND','COMP_BEN_SA','INCSA_PCT','HB_SUM_ASSD',
            'SURR_FAC_1','CIC_BEN_PP','ACC_SUM_ASSD','CC2_SUM_ASSD','LS_SUM_ASSD','AMR_SA_PP','ROP_MAT_PC','HC_SUM_ASSD','INCR_BEN','YI_DTH_TPD','SB_BEN','ADEN_BEN',
            'ECA_BEN1','ECA_BEN2','SB_S_YR','WAIVED_IND','WAIVED_PREM','WAIVED_TERM','RI_CED_ACHL1','RI_CED_ADTL1','RI_CED_ADTL2','RI_CED_ADTL3','RI_CED_ADTL4',
            'RI_CED_CIC1','RI_CED_CIC2','RI_CED_DEATH','RI_CED_COMP_BEN','BASIC_ENTRY_MONTH','BASIC_ENTRY_YEAR','RECORD_MPF']
    
    return list



def summary_par_index_list():

    list = ['AGE_AT_ENTRY','SEX','SMOKER_STAT','ENTRY_MONTH','ENTRY_YEAR','POL_TERM_Y','BFT_PAYBL_Y','PREM_PAYBL_Y','PREM_FREQ','PREM_MODE',
            'ANNUAL_PREM','SINGLE_PREM','SUBSTD_PREM','SUM_ASSURED','DEATH_BEN','DURATIONIF_M','INIT_POLS_IF','INIT_DECB_IF','SN_LOAN','PLAN_CODE',
            'CC_ANN_PREM','CC_SUM_ASSD','CC_TERM_Y','GN_INTRIM_RB','GN_REVBON_LY','PUPINDICATOR','INIT_AS_IF','INIT_AS_SURD','AS_CURR_MTH','AS_CURR_YEAR',
            'INIT_AGC_IF','AGE2_ATENTRY','SEX2','SMOKER2_STAT','FUNDTYPE','DEFER_PER_Y','MORT_INT_PC','OCC_CLASS','NONPAR_PREM','LINK_PREM','RIDER_BEN_PP',
            'MAT_BEN_PP','G_MAT_BEN_PP','SA_IND','IND_PC','WAIVED_STAT','CI_DURIF_M','NO_LS_CLAIM','CIS_I','RECV_I','PREG_I','BABY_I','OVA_I','LOST_I','MOM_I',
            'DEL_I','ISSUE_MONTH','ISSUE_YEAR','MME_PREM','SERIES_IND','RETIREMENT_AGE','YTD_COST_TB','WAIV_TYPE','COMM_IND','COMP_BEN_SA','INCSA_PCT','HB_SUM_ASSD',
            'SURR_FAC_1','CIC_BEN_PP','ACC_SUM_ASSD','CC2_SUM_ASSD','LS_SUM_ASSD','AMR_SA_PP','ROP_MAT_PC','HC_SUM_ASSD','INCR_BEN','YI_DTH_TPD','SB_BEN','ADEN_BEN',
            'ECA_BEN1','ECA_BEN2','SB_S_YR','WAIVED_IND','WAIVED_PREM','WAIVED_TERM','RI_CED_ACHL1','RI_CED_ADTL1','RI_CED_ADTL2','RI_CED_ADTL3','RI_CED_ADTL4',
            'RI_CED_CIC1','RI_CED_CIC2','RI_CED_DEATH','RI_CED_COMP_BEN','BASIC_ENTRY_MONTH','BASIC_ENTRY_YEAR','PAR_COHORT_INDEX','RECORD_MPF']
    
    return list

def as_sort_list():

    list = ['VAL_TYPE','PRODUCT_NAME','SPCODE','POL_NUMBER','AGE_AT_ENTRY','SEX','SMOKER_STAT','ENTRY_MONTH','ENTRY_YEAR','POL_TERM_Y','BFT_PAYBL_Y',
            'PREM_PAYBL_Y','PREM_FREQ','PREM_MODE','ANNUAL_PREM','SINGLE_PREM','SUBSTD_PREM','SUM_ASSURED','DEATH_BEN','DURATIONIF_M','INIT_POLS_IF',
            'INIT_DECB_IF','SN_LOAN','PLAN_CODE','CC_ANN_PREM','CC_SUM_ASSD','CC_TERM_Y','GN_INTRIM_RB','GN_REVBON_LY','PUPINDICATOR','INIT_AS_IF',
            'INIT_AS_SURD','AS_CURR_MTH','AS_CURR_YEAR','INIT_AGC_IF','AGE2_ATENTRY','SEX2','SMOKER2_STAT','FUNDTYPE','DEFER_PER_Y','MORT_INT_PC',
            'OCC_CLASS','NONPAR_PREM','LINK_PREM','RIDER_BEN_PP','MAT_BEN_PP','G_MAT_BEN_PP','SA_IND','IND_PC','WAIVED_STAT','CI_DURIF_M','NO_LS_CLAIM',
            'CIS_I','RECV_I','PREG_I','BABY_I','OVA_I','LOST_I','MOM_I','DEL_I','ISSUE_MONTH','ISSUE_YEAR','MME_PREM','SERIES_IND','RETIREMENT_AGE',
            'YTD_COST_TB','WAIV_TYPE','COMM_IND','COMP_BEN_SA','INCSA_PCT','HB_SUM_ASSD','SURR_FAC_1','CIC_BEN_PP','ACC_SUM_ASSD','CC2_SUM_ASSD',
            'LS_SUM_ASSD','AMR_SA_PP','ROP_MAT_PC','HC_SUM_ASSD','INCR_BEN','YI_DTH_TPD','SB_BEN','ADEN_BEN','ECA_BEN1','ECA_BEN2','SB_S_YR','WAIVED_IND',
            'WAIVED_PREM','WAIVED_TERM','RI_CED_ACHL1','RI_CED_ADTL1','RI_CED_ADTL2','RI_CED_ADTL3','RI_CED_ADTL4','RI_CED_CIC1','RI_CED_CIC2','RI_CED_DEATH',
            'RI_CED_COMP_BEN','TREATY_ID_TREATY1','TREATY_ID_TREATY2','TREATY_ID_TREATY3','TREATY_ID_TREATY4','TREATY_ID_TREATY5','PROD_CD','BENEFIT_CODE',
            'POL_NO_IFRS17','BASIC_ENTRY_MONTH','BASIC_ENTRY_YEAR']
    
    return list

IF = "IF"
ET = "ET"
EX = "EX"
