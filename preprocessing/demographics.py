import pandas as pd 
import numpy as np
from utils.constants import WEIGHT_ITEMIDS, WEIGHT_MIN, WEIGHT_MAX

def get_demographics(cohort, admissions, patients):
    mimic = cohort['mimic'].iloc[0]
    if mimic == 3:
        cohort = cohort.merge(patients[['subject_id', 'dob', 'gender']], 
                                how='inner', 
                                on='subject_id')
        cohort = cohort.merge(admissions[['subject_id', 'hadm_id', 'admittime', 'dischtime', 'ethnicity']], 
                                how='inner', 
                                on=['subject_id','hadm_id'])
    if mimic == 4:
        cohort = cohort.merge(patients[['subject_id', 'anchor_age', 'anchor_year', 'gender']], 
                                how='inner', 
                                on='subject_id')
        cohort = cohort.merge(admissions[['subject_id', 'hadm_id', 'admittime', 'dischtime', 'race']], 
                                how='inner', 
                                on=['subject_id','hadm_id']).rename(columns={'race': 'ethnicity'})
        
    result = calculate_age(mimic, cohort)
    result['ethnicity_group'] = result['ethnicity'].apply(group_ethnicity)

    return result


def calculate_age(mimic, df):
    '''
    Calculate age at time of admission.
    '''
    if mimic == 3:
        df['age'] = (pd.to_datetime(df['admittime']).dt.year 
                    - pd.to_datetime(df['dob']).dt.year).astype(int)
    if mimic == 4:
        if mimic == 4:
            df['age'] = (df['anchor_age'] +
                    pd.to_datetime(df['admittime']).dt.year - df['anchor_year']).astype(int)
    df['age_group'] = pd.cut(df['age'], 
                             bins=[0, 17, 25, 35, 45, 55, 65, 75, 85, np.inf], 
                             labels=['<18', '18-25', '26-35', '36-45', '46-55', '56-65', '66-75', '76-85', '85>']
                             )
    return df


def group_ethnicity(eth):
    eth = str(eth).upper()
    if 'WHITE' in eth: return 'WHITE'
    elif 'BLACK' in eth: return 'BLACK/AFRICAN AMERICAN'
    elif 'HISPANIC' in eth: return 'HISPANIC/LATINO'
    elif 'ASIAN' in eth: return 'ASIAN'
    elif 'UNKNOWN' in eth or 'UNABLE' in eth or 'DECLINED' in eth: return 'UNKNOWN'
    else: return 'OTHER'

