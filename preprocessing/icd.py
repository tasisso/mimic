import pandas as pd
from utils.utils import load_tbl, get_path
from utils.constants import LAB_LABELS
import sys
import os
import numpy as np
sys.path.append(os.path.join(os.path.dirname(__file__), '../../extern'))

from extern.icd_gem import ICD_CM_Conversion

def match_icd(icd, cohort):
    '''
    Returns dataframe of all icd codes for all (subject_id, hadm_id) in cohort 
    '''
    admission_icd = icd.merge(
        cohort[['subject_id', 'hadm_id']],
        how='inner',
        on=['subject_id', 'hadm_id']
    )
    
    return admission_icd

def map_icd9_to_10(codes, gem_dir):
    '''
    Convert ICD-9 codes to ICD-10 using GEM mapping files
    
    Args:
        codes: list of ICD-9 codes
        gem_dir: path to gem_files directory
    Returns:
        dict mapping icd9 -> [icd10 codes]
    '''
    converter = ICD_CM_Conversion(
        i9_cm_path=os.path.join(gem_dir, '2018_I9gem.txt'),
        i10_cm_path=os.path.join(gem_dir, '2018_I10gem.txt')
    )
    mapping = converter.icd9_to_10_cm(codes)
    #Mapping values are [code, flag] ndarray pair
    
    return mapping

def filter_icd(icd):
    '''
    Filter Y codes
    Y: "External Causes of Morbidity"
    '''
    mask = icd['icd10_code'].str.startswith('Y')
    return icd[~mask]

def get_icd(config, dirs, cohort):
    mimic = cohort['mimic'].iloc[0]
    gem_dir = config['paths']['gem']
    mimic4_root = config['paths'][f'mimic4_clinical_root']

    # Load and match respective mimic icd
    icd = load_tbl('DIAGNOSES_ICD.csv.gz', source='hosp', dirs=dirs)
    matched = match_icd(icd, cohort)
    
    if mimic == 3:
        codes = matched['icd9_code'].dropna().unique().tolist()
        mapping = map_icd9_to_10(codes, gem_dir)
        matched['icd10_code'] = matched['icd9_code'].map(mapping).apply(lambda x: [pair[0] for pair in x] if isinstance(x, (list, np.ndarray)) else x)
        # print(matched['icd10_code'].apply(type).value_counts())
        # print(matched['icd10_code'].head(10))
        matched['icd_version'] = 9
        matched = matched.rename(columns={'icd9_code': 'vers_code'})
        matched = matched.explode('icd10_code').reset_index(drop=True)
    elif mimic == 4:
        # split on version
        icd9_rows = matched[matched['icd_version'] == 9].copy()
        icd10_rows = matched[matched['icd_version'] == 10].copy()
        # map icd9 rows to icd10
        codes = icd9_rows['icd_code'].dropna().unique().tolist()
        mapping = map_icd9_to_10(codes, gem_dir)
        icd9_rows['icd10_code'] = matched['icd_code'].map(mapping).apply(lambda x: [pair[0] for pair in x] if isinstance(x, (list, np.ndarray)) else x)
        icd9_rows = icd9_rows.explode('icd10_code').reset_index(drop=True)
        # icd10 remains the same
        icd10_rows['icd10_code'] = icd10_rows['icd_code']
        # recombine icd9 converted and icd10
        matched = pd.concat([icd9_rows, icd10_rows], ignore_index=True)
        matched = matched.rename(columns = {'icd_code': 'vers_code'})

    # load mimic4 d_icd for icd10 labels
    d_icd = pd.read_csv(os.path.join(mimic4_root, 
                                       'hosp', 
                                       'd_icd_diagnoses.csv.gz'))
    labeled = matched.merge(d_icd[['icd_code', 'long_title']],
                            how='inner',
                            left_on='icd10_code',
                            right_on='icd_code').drop(columns='icd_code')
    
    return filter_icd(labeled)
