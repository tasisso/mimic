import pandas as pd
from utils.utils import load_tbl, build_dirs
from utils.constants import LAB_LABELS
import sys
import os
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
        i9_cm_path=os.path.join(gem_dir, '2018_I9_cm_gem.txt'),
        i10_cm_path=os.path.join(gem_dir, '2018_I10_cm_gem.txt')
    )
    mapping = converter.icd9_to_10_cm(codes)
    
    return mapping

def filter_icd(icd, labels=LAB_LABELS):
    '''filter to labels of interest'''
    return icd[icd['label'].isin(labels)]

def get_icd(config, dirs,  cohort):
    mimic = cohort['mimic'].iloc[0]
    gem_dir = config['paths']['gem']
    mimic4_root = config['paths'][f'mimic4_clinical_root']

    # Load and match respective mimic icd
    icd = load_tbl('DIAGNOSES_ICD.csv.gz', source='hosp', dirs=dirs)
    matched = match_icd(icd, cohort)
    
    if mimic == 3:
        codes = matched['icd9_code'].dropna().unique().tolist()
        matched['icd10_codes'] = matched['icd9_code'].map(mapping)
        matched = matched.explode('icd10_code').reset_index(drop=True)
    elif mimic == 4:
        # split on version
        icd9_rows = matched[matched['icd_version'] == 9].copy()
        icd10_rows = matched[matched['icd_version'] == 10].copy()
        # map icd9 rows to icd10
        codes = icd9_rows['icd_code'].dropna().unique().tolist()
        mapping = map_icd9_to_10(codes, gem_dir)
        icd9_rows['icd10_code'] = icd9_rows['icd_code'].map(mapping)
        icd9_rows = icd9_rows.explode('icd10_code').reset_index(drop=True)
        # icd10 remains the same
        icd10_rows['icd10_code'] = icd10_rows['icd_code']
        # recombine icd9 converted and icd10
        matched = pd.concat([icd9_rows, icd10_rows], ignore_index=True)

    # load mimic4 d_icd for icd10 labels
    d_icd = pd.read_csv(os.path.join(mimic4_root, 
                                       'hosp', 
                                       'd_icd_diagnoses.csv.gz'))
    labeled = matched.merge(d_icd[['icd_code', 'long_title']],
                            how='inner',
                            left_on='icd10_code',
                            right_on='icd_code')
    
    return filter_icd(labeled)
