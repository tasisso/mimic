import pandas as pd
from utils.utils import load_tbl, build_dirs
from utils.constants import LAB_LABELS


def load_labs(dirs):
    '''load and label raw lab events'''
    labs = load_tbl('LABEVENTS.csv.gz', source='hosp', dirs=dirs)
    d_labs = load_tbl('D_LABITEMS.csv.gz', source='hosp', dirs=dirs)
    return labs.merge(d_labs[['itemid', 'label']], how='inner', on='itemid')

def match_labs(labs, cohort):
    '''match labs to waveform windows'''
    
    admission_labs = labs.merge(
        cohort[['subject_id', 'hadm_id', 'record_id', 'start_timestamp', 'end_timestamp']],
        how='inner',
        on=['subject_id', 'hadm_id']
    )
    admission_labs['charttime'] = pd.to_datetime(admission_labs['charttime'])
    
    #Charted within the waveform window -> match to stay
    stay_labs = admission_labs[
        (admission_labs['charttime'] >= admission_labs['start_timestamp']) &
        (admission_labs['charttime'] < admission_labs['end_timestamp'])
    ]
    return stay_labs

def filter_labs(matched_labs, labels=LAB_LABELS):
    '''Filter to cohort to those having ALL labels within icustay '''
    record_labflags = (matched_labs[matched_labs['label'].isin(labels)]
        .assign(present=lambda x: x['valuenum'].notna().astype(int))
        .pivot_table(index='record_id', columns='label', values='present', aggfunc='max', fill_value=0)
        .reset_index())   
    records_with_all_labs = record_labflags[record_labflags[labels].eq(1).all(axis=1)]

    return matched_labs.merged(records_with_all_labs,
                               on='record_id',
                               how='inner')

def get_labs(dirs, cohort):

    labs = load_labs(dirs)
    matched = match_labs(labs, cohort)

    return filter_labs(matched)

