import pandas as pd
from utils.constants import FINAL_SIGNALS, MIN_SIGNAL_HRS

def match_icustays(meta, icustays):
    mimic = meta['mimic'].iloc[0]
    if mimic == 4:
        merge_keys = ['subject_id', 'hadm_id'] #hadm in meta extracted from Physionet 
        stay_key = 'stay_id'
        icustays['dbsource'] = 'metavision' #add dbsource column -> mimic4 on metavision
    elif mimic == 3:
        stay_key = 'icustay_id'
        merge_keys = ['subject_id']
    #Merge 
    df = meta.merge(icustays[['subject_id', 'hadm_id', stay_key, 'first_careunit', 'last_careunit', 'intime', 'outtime', 'los', 'dbsource']], 
                       how='inner', 
                       on=merge_keys).reset_index(drop=True)
    #Timestamps
    df['start_timestamp'] = pd.to_datetime(df['start_timestamp'])
    df['end_timestamp'] = pd.to_datetime(df['end_timestamp'])
    df['intime'] = pd.to_datetime(df['intime'])
    df['outtime'] = pd.to_datetime(df['outtime'])
    #Select waveform recordings that have any overlap with with icustays 
    df = df[
        (df['start_timestamp'] < df['outtime']) &
        (df['end_timestamp'] > df['intime'])
        ].reset_index(drop=True)
    #Calculate overlap
    df['hrs_overlap'] = (df['end_timestamp'] - df['intime']).dt.total_seconds() / 3600
    
    return df

def filter_signal(cohort, signals=FINAL_SIGNALS, min_length = MIN_SIGNAL_HRS):
    mask = pd.Series(True, index=cohort.index)
    for signal in signals:
        mask = mask & (cohort[f'{signal}_hrs'] >= min_length)

    return cohort[mask]
 

