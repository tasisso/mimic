import pandas as pd 
from utils.constants import WEIGHT_ITEMIDS, WEIGHT_MAX, WEIGHT_MIN
from utils.utils import load_tbl

def get_stay_weight(dirs, cohort):
    mimic = cohort['mimic'].iloc[0]
    if mimic == 3:
        weight_events = load_tbl('weightevents_mimic3.csv', source='derived', dirs=dirs)
        stay_weights = get_weights_from_charts(cohort, weight_events)
        stay_key = 'icustay_id'
    elif mimic == 4:
        inputs = load_tbl('INPUTEVENTS.csv.gz', source='icu', dirs=dirs)
        stay_weights = get_weights_from_inputs(cohort, inputs)
        stay_key = 'stay_id'
    result = cohort.merge(stay_weights, 
                          how='left', 
                          on=stay_key)
    return result

def get_weightevents(charts_path):
    #MIMIC-III only
    chunks = []
    for chunk in pd.read_csv(charts_path, 
                             usecols = ['ITEMID', 'ICUSTAY_ID', 'CHARTTIME', 'VALUE', 'VALUENUM', 'VALUEUOM'], 
                             chunksize=500000):
        # filter immediately before concatenating
        chunk = chunk[chunk['ITEMID'].isin(WEIGHT_ITEMIDS)]
        chunks.append(chunk)
    weight_events = pd.concat(chunks, ignore_index=True)

    return weight_events

def get_weights_from_charts(icustays, weight_events):
    df = icustays.merge(weight_events, on='icustay_id', how='inner')

    df['charttime'] = pd.to_datetime(df['charttime'])
    df['hrs_from_start'] = abs(
        (df['start_timestamp'] - df['charttime']).dt.total_seconds() / 3600
    )
    
    # filter valid weights and convert lbs to kg
    df = df[df['valuenum'].notna()].copy()
    df = df[df['label'].isin(['Previous WeightF', 'Daily Weight', 'Admission Weight (Kg)',
       'Admission Weight (lbs.)', 'Previous Weight'
       ])]
    df.loc[df['label'] == 'Admission Weight (lbs.)', 'valuenum'] = (
        df.loc[df['label'] == 'Admission Weight (lbs.)', 'valuenum'] * 0.453592
    )
    df = df[df['valuenum'].between(WEIGHT_MIN, WEIGHT_MAX)]
    
    # select closest valid weight to waveform start per icustay
    idx = df.groupby('icustay_id')['hrs_from_start'].idxmin()
    result = df.loc[idx][['icustay_id', 'valuenum']]
    result = result.rename(columns={'valuenum': 'weight_kg'})
    
    return result

def get_weights_from_inputs(icustays, inputs):
    inputs = inputs[['subject_id', 'hadm_id', 'stay_id', 'patientweight', 'starttime']].drop_duplicates(subset=['patientweight'])
    stay_weights = inputs.merge(icustays,
                                on=['subject_id', 'hadm_id', 'stay_id'],
                                how='inner')
    
    stay_weights['starttime'] = pd.to_datetime(stay_weights['starttime'])
    stay_weights['hrs_from_start'] = abs((stay_weights['start_timestamp'] - stay_weights['starttime']).dt.total_seconds() / 3600)
    stay_weights = stay_weights[stay_weights['patientweight'].between(WEIGHT_MIN, WEIGHT_MAX)]
    idx = stay_weights.groupby('stay_id')['hrs_from_start'].idxmin()
    result = stay_weights.loc[idx][['stay_id', 'patientweight']]
    result = result.rename(columns={'patientweight': 'weight_kg'})

    # weight_per_stay = (
    #     stay_weights
    #     .sort_values('patientweight', ascending=False)
    #     .groupby('stay_id')
    #     .first()
    #     .reset_index()
    # )
    return result
    