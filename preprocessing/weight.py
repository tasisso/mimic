import pandas as pd 
from utils.constants import WEIGHT_ITEMIDS, WEIGHT_MAX, WEIGHT_MIN, HEIGHT_ITEMIDS
from utils.utils import load_tbl

def get_stay_weight_height(dirs, cohort):
    mimic = cohort['mimic'].iloc[0]
    if mimic == 3:
        stay_key = 'icustay_id'
        chart_events = load_tbl('weight_height_events.csv', source='derived', dirs=dirs)
        inputs = load_tbl('INPUTEVENTS_MV.csv.gz', source='icu', dirs=dirs)
        
    elif mimic == 4:
        stay_key = 'stay_id'
        chart_events = load_tbl('weight_height_events.csv', source='derived', dirs=dirs)
        inputs = load_tbl('inputevents.csv.gz', source='icu', dirs=dirs)

    weights_inputs = get_weights_inputs(cohort, inputs, stay_key)
    weights_charts = get_weights_charts(cohort, chart_events, stay_key)

    # Combine: prioritize inputs, fall back to charts
    stay_weights = (weights_inputs
                    .merge(weights_charts[[stay_key, 'weight_kg']], 
                           on=stay_key, 
                           how='outer', 
                           suffixes=('_inputs', '_charts'))
    )
    stay_weights['weight_kg'] = stay_weights['weight_kg_inputs'].fillna(stay_weights['weight_kg_charts'])
    stay_weights = stay_weights[[stay_key, 'weight_kg']]

    stay_heights = get_heights(cohort, chart_events, stay_key)

    result = cohort.merge(stay_weights, how='left', on=stay_key)
    result = result.merge(stay_heights[[stay_key, 'height_cm']], how='left', on=stay_key)

    return result


def get_heights(icustays, chart_events, stay_key):
    height_events = chart_events[chart_events['itemid'].isin(HEIGHT_ITEMIDS)]

    df = icustays.merge(height_events, on=stay_key, how='inner')
    df = df[df['valuenum'].notna() & (df['valuenum'] > 0)].copy()
    #inch -> cm
    inch_itemids = [920, 1394, 3486, 4187, 226707]
    df.loc[df['itemid'].isin(inch_itemids), 'valuenum'] *= 2.54
    
    df['charttime'] = pd.to_datetime(df['charttime'])
    df['hrs_from_start'] = abs(
        (df['start_timestamp'] - df['charttime']).dt.total_seconds() / 3600
    )

    idx = df.groupby(stay_key)['hrs_from_start'].idxmin()
    result = df.loc[idx][[stay_key, 'itemid', 'valuenum']]
    result = result.rename(columns={'valuenum': 'height_cm'})
    
    return result


def get_weights_charts(icustays, chart_events, stay_key):
    weight_events = chart_events[chart_events['itemid'].isin(WEIGHT_ITEMIDS)]

    #convert lbs. -> kg
    lbs_itemids = [226531]
    weight_events = weight_events.copy()
    weight_events.loc[weight_events['itemid'].isin(lbs_itemids), 'valuenum'] = (
        weight_events.loc[weight_events['itemid'].isin(lbs_itemids), 'valuenum'] * 0.453592
    ).round(1)

    
    df = icustays.merge(weight_events, on=stay_key, how='inner')
    df = df[(df['valuenum'].notna()) & (df['valuenum'] != 1.0) & (df['valuenum'] > 0)].copy()
    df['charttime'] = pd.to_datetime(df['charttime'])
    df['hrs_from_start'] = abs(
        (df['start_timestamp'] - df['charttime']).dt.total_seconds() / 3600
    )

    
    # select closest valid weight to waveform start per icustay
    idx = df.groupby(stay_key)['hrs_from_start'].idxmin()
    result = df.loc[idx][[stay_key, 'itemid', 'valuenum']]
    result = result.rename(columns={'valuenum': 'weight_kg'})
    
    return result

def get_weights_inputs(icustays, inputs, stay_key):
    inputs = inputs[['subject_id', 'hadm_id', stay_key, 'patientweight', 'starttime']].copy()
    inputs = inputs[(inputs['patientweight'].notna()) & (inputs['patientweight'] != 1.0) & (inputs['patientweight'] > 0)]
    stay_weights = inputs.merge(icustays,
                                on=['subject_id', 'hadm_id', stay_key],
                                how='inner')
    stay_weights['starttime'] = pd.to_datetime(stay_weights['starttime'])
    stay_weights['hrs_from_start'] = abs((stay_weights['start_timestamp'] - stay_weights['starttime']).dt.total_seconds() / 3600)
    #stay_weights = stay_weights[stay_weights['patientweight'].between(WEIGHT_MIN, WEIGHT_MAX)]
    idx = stay_weights.groupby(stay_key)['hrs_from_start'].idxmin()
    result = stay_weights.loc[idx][[stay_key, 'patientweight']]
    result = result.rename(columns={'patientweight': 'weight_kg'})

    return result

def get_events(charts_path, stay_key):
    chunks = []
    cols = ['ITEMID', stay_key, 'CHARTTIME', 'VALUE', 'VALUENUM', 'VALUEUOM']
    itemid = 'ITEMID'
    if stay_key == 'stay_id':
        cols = [c.lower() for c in cols]
        itemid = itemid.lower()
    for chunk in pd.read_csv(charts_path, 
                             usecols = cols, 
                             chunksize=500000):
        # filter immediately before concatenating
        chunk = chunk[chunk[itemid].isin(WEIGHT_ITEMIDS+HEIGHT_ITEMIDS)]
        chunks.append(chunk)
    result = pd.concat(chunks, ignore_index=True)

    return result