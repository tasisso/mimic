import pandas as pd
import numpy as np
import ast
from utils.utils import load_tbl
from utils.constants import INPUT_LABELS, MED_MAP, DOSE_DEPENDENT

def get_inputs(dirs, cohort):
    mimic = cohort['mimic'].iloc[0]
    
    inputs = load_inputs(dirs, mimic)
    matched = match_inputs(inputs, cohort)
    filtered = filter_inputs(matched)
    #add normalized label and label categories (list)
    filtered = normalize_label(filtered, MED_MAP)
    filtered['ratenorm'] = normalize_rate(filtered)
    filtered['resolved_categories'] = filtered.apply(get_dose_categories, axis=1)
    filtered['categories_str'] = filtered['resolved_categories'].apply('|'.join)
    return filtered


def load_inputs(dirs, mimic):
    '''load and label raw lab events'''
    d_inputs = load_tbl('D_ITEMS.csv.gz', source='icu', dirs=dirs)
    if mimic == 3:
        inputs_cv = load_tbl('INPUTEVENTS_CV.csv.gz', source='icu', dirs=dirs)
        inputs_cv = inputs_cv.merge(d_inputs[['itemid', 'label']], 
                                    how='inner', 
                                    on='itemid')
        inputs_cv = format_cv(inputs_cv)
        inputs_cv['dbsource'] = 'carevue'

        inputs_mv = load_tbl('INPUTEVENTS_MV.csv.gz', source='icu', dirs=dirs)
        inputs_mv = inputs_mv.merge(d_inputs[['itemid', 'label']], 
                                    how='inner', 
                                    on='itemid')
        inputs_mv = format_mv(inputs_mv, mimic)
        inputs_mv['dbsource'] = 'metavision'

        inputs = pd.concat([inputs_cv, inputs_mv])
    elif mimic == 4:
        inputs = load_tbl('INPUTEVENTS.csv.gz', source='icu', dirs=dirs)
        inputs = inputs.merge(d_inputs[['itemid', 'label']], 
                              how='inner', 
                              on='itemid')
        inputs = format_mv(inputs, mimic)
        inputs['dbsource'] = 'metavision'

    return inputs

def match_inputs(inputs, cohort):
    '''
    Match inputs to waveform window
    '''
    #buffer = pd.Timedelta(hours=2.0)
    mimic = cohort['mimic'].iloc[0]
    if mimic == 3:
        merge_key = 'icustay_id'
    elif mimic == 4:
        merge_key = 'stay_id'
    stay_inputs = inputs.merge(
        cohort[[merge_key, 'record_id', 'start_timestamp', 'end_timestamp', 'weight_kg']],
        how='inner',
        on=merge_key
    )
    stay_inputs['starttime'] = pd.to_datetime(stay_inputs['starttime'])
    stay_inputs['endtime'] = pd.to_datetime(stay_inputs['endtime'])
    
    #Overlapping with the waveform window
    return stay_inputs[
        (stay_inputs['start_timestamp'] <= stay_inputs['endtime']) &
        (stay_inputs['end_timestamp'] > stay_inputs['starttime'])
    ]

def filter_inputs(inputs, labels=INPUT_LABELS):
    '''filter to labels of interest'''
    inputs = inputs[inputs['label'].isin(labels)]
    return inputs[
        ((inputs['rate'].notna()) & (inputs['rate'] > 0)) |
        ((inputs['amount'].notna()) & (inputs['amount'] > 0))
    ]


def format_mv(df, mimic):
    if mimic == 3:
        stay_key = 'icustay_id'
    elif mimic == 4:
        stay_key = 'stay_id'
    return df[[stay_key, 'itemid', 'label', 'starttime', 'endtime', 
            'rate', 'rateuom', 'amount', 'amountuom', 'ordercategorydescription', 'statusdescription']]


def format_cv(df):
    '''
    Formats carevue inputs to match metavision starttime/endtime format.
    Handles two route types:
        - IV Drip: reconstruct continuous intervals from charttimes
        - Intravenous Push: reconstruct intervals from consecutive row amounts
            Note: Requires unfiltered inputs_cv for get_elapsed_row0
    '''
    df = preprocess_cv(df)
    intervals = []
    intervals.extend(drip_intervals(df))
    intervals.extend(push_intervals(df))
    return pd.DataFrame(intervals)


def preprocess_cv(df):
    df = (df.drop_duplicates(subset=['icustay_id', 'itemid', 'charttime', 
                                   'storetime', 'cgid', 'amount', 'rate'])
          .sort_values(['icustay_id', 'itemid', 'charttime'])
          .copy())
    df['charttime'] = pd.to_datetime(df['charttime'])
    return df


def new_interval(icustay_id, itemid, label, starttime, endtime, 
                  rate, uom, amount, amountuom, category):
    return {
        'icustay_id': icustay_id,
        'itemid': itemid,
        'label': label,
        'starttime': starttime,
        'endtime': endtime,
        'rate': rate,
        'rateuom': uom,
        'amount': amount,
        'amountuom': amountuom,
        'ordercategorydescription': category,
    }


def drip_intervals(df):
    intervals = []
    drips = df[df['originalroute'] == 'IV Drip'].copy()

    for (icustay_id, itemid), group in drips.groupby(['icustay_id', 'itemid']):
        group = group.reset_index(drop=True)
        interval_rate, interval_uom, starttime = None, None, None

        for i, row in group.iterrows():
            rate       = row['rate']
            is_last    = i == len(group) - 1
            is_valid   = pd.notna(rate) and rate > 0.0
            is_stopped = row['stopped'] == 'Stopped'

            if not is_valid:
                if interval_rate is not None:
                    intervals.append(new_interval(
                        icustay_id, itemid, row['label'],
                        starttime, row['charttime'],
                        interval_rate, interval_uom,
                        None, None, 'Continuous Med'
                    ))
                    interval_rate, interval_uom, starttime = None, None, None
                continue

            if interval_rate is None:
                interval_rate, interval_uom, starttime = rate, row['rateuom'], row['charttime']

            elif is_stopped or is_last or interval_rate != rate:
                intervals.append(new_interval(
                    icustay_id, itemid, row['label'],
                    starttime, row['charttime'],
                    interval_rate, interval_uom,
                    None, None, 'Continuous Med'
                ))
                if not is_stopped and not is_last:
                    interval_rate, interval_uom, starttime = rate, row['rateuom'], row['charttime']
                else:
                    interval_rate, interval_uom, starttime = None, None, None

    return intervals


def get_elapsed_row0(run, icustay_id, all_pushes):
    """Determine elapsed time for first row of a push run."""
    first_charttime = run['charttime'].iloc[0]

    # 1) Consistent interval in rest of run —> inherit
    if run['elapsed_min'].iloc[1] == run['elapsed_min'].iloc[2]:
        return run['elapsed_min'].iloc[1]

    # 2) Prior push charttime available
    prior = all_pushes[
        (all_pushes['icustay_id'] == icustay_id) &
        (all_pushes['charttime'] < first_charttime)
    ]['charttime']
    if not prior.empty:
        return (first_charttime - prior.max()).total_seconds() / 60

    # 3) Fallback —> assume hourly
    return 60.0


def compute_push_run_rates(run, icustay_id, all_pushes):
    """Add elapsed_min, rate, rateuom columns to a qualifying push run."""
    run = run.copy()
    run['elapsed_min'] = run['charttime'].diff().dt.total_seconds() / 60
    run.loc[run.index[0], 'elapsed_min'] = get_elapsed_row0(run, icustay_id, all_pushes)
    run['rate']    = run['amount'] / run['elapsed_min']
    run['rateuom'] = run['amountuom'] + '/min'
    return run


def push_intervals(df):
    df = df.sort_values(['icustay_id', 'itemid', 'charttime']).copy().drop_duplicates(subset=[
    'icustay_id', 'itemid', 'charttime', 'storetime', 
    'cgid', 'amount', 'rate'])
    df['charttime'] = pd.to_datetime(df['charttime'])
    
    # Build lookup of previous push charttime across all labels per icustay
    all_pushes = df[df['originalroute'] == 'Intravenous Push'][['icustay_id', 'charttime']].drop_duplicates()
    
    intervals = []
    for (icustay_id, itemid), group in df.groupby(['icustay_id', 'itemid']):
        if not group['label'].isin(INPUT_LABELS).any():
            continue
        group['run_id'] = (
            group['originalroute'] != group['originalroute'].shift()
        ).fillna(False).cumsum()

        for run_id, run in group.groupby('run_id'):
            if run['originalroute'].iloc[0] != 'Intravenous Push' or len(run) < 3:
                continue
            run = run[run['amount'].fillna(0) > 0]
            if len(run) < 3:
                continue

            run = compute_push_run_rates(run, icustay_id, all_pushes)

            for _, row in run.iterrows():
                if pd.isna(row['elapsed_min']) or row['elapsed_min'] <= 0:
                    continue
                intervals.append(new_interval(
                    icustay_id, itemid, row['label'],
                    row['charttime'] - pd.Timedelta(minutes=row['elapsed_min']),
                    row['charttime'],
                    row['rate'], row['rateuom'],
                    row['amount'], row['amountuom'],
                    'Continuous IV'
                ))

    return intervals

def normalize_rate(df):
    conversions = {
        'mcg/kg/min': lambda r, weight: r,
        'mcgkgmin':   lambda r, weight: r,
        'mcg/kg/hour': lambda r, weight: r / 60,
        'mcgkghr':    lambda r, weight: r / 60,
        'mcgmin':     lambda r, weight: r / weight,
        'mcg/min':    lambda r, weight: r / weight,
        'mcg/hour':   lambda r, weight: r / weight / 60,
        'mcghr':      lambda r, weight: r / weight / 60,
        'ng/kg/min':  lambda r, weight: r / 1000,

        'mg/kg/hour': lambda r, weight: r * 1000 / 60,
        'mgkghr':     lambda r, weight: r * 1000 / 60,
        'mg/hour':    lambda r, weight: r * 1000 / weight / 60,
        'mghr':       lambda r, weight: r * 1000 / weight / 60,
        'mg/min':     lambda r, weight: r * 1000 / weight,
        'mgmin':      lambda r, weight: r * 1000 / weight,

        'gm/min':     lambda r, weight: r * 1e6 / weight,
        'grams/min':  lambda r, weight: r * 1e6 / weight,
        'grams/hour': lambda r, weight: r * 1e6 / weight / 60,

        #Units rates not weight-normalized
        'units/hour': lambda r, weight: r / 60,
        'Uhr':        lambda r, weight: r / 60,
        'units/min':  lambda r, weight: r,
        'Umin':       lambda r, weight: r,

        # ml/min — Fentanyl Base (MIMIC-III)
        'ml/min': lambda r, weight: r * 50 / weight # mcg/min, assuming 50 mcg/ml concentration
    }
    result = pd.Series(np.nan, index=df.index, dtype=np.float64)
    for uom, fn in conversions.items():
        mask = (df['rateuom'] == uom) & df['rate'].notna() & df['weight_kg'].notna()
        result[mask] = fn(df.loc[mask, 'rate'], df.loc[mask, 'weight_kg'])

    return result.astype(np.float32)

def normalize_label(df, medication_map):
    df = df.copy()
    df['med_name'] = df['label'].map(lambda l: medication_map.get(l, (None, None))[0])
    df['categories'] = df['label'].map(lambda l: medication_map.get(l, (None, None))[1])
    return df

def get_dose_categories(row):
    if row['med_name'] not in DOSE_DEPENDENT:
        return row['categories']  # fixed categories, unchanged
    if pd.isna(row['ratenorm']):
        return ['vasoactive'] #weight missing -> fallback to vasoactive 
    ranges = DOSE_DEPENDENT[row['med_name']]
    for low, high, cats in ranges:
        if low <= row['ratenorm'] < high:
            return cats
    return ['vasoactive'] #dose fell outside of defined range -> fallback to vasoactive