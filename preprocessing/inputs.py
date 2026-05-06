import pandas as pd
from utils.utils import load_tbl
from utils.constants import INPUT_LABELS

def get_inputs(dirs, cohort):
    mimic = cohort['mimic'].iloc[0]
    
    inputs = load_inputs(dirs, mimic)
    matched = match_inputs(inputs, cohort)
    return filter_inputs(matched)

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
        inputs_mv = inputs_cv.merge(d_inputs[['itemid', 'label']], 
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
    '''match labs to waveform windows'''
    buffer = pd.Timedelta(hours=2.0)
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
    
    #Overlapping with the waveform window
    return stay_inputs[
        (stay_inputs['start_timestamp'] < stay_inputs['endtime'] + buffer) &
        (stay_inputs['end_timestamp'] > stay_inputs['starttime'])
    ]

def filter_inputs(inputs, labels=INPUT_LABELS):
    '''filter to labels of interest'''
    inputs = inputs[inputs['label'].isin(labels)]
    return inputs


def format_mv(df, mimic):
    if mimic == 3:
        stay_key = 'icustay_id'
    elif mimic == 4:
        stay_key = 'stay_id'
    return df[[stay_key, 'itemid', 'label', 'starttime', 'endtime', 
            'rate', 'rateuom', 'amount', 'amountuom', 'ordercategorydescription']]


def format_cv(df):
    '''
    Formats carevue inputs to match metavision starttime/endtime format.
    Handles two route types:
        - IV Drip: reconstruct continuous intervals from charttimes
        - Intravenous Push: reconstruct intervals from consecutive row amounts
            Note: Requires full inputs_cv for get_elapsed_row0
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
    if len(run) >= 3 and run['elapsed_min'].iloc[1] == run['elapsed_min'].iloc[2]:
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
    intervals  = []
    labels_set = set(INPUT_LABELS)
    all_pushes = (df[df['originalroute'] == 'Intravenous Push']
                  [['icustay_id', 'charttime']]
                  .drop_duplicates())

    pushes = df[
        (df['originalroute'] == 'Intravenous Push') &
        (df['label'].isin(labels_set))
    ].copy()

    for (icustay_id, itemid), group in pushes.groupby(['icustay_id', 'itemid']):
        group['run_id'] = (
            group['originalroute'] != group['originalroute'].shift()
        ).fillna(False).cumsum()

        for run_id, run in group.groupby('run_id'):
            if len(run) < 3:
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
                    'Continuous Med'
                ))

    return intervals