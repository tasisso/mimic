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
        inputs_cv = format_cv(inputs_cv)
        inputs_cv['dbsource'] = 'carevue'

        inputs_mv = load_tbl('INPUTEVENTS_MV.csv.gz', source='icu', dirs=dirs)
        inputs_mv = format_mv(inputs_mv, mimic)
        inputs_mv['dbsource'] = 'metavision'

        inputs = pd.concat(inputs_cv, inputs_mv)
    elif mimic == 4:
        inputs = load_tbl('INPUTEVENTS.csv.gz', source='icu', dirs=dirs)
        inputs = format_mv(inputs, mimic)
        inputs['dbsource'] = 'metavision'

    return inputs.merge(d_inputs[['itemid', 'label']], how='inner', on='itemid')

def match_inputs(inputs, cohort):
    '''match labs to waveform windows'''
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
        (stay_inputs['start_timestamp'] < stay_inputs['endtime']) &
        (stay_inputs['end_timestamp'] > stay_inputs['starttime'])
    ]

def filter_inputs(inputs, labels=INPUT_LABELS):
    '''filter to labels of interest'''
    inputs = inputs[inputs['label'].isin(labels)]
    return inputs


def format_cv(df):
    '''
    Formats carevue inputs to match metavision
    charttime col -> starttime and endtime cols
        Note: Carevue input documentation lacks bolus information; 
        either missing or absorbed into cummulative titration amounts
    '''
    drips = (df[df['originalroute'] == 'IV Drip']
             .copy()
             .sort_values(['icustay_id', 'itemid', 'charttime'])
             .reset_index(drop=True))
    
    intervals = []

    def new_interval(icustay_id, itemid, starttime, endtime, rate, uom):
        return {
            'icustay_id': icustay_id,
            'itemid': itemid,
            'starttime': starttime,
            'endtime': endtime,
            'rate': rate,
            'rateuom': uom,
            'amount': None,
            'amountuom': None,
            'ordercategorydescription': 'Continuous Med', #matching inputs_mv format
        }

    def reset_state():
        return None, None, None  # rate, uom, starttime

    for (icustay_id, itemid), group in drips.groupby(['icustay_id', 'itemid']):
        group = group.reset_index(drop=True)
        interval_rate, interval_uom, starttime = reset_state()

        for i, row in group.iterrows():
            rate = row['rate']
            is_last = i == len(group) - 1
            is_valid = not pd.isna(rate) and rate > 0.0
            is_stopped = row['stopped'] == 'Stopped'

            if not is_valid:
                if interval_rate is not None:
                    intervals.append(new_interval(icustay_id, itemid, starttime, row['charttime'], interval_rate, interval_uom))
                    interval_rate, interval_uom, starttime = reset_state()
                continue

            if interval_rate is None:
                # start new interval
                interval_rate, interval_uom, starttime = rate, row['rateuom'], row['charttime']

            elif is_stopped or is_last or interval_rate != rate:
                # close current interval
                intervals.append(new_interval(icustay_id, itemid, starttime, row['charttime'], interval_rate, interval_uom))
                # start new interval if rate changed and valid
                if not is_stopped and not is_last:
                    interval_rate, interval_uom, starttime = rate, row['rateuom'], row['charttime']
                else:
                    interval_rate, interval_uom, starttime = reset_state()

    return pd.DataFrame(intervals)

def format_mv(df, mimic):
    if mimic == 3:
        stay_key = 'icustay_id'
    elif mimic == 4:
        stay_key = 'stay_id'
    return df[[stay_key, 'itemid', 'starttime', 'endtime', 'rate', 'rateuom', 'amount', 'amountuom', 'ordercategorydescription']]