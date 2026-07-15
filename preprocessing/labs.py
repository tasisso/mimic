import pandas as pd
from utils.utils import load_tbl, get_path
from utils.constants import LAB_LABELS, LAB_MAP

def get_labs(dirs, cohort):

    labs = load_labs(dirs, cohort)
    matched = match_labs(labs, cohort)
    filtered = filter_labs(matched)
    filtered = normalize_label(filtered, LAB_MAP)
    return filtered

def load_labs(dirs, cohort, chunksize=200000):
    '''load and label raw lab events'''
    path = get_path('LABEVENTS.csv.gz', source='hosp', dirs=dirs)
    d_labs = load_tbl('D_LABITEMS.csv.gz', source='hosp', dirs=dirs)
    label_map = d_labs.set_index('itemid')['label'].to_dict()
    hadm_ids = set(cohort['hadm_id'])

    kept_rows = []
    for chunk in pd.read_csv(path, chunksize=chunksize):
        chunk.columns = chunk.columns.str.lower()
        chunk = chunk[chunk['hadm_id'].isin(hadm_ids)]
        if chunk.empty:
            continue
        chunk['label'] = chunk['itemid'].map(label_map)
        if not chunk.empty:
            kept_rows.append(chunk)

    return pd.concat(kept_rows, ignore_index=True)

def match_labs(labs, cohort):
    '''
    Match labs to waveform windows,
    includes a 12 hour buffer before waveform start
    '''
    buffer = pd.Timedelta(hours=12.0)
    admission_labs = labs[['subject_id', 'hadm_id', 'itemid', 'label', 'charttime', 'valuenum', 'valueuom', 'flag']].merge(
        cohort[['subject_id', 'hadm_id', 'record_id', 'start_timestamp', 'end_timestamp']],
        how='inner',
        on=['subject_id', 'hadm_id']
    )
    admission_labs['hadm_id'] = admission_labs['hadm_id'].astype(int)

    admission_labs['charttime'] = pd.to_datetime(admission_labs['charttime'])
    
    #Charted within the waveform window -> match to stay
    stay_labs = admission_labs[
        (admission_labs['charttime'] >= admission_labs['start_timestamp'] - buffer) &
        (admission_labs['charttime'] < admission_labs['end_timestamp'])
    ]
    return stay_labs

def filter_labs(matched_labs, labels=LAB_LABELS):
    '''Filter to cohort to those having ALL labels within icustay '''
    # record_labflags = (matched_labs[matched_labs['label'].isin(labels)]
    #     .assign(present=lambda x: x['valuenum'].notna().astype(int))
    #     .pivot_table(index='record_id', columns='label', values='present', aggfunc='max', fill_value=0)
    #     .reset_index())   
    # records_with_all_labs = list(record_labflags[record_labflags[labels].eq(1).all(axis=1)]['record_id'])

    return matched_labs[matched_labs['label'].isin(labels)]

def normalize_label(df, lab_map):
    df = df.copy()
    df['lab_name'] = df['label'].map(lambda l: lab_map.get(l))
    return df