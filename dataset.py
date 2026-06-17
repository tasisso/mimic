from dataset.waveforms import get_record_meta, stream_waveform_chunks, build_path
from dataset.h5_writer import H5ChunkWriter
from dataset.ehr import ehrExtractor
from utils.utils import load_config
from utils.constants import TARGET_SIGNALS, LAB_MAP, INPUT_FEATURES, MED_CATEGORIES, COMMON_CODES, BOLUS_FEATURES, TOP_20_LABS
from torch.utils.data import IterableDataset, DataLoader
from tqdm import tqdm
import pandas as pd
import numpy as np
import torch
import h5py
import os 

# def build_path(mimic, subject_id, record_id, base_path) -> tuple[str, str]:
#     """Returns (master_path, record_dir) for a given subject + record."""
#     sub_group  = f"p{str(subject_id)[:3]}"
#     subject_dir = f"p{subject_id}"
#     record_dir  = f"{base_path}/{sub_group}/{subject_dir}/{record_id}"
#     master_path = f"{record_dir}/{record_id}"
#     return master_path, record_dir

def append_index_row(csv_path, row_dict):
    row_df = pd.DataFrame([row_dict])
    if os.path.exists(csv_path):
        row_df.to_csv(csv_path, mode='a', header=False, index=False)
    else:
        row_df.to_csv(csv_path, mode='w', header=True, index=False)


def to_h5(mimic, config) -> None:
    cohort = pd.read_csv(config['paths'][f'mimic{mimic}']['cohort'])
    buffer = pd.Timedelta(hours=config['lookback_buffer'])

    extractor = ehrExtractor(
        inputs=pd.read_csv(config['paths'][f'mimic{mimic}']['inputs']),
        labs=pd.read_csv(config['paths'][f'mimic{mimic}']['labs']),
        codes=pd.read_csv(config['paths'][f'mimic{mimic}']['icd']),
    )
    #H5 Meta
    meta_path      = f"{config['paths'][f'mimic{mimic}']['output_dir']}/metadata.csv"
    meds_labs_path = f"{config['paths'][f'mimic{mimic}']['output_dir']}/meds_labs_index.csv"
    codes_path     = f"{config['paths'][f'mimic{mimic}']['output_dir']}/icd_codes_index.csv"

    # load already processed ids at startup
    processed_ids = set()
    if os.path.exists(meta_path):
        processed_ids = set(pd.read_csv(meta_path)['record_id'])

    #Static parameters
    target_fs = config['signals']['target_fs']
    chunk_duration = config['signals']['chunk_duration']
    chunk_size = int(chunk_duration * target_fs)
    with tqdm(total=cohort['record_hrs'].sum(), unit='hrs', unit_scale=True, dynamic_ncols=True, leave=True) as pbar:
        for _, row in cohort.iterrows():
            subject_id = row['subject_id']
            hadm_id = row['hadm_id']
            stay_id = row['stay_id']
            record_id = row['record_id']
            weight_kg = row['weight_kg']
            pbar.set_postfix({'record': record_id, 'subject': subject_id})
            icu_meta = {
                'age': row['age'],
                'age_group': row['age_group'],
                'ethnicity': row['ethnicity_group'],
                'gender': row['gender'],
                'weight_kg': weight_kg,
                'los': row['los'],
                'dbsource': row['dbsource'],
                'mimic': mimic
            }
            if record_id in processed_ids:
                pbar.update(row['record_hrs'])
                print(f'Skipping: Record {record_id} - Processed')
                continue

            master_paths, record_dir = build_path(
                mimic, subject_id, record_id, config['paths'][f'mimic{mimic}']['waveforms_root']
            )
            for master_path in master_paths:
                record_meta = get_record_meta(master_path, record_dir, row['fs'], chunk_duration)
                if record_meta is None:
                    print(f"No target signals for record {record_id}, skipping.")
                    pbar.update(row['record_hrs'])
                    continue

                signal_map = record_meta['signal_map']
                total_chunks = record_meta['total_chunks']

                med_feats, med_cat_feats = extractor.stay_med_feats.get(stay_id, (set(), set()))
                lab_feats = extractor.stay_lab_feats.get((subject_id, hadm_id), set())

                has_meds = 0 if len(med_feats) == 0 else 1
                has_labs = 0 if len(lab_feats) == 0 else 1

                if not has_meds and not has_labs:
                    print(f'Skipping Record {record_id} -  No meds or labs features')
                    continue

                with H5ChunkWriter(
                    output_dir=config['paths'][f'mimic{mimic}']['output_dir'],
                    subject_id=subject_id,
                    hadm_id=hadm_id,
                    stay_id=stay_id,
                    record_id=record_id,
                    chunk_size=chunk_size,
                    total_chunks=total_chunks,
                    record_signals=record_meta['record_signals'],
                    med_cat_feats=med_cat_feats,
                    med_feats=med_feats,
                    lab_feats=lab_feats,
                ) as writer:
                    # Static features as h5 attributes
                    codes = extractor.get_codes(subject_id, hadm_id)
                    writer.write_static(codes=codes)

                    #Yield one waveform chunk at a time and extract ehr features 
                    for chunk_id, chunk_starttime, chunk_data in stream_waveform_chunks(
                        master_header=record_meta['master_header'],
                        record_dir=record_dir,
                        source_fs=row['fs'],
                        signal_map=signal_map,
                        chunk_size=chunk_size,
                        chunk_timestamps=record_meta['chunk_timestamps']
                    ):
                        lookback = pd.Timestamp(chunk_starttime) - buffer if chunk_id == 0 else None
                        ehr_dict = extractor.get_features(
                        subject_id=subject_id,
                        hadm_id=hadm_id,
                        stay_id=stay_id,
                        patientweight=weight_kg,
                        chunk_starttime=chunk_starttime,
                        lookback=lookback
                    )
                        writer.write_chunk(chunk_id, chunk_starttime, chunk_data, signal_map, ehr_dict)
                        #print(f"[{record_id}] chunk {chunk_id}/{total_chunks}")
                print(f'Done with {record_id}')
                pbar.update(row['record_hrs'])
                append_index_row(meta_path, {
                    'filepath':   writer.filepath,
                    'subject_id': int(subject_id),
                    'hadm_id':    int(hadm_id),
                    'stay_id':    int(stay_id),
                    'record_id':  int(record_id),
                    'n_chunks':   int(total_chunks),
                    'has_meds':   has_meds,
                    'has_labs':   has_labs,
                    **icu_meta,
                    **{sig: int(sig in signal_map) for sig in TARGET_SIGNALS},
                })
                append_index_row(meds_labs_path, {
                    'record_id': int(record_id),
                    **{cat: int(cat in med_cat_feats) for cat in MED_CATEGORIES},
                    **{med: int(med in med_feats)     for med in INPUT_FEATURES},
                    **{lab: int(lab in lab_feats)     for lab in LAB_MAP.values()},
                })
                append_index_row(codes_path, {
                    'record_id': int(record_id),
                    **{code: int(code in codes) for code in COMMON_CODES},
                })

    return meta_path

class ICUDataset(IterableDataset):
    def __init__(self, 
                 h5_dir,
                 meta_index,
                 med_lab_index,
                 code_index,
                 signals=['PLETH', 'II'], 
                 med_labels=[],
                 med_categories=[],
                 lab_labels=[],
                 code_labels=[],
                 include_demographics=False,
                 require_weight=False,
                 require_all=False):
        self.h5_dir = h5_dir
        self.signals = signals
        self.lab_labels = lab_labels
        self.med_labels = med_labels
        self.code_labels = code_labels

        self.demographic_cols = ['age_group', 'ethnicity', 'gender', 'weight_kg'] if include_demographics else []
        self.float_features = lab_labels + [f'{lab_label}_abnorm' for lab_label in lab_labels] + [f'{lab_label}_stale' for lab_label in lab_labels] + [f'{med_label}_ratenorm' for med_label in med_labels] + ['norepi_eq'] 
        self.int_features = code_labels  + [f'{med_label}_bolus' for med_label in med_labels if med_label in BOLUS_FEATURES] + [f'{med_category}_on' for med_category in med_categories]

        # filter subjects upfront based on required features
        mask = pd.Series(True, index=meta_index.index)

        # must have all requested signals
        for signal in signals:
            if signal in meta_index.columns:
                mask &= meta_index[signal] == 1

        if require_weight:
            mask &= meta_index['weight'].notna()

        valid_ids = meta_index[mask]['record_id']

        # filter med/lab index to valid subjects that have requested features
        if lab_labels or med_labels or med_categories:
            feature_cols = ['record_id'] + lab_labels + med_labels + med_categories
            available_cols = [c for c in feature_cols if c in med_lab_index.columns]
            self.feature_index = med_lab_index[
                med_lab_index['record_id'].isin(valid_ids)
            ][available_cols]
            present_features = [c for c in lab_labels + med_labels + med_categories if c in self.feature_index.columns]
            if require_all:
                feature_mask = self.feature_index[present_features].all(axis=1)
            else:
                feature_mask = self.feature_index[present_features].any(axis=1)

            valid_ids = self.feature_index[feature_mask]['record_id']
        
        if code_labels:
            present_codes = [c for c in code_labels if c in code_index.columns]
            self.code_index = code_index[
                code_index['record_id'].isin(valid_ids)
            ][['record_id'] + present_codes]

            if require_all:
                code_mask = self.code_index[present_codes].all(axis=1)
            else:
                code_mask = self.code_index[present_codes].any(axis=1)

            valid_ids = valid_ids[valid_ids.isin(self.code_index[code_mask]['record_id'])]

            self.code_array = self.code_index.set_index('record_id').reindex(
                columns=code_labels, fill_value=-1
            ).values.astype(np.int8)
            self.code_index_lookup = {rid: i for i, rid in enumerate(self.code_index['record_id'])}

        self.valid_meta = meta_index[meta_index['record_id'].isin(valid_ids)].reset_index(drop=True)

        if include_demographics:
            demo_df = pd.get_dummies(
                self.valid_meta[['weight_kg', 'gender', 'age_group', 'ethnicity']],
                columns=['gender', 'age_group', 'ethnicity'],
                dummy_na=True
            ).astype(np.float32)

            self.demo_array = demo_df.values.astype(np.float32)
            self.demo_index = {rid: i for i, rid in enumerate(self.valid_meta['record_id'])}
            self.demographic_cols = demo_df.columns.tolist()
        else:
            self.demo_array = None
            self.demographic_cols = []

    
    def __iter__(self):
        for j, row in self.valid_meta.iterrows():
            subject_id = row['subject_id']
            record_id = row['record_id']
            h5_path = f'{self.h5_dir}/{subject_id}_{record_id}.h5'

            with h5py.File(h5_path, 'r') as f:
                n_chunks = row['n_chunks']
                timestamps = f['timestamps'][:]

                block_size = 512  # tune based on memory
                prev_row = None
                for block_start in range(0, n_chunks, block_size):
                    block_end = min(block_start + block_size, n_chunks)

                    ehr_block = f['ehr'][block_start:block_end] if (self.lab_labels or self.med_labels) else None
                    ehr_df = pd.DataFrame(ehr_block)

                    wave_block = {
                        sig: torch.tensor(f['waveforms'][sig][block_start:block_end], dtype=torch.float32)
                        for sig in self.signals if sig in f['waveforms']
                    }

                    # forward fill and reindex block
                    if ehr_block is not None:
                        if prev_row is not None:
                            ehr_df = pd.concat([prev_row, ehr_df], ignore_index=True)
                            ehr_df = self._forward_fill(ehr_df, timestamps[block_start-1:block_end])
                            ehr_df = ehr_df.iloc[1:]  # drop the prepended row
                        else:
                            ehr_df = self._forward_fill(ehr_df, timestamps[block_start:block_end])
    
                        prev_row = pd.DataFrame(f['ehr'][block_end-1:block_end])

                        float_array = ehr_df.reindex(columns=self.float_features, fill_value=np.nan).values.astype(np.float32)
                        int_array = ehr_df.reindex(columns=self.int_features, fill_value=-1).values.astype(np.int32)

                    for i in range(block_end - block_start):
                        sample = {
                            'record_id': record_id,
                            'chunk_id': block_start + i,
                            'waves': {sig: wave_block[sig][i] for sig in wave_block},
                        }
                        if ehr_block is not None:
                            sample['ehr_float'] = torch.tensor(float_array[i], dtype=torch.float32)
                            sample['ehr_int'] = torch.tensor(int_array[i], dtype=torch.int32)
                        if self.code_labels:
                            sample['codes'] = torch.tensor(
                                self.code_array[self.code_index_lookup[record_id]],
                                dtype=torch.int8
                            )
                        if self.demographic_cols:
                            sample['demographics'] = torch.tensor(self.demo_array[j], dtype=torch.float32)
                        yield sample

    def _forward_fill(self, ehr_df, timestamps):
        #Forward fill lab values and abnormality flag, calculate staleness in hours
        present_labs = [lab for lab in self.lab_labels if lab in ehr_df.columns]
        present_abnorm = [f'{lab}_abnorm' for lab in present_labs]

        chunk_times = pd.to_datetime([t.decode('utf-8') for t in timestamps])
        stale_cols = {}
        for lab in present_labs:
            last_draw_time = chunk_times.where(ehr_df[lab].notna()).ffill()
            stale_cols[f'{lab}_stale'] = (chunk_times - last_draw_time).dt.total_seconds() / 60 / 60

        ehr_df[present_labs + present_abnorm] = ehr_df[present_labs + present_abnorm].ffill()
        ehr_df = pd.concat([ehr_df, pd.DataFrame(stale_cols, index=ehr_df.index)], axis=1)
        return ehr_df

def pull_icd_labels(code_labels=COMMON_CODES, include_demographics=False, signals = ['PLETH', 'II']):
    ds = ICUDataset(h5_dir='/u/project/jchiang/tsisson/mimic/data/h5/mimic4',
                    meta_index='/u/project/jchiang/tsisson/mimic/data/h5/mimic4/meta_index.csv',
                    code_index='/u/project/jchiang/tsisson/mimic/data/h5/mimic4/code_index.csv',
                    med_lab_index=None,
                    signals=signals,
                    code_labels=code_labels,
                    include_demographics=include_demographics)
    return ds

def pull_lab_labels(lab_labels=TOP_20_LABS, include_demographics=False, signals = ['PLETH', 'II']):
    ds = ICUDataset(h5_dir='/u/project/jchiang/tsisson/mimic/data/h5/mimic4',
                    meta_index='/u/project/jchiang/tsisson/mimic/data/h5/mimic4/meta_index.csv',
                    code_index=None,
                    med_lab_index='/u/project/jchiang/tsisson/mimic/data/h5/mimic4/meds_labs_index.csv',
                    signals=signals,
                    lab_labels=lab_labels,
                    include_demographics=include_demographics)
    return ds

def pull_med_labels(med_labels=INPUT_FEATURES, include_demographics=False, signals = ['PLETH', 'II']):
    ds = ICUDataset(h5_dir='/u/project/jchiang/tsisson/mimic/data/h5/mimic4',
                    meta_index='/u/project/jchiang/tsisson/mimic/data/h5/mimic4/meta_index.csv',
                    code_index=None,
                    med_lab_index='/u/project/jchiang/tsisson/mimic/data/h5/mimic4/meds_labs_index.csv',
                    signals=signals,
                    med_labels=med_labels,
                    med_categories=MED_CATEGORIES,
                    include_demographics=include_demographics)
    return ds


def test_dataloader(dataset, n_batches=3):
    print(f"Dataset size: {len(dataset.valid_meta)} subjects")
    print(f"Float features ({len(dataset.float_features)}): {dataset.float_features}")
    print(f"Int features ({len(dataset.int_features)}): {dataset.int_features}")
    print(f"Demo features ({len(dataset.demographic_cols)}): {dataset.demographic_cols}")
    print()

    loader = DataLoader(dataset, batch_size=1, num_workers=0)

    for i, batch in enumerate(loader):
        if i >= n_batches:
            break
        
        print(f"--- Batch {i} ---")
        for key, val in batch.items():
            if isinstance(val, torch.Tensor):
                print(f"  {key}: shape={val.shape}, dtype={val.dtype}, "
                      f"nan%={val.isnan().float().mean():.2%}")
            elif isinstance(val, dict):  # waves
                for sig, tensor in val.items():
                    print(f"  waves/{sig}: shape={tensor.shape}, dtype={tensor.dtype}")
            else:
                print(f"  {key}: {val}")
        print()

    # single subject sanity check - verify temporal continuity
    first_subject = dataset.valid_meta.iloc[0]['record_id']
    subject_chunks = [(s['chunk_id'], s['ehr_float']) for s in dataset 
                      if s['record_id'] == first_subject]
    
    chunk_ids = [c[0] for c in subject_chunks]
    assert chunk_ids == list(range(len(chunk_ids))), "chunks not sequential"
    print(f"Subject {first_subject}: {len(chunk_ids)} chunks, sequential OK")

    # verify ffill worked - no leading nans beyond first chunks
    ehr_stack = torch.stack([c[1] for c in subject_chunks])
    nan_by_chunk = ehr_stack.isnan().float().mean(dim=1)
    print(f"NaN% by chunk (first 10): {nan_by_chunk[:10].tolist()}")

if __name__ == '__main__':
    # config = load_config()
    # to_h5(mimic=4, config=config)
    meta_index = pd.read_csv('')
    med_lab_index = pd.read_csv('')
    code_index = pd.read_csv('')
    dataset = ICUDataset(meta_index=meta_index,
                         med_lab_index=med_lab_index,
                         code_index=code_index)