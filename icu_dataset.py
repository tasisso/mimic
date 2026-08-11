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
            height_cm = row['height_cm']
            intime = row['intime']
            outtime = row['outtime']
            record_length = row['record_hrs']

            pbar.set_postfix({
                'record': f'{record_id}',
                'subject': f'{subject_id}',
            })

            if record_id in processed_ids:
                pbar.update(row['record_hrs'])
                print(f'Skipping: Record {record_id} - Processed')
                continue

            if record_length < 0.4:
                pbar.update(row['record_hrs'])
                print(f'Skipping: Record {record_id} - Less than 24 minutes of waveform data')
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

                meds_with_rate, med_on_only, med_categories = extractor.stay_input_labels.get(stay_id, (set(), set(), set()))
                lab_labels = extractor.stay_lab_labels.get((subject_id, hadm_id), set())

                has_meds = 0 if len(med_categories) == 0 else 1
                has_labs = 0 if len(lab_labels) == 0 else 1
                has_weight = pd.notna(weight_kg)

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
                    med_categories=med_categories,
                    meds_with_rate=meds_with_rate,
                    meds_on_only=med_on_only,
                    lab_labels=lab_labels,
                    has_weight=has_weight
                ) as writer:
                    # Static features as h5 attributes
                    _, codes, icd10_3 = extractor.get_codes(subject_id, hadm_id)
                    #writer.write_static(codes=codes)

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
                        chunk_starttime=chunk_starttime,
                        lookback=lookback
                    )
                        writer.write_chunk(chunk_id, chunk_starttime, chunk_data, signal_map, ehr_dict)
                        #print(f"[{record_id}] chunk {chunk_id}/{total_chunks}")
                print(f'Done with {record_id}')
                pbar.update(row['record_hrs'])
                append_index_row(meta_path, {
                    'record_id':  int(record_id),
                    'folder_path': master_path,
                    'start_record': record_meta['chunk_timestamps'][0],
                    'record_length': record_length,
                    'n_chunks':   int(total_chunks),
                    'signals':    '|'.join(sorted(signal_map.keys())),
                    'h5_filepath':   writer.filepath,
                    'subject_id': int(subject_id),
                    'hadm_id':    int(hadm_id),
                    'stay_id':    int(stay_id),
                    'icu_intime': intime,
                    'icu_outtime': outtime,
                    'los': row['los'],
                    'clinical_information_system': row['dbsource'],
                    'age': row['age'],
                    'age_group': row['age_group'],
                    'weight': weight_kg,
                    'height': height_cm,
                    'gender': row['gender'],
                    'ethnicity': row['ethnicity_group'],
                    'icd10':           '|'.join(codes),
                    'icd10_truncated': '|'.join(icd10_3),
                    'has_inputs': has_meds,
                    'has_labs': has_labs,
                    'inputs':          '|'.join(sorted(meds_with_rate | med_on_only)),
                    'input_categories': '|'.join(sorted(med_categories)),
                    'labs':            '|'.join(sorted(lab_labels)),
                })
                append_index_row(meds_labs_path, {
                    'record_id': int(record_id),
                    **{cat: int(cat in med_categories) for cat in MED_CATEGORIES},
                    **{med: int(med in (meds_with_rate | med_on_only))     for med in INPUT_FEATURES},
                    **{lab: int(lab in lab_labels)     for lab in LAB_MAP.values()},
                })
                append_index_row(codes_path, {
                    'record_id': int(record_id),
                    **{code: int(code in codes) for code in COMMON_CODES},
                })

    return meta_path

def _format_shape(t):
    return f"{tuple(t.shape)} {t.dtype}"


class ICUDataset(IterableDataset):
    def __init__(self,
                 metadata,
                 signals=['PLETH', 'II', 'ABP'],
                 med_labels=[],
                 med_categories=[],
                 lab_labels=[],
                 task='input_classification',
                 data_dir='./data/data'
                 ):
        """
        task options:
            'input_classification'  - binary on/off per med label
            'category_classification' - binary on/off per med category  
            'rate_regression'       - continuous infusion rates
            'icd_classification'    - static ICD labels
        """
        self.data_dir = data_dir
        self.task = task
        self.metadata = metadata
        self.signals = signals
        self.med_labels = list(med_labels)
        self.med_categories = list(med_categories)
        self.lab_labels = list(lab_labels)

        # EHR feature vector layout: for each lab → [value, abnormal, last_drawn_hrs]
        self.n_ehr_features = len(lab_labels) * 3
        # Target vector layouts
        self.n_input_targets    = len(med_labels)
        self.n_category_targets = len(med_categories)

    def __iter__(self):
        for _, row in self.metadata.iterrows():
            h5_path = row['h5_filepath']
            h5_path = self.data_dir + h5_path
            n_chunks = int(row['n_chunks'])

            file_labs   = set(row['labs'].split('|')) if row['has_labs'] else set()
            file_inputs = set(row['inputs'].split('|')) if row['has_inputs'] else set()
            file_cats   = set(row['input_categories'].split('|')) if row['has_inputs'] else set()

            with h5py.File(h5_path, 'r') as f:
                wave_keys = set(f['waveforms'].keys())
                chunk_size = f['waveforms'][next(iter(wave_keys))].shape[1]

                # Pre-load full arrays for each dataset we'll need — avoids per-chunk H5 indexing overhead
                wave_arrays = {
                    s: f['waveforms'][s][:] if s in wave_keys else None
                    for s in self.signals
                }

                # lab_arrays = {}
                # if 'labs' in f:
                #     for lab in self.lab_labels:
                #         if lab in file_labs:
                #             lab_arrays[lab] = {
                #                 'value':         f['labs'][f'{lab}_value'][:],
                #                 'abnormal':      f['labs'][f'{lab}_abnormal'][:],
                #                 'last_drawn_hrs': f['labs'][f'{lab}_last_drawn_hrs'][:],
                #             }

                input_arrays = {}
                cat_arrays = {}
                if 'inputs' in f:
                    for med in self.med_labels:
                        key = f'{med}_on'
                        if med in file_inputs and key in f['inputs']:
                            input_arrays[med] = f['inputs'][key][:]
                    for cat in self.med_categories:
                        key = f'{cat}_on'
                        if cat in file_cats and key in f['inputs']:
                            cat_arrays[cat] = f['inputs'][key][:]

            for i in range(n_chunks):
                # Waveform: (n_signals, chunk_size)
                waveform = np.stack([
                    wave_arrays[s][i] if wave_arrays[s] is not None
                    else np.full(chunk_size, np.nan, dtype=np.float32)
                    for s in self.signals
                ])
                waveform = np.nan_to_num(waveform, nan=0.0)

                # EHR: flat vector [lab0_value, lab0_abnormal, lab0_last_drawn_hrs, lab1_value, ...]
                # ehr = np.empty(self.n_ehr_features, dtype=np.float32)
                # ehr[:] = np.nan
                # for k, lab in enumerate(self.lab_labels):
                #     offset = k * 3
                #     if lab in lab_arrays:
                #         ehr[offset]     = lab_arrays[lab]['value'][i]
                #         ehr[offset + 1] = lab_arrays[lab]['abnormal'][i]
                #         ehr[offset + 2] = lab_arrays[lab]['last_drawn_hrs'][i]

                # Input key targets: (n_med_labels,) binary
                input_targets = np.full(self.n_input_targets, -1, dtype=np.float32)
                for k, med in enumerate(self.med_labels):
                    if med in input_arrays:
                        input_targets[k] = input_arrays[med][i]

                # Input category targets: (n_med_categories,) binary
                category_targets = np.full(self.n_category_targets, -1, dtype=np.float32)
                for k, cat in enumerate(self.med_categories):
                    if cat in cat_arrays:
                        category_targets[k] = cat_arrays[cat][i]
                
                all_targets = np.concatenate([input_targets, category_targets])  # (n_med_labels + n_med_categories,)


                yield (
                    torch.tensor(waveform, dtype=torch.float32),
                    #torch.tensor(ehr, dtype=torch.float32),
                    #torch.tensor(input_targets, dtype=torch.float32),
                    torch.tensor(all_targets, dtype=torch.float32),
                )

class ICUDatasetICD(IterableDataset):
    def __init__(self, metadata, icd_matrix, signals, data_dir):
        self.metadata   = metadata.reset_index(drop=True)
        self.icd_matrix = icd_matrix  # (n_files, n_icd_codes)
        self.signals    = signals
        self.data_dir   = data_dir

    def __iter__(self):
        for j, row in self.metadata.iterrows():
            h5_path  = os.path.join(self.data_dir, row['h5_filepath'])
            icd_vec  = self.icd_matrix[j].astype(np.float32)  # (n_icd_codes,)
            n_chunks = row['n_chunks']

            with h5py.File(h5_path, 'r') as f:
                wave_keys  = set(f['waveforms'].keys())
                chunk_size = f['waveforms'][next(iter(wave_keys))].shape[1]

                wave_arrays = {
                    s: f['waveforms'][s][:] if s in wave_keys else None
                    for s in self.signals
                }

            for i in range(n_chunks):
                waveform = np.stack([
                    wave_arrays[s][i] if wave_arrays[s] is not None
                    else np.full(chunk_size, 0.0, dtype=np.float32)
                    for s in self.signals
                ])
                waveform = np.nan_to_num(waveform, nan=0.0)

                yield (
                    torch.tensor(waveform, dtype=torch.float32),
                    torch.tensor(icd_vec,  dtype=torch.float32)
                )


if __name__ == '__main__':
    import argparse
    from utils.utils import load_config
    from utils.constants import INPUT_FEATURES, MED_CATEGORIES, LAB_MAP

    parser = argparse.ArgumentParser()
    parser.add_argument('--mimic', type=int, default=4, choices=[3, 4])
    parser.add_argument('--n_files', type=int, default=3, help='Number of H5 files to test on')
    parser.add_argument('--batch_size', type=int, default=8)
    parser.add_argument('--num_workers', type=int, default=0)
    args = parser.parse_args()

    config = load_config('configs/config.yaml')
    #meta_path = f"{config['paths'][f'mimic{args.mimic}']['output_dir']}/metadata.csv"
    meta_path = 'data/metadata.csv'

    metadata = pd.read_csv(meta_path).head(args.n_files)
    print(f"Testing on {len(metadata)} files, {metadata['n_chunks'].sum()} total chunks")

    lab_labels   = list(LAB_MAP.values())
    med_labels   = INPUT_FEATURES
    med_categories = list(MED_CATEGORIES)

    dataset = ICUDataset(
        metadata=metadata,
        signals=['PLETH', 'II', 'ABP'],
        med_labels=med_labels,
        med_categories=med_categories,
        lab_labels=lab_labels,
    )

    loader = DataLoader(dataset, batch_size=args.batch_size, num_workers=args.num_workers)

    print(f"\nEHR feature dim : {dataset.n_ehr_features}  ({len(lab_labels)} labs × 3)")
    print(f"Input targets   : {dataset.n_input_targets}  ({len(med_labels)} meds)")
    print(f"Category targets: {dataset.n_category_targets}  ({len(med_categories)} categories)")

    n_batches = 0
    for batch in loader:
        waveform, ehr, input_targets, category_targets = batch
        if n_batches == 0:
            print(f"\nFirst batch shapes:")
            print(f"  waveform        : {_format_shape(waveform)}")
            print(f"  ehr             : {_format_shape(ehr)}")
            print(f"  input_targets   : {_format_shape(input_targets)}")
            print(f"  category_targets: {_format_shape(category_targets)}")

            nan_frac = waveform.isnan().float().mean().item()
            ehr_nan  = ehr.isnan().float().mean().item()
            print(f"\n  waveform NaN fraction : {nan_frac:.3f}")
            print(f"  ehr      NaN fraction : {ehr_nan:.3f}")

            active_inputs = input_targets.sum(dim=0)
            active_cats   = category_targets.sum(dim=0)
            print(f"\n  Active input labels in batch  (nonzero): {(active_inputs > 0).sum().item()}/{dataset.n_input_targets}")
            print(f"  Active category labels in batch (nonzero): {(active_cats > 0).sum().item()}/{dataset.n_category_targets}")
            print(f"\n  Category label counts:")
            for cat, count in zip(med_categories, active_cats.tolist()):
                if count > 0:
                    print(f"    {cat:<22}: {int(count)}")

        n_batches += 1

    total_chunks = n_batches * args.batch_size
    print(f"\nIterated {n_batches} batches (~{total_chunks} chunks). Done.")
