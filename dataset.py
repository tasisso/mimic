from dataset.waveforms import extract_waveforms
from dataset.h5_writer import H5ChunkWriter
from dataset.ehr import ehrExtractor
from utils.utils import load_config
from utils.constants import TARGET_SIGNALS, LAB_MAP, INPUT_FEATURES, MED_CATEGORIES
import pandas as pd


def build_path(mimic, subject_id, record_id, base_path) -> tuple[str, str]:
    """Returns (master_path, record_dir) for a given subject + record."""
    sub_group  = f"p{str(subject_id)[:3]}"
    subject_dir = f"p{subject_id}"
    record_dir  = f"{base_path}/{sub_group}/{subject_dir}/{record_id}"
    master_path = f"{record_dir}/{record_id}"
    return master_path, record_dir


def to_h5(mimic, config) -> None:
    cohort = pd.read_csv(config['paths'][f'mimic{mimic}']['cohort'])

    extractor = ehrExtractor(
        inputs=pd.read_csv(config['paths'][f'mimic{mimic}']['inputs']),
        labs=pd.read_csv(config['paths'][f'mimic{mimic}']['labs']),
        codes=pd.read_csv(config['paths'][f'mimic{mimic}']['icd']),
    )
    #H5 Meta
    index_rows = []
    for _, row in cohort.iterrows():
        subject_id = row['subject_id']
        hadm_id = row['hadm_id']
        stay_id = row['stay_id']
        record_id = row['record_id']
        weight_kg = row['weight_kg']
        meta = {
            'age': row['age'],
            'age_group': row['age_group'],
            'ethnicity_group': row['ethnicity_group'],
            'gender': row['gender'],
            'weight': weight_kg,
            'los': row['los'],
            'dbsource': row['dbsource'],
            'mimic': mimic
        }

        master_path, record_dir = build_path(
            mimic, subject_id, record_id, config['paths'][f'mimic{mimic}']['waveforms_root']
        )

        waveform_chunks, chunk_timestamps, signal_map, total_chunks = extract_waveforms(
            record_path=master_path,
            record_dir=record_dir,
            chunk_duration=config['signals']['chunk_duration'],
            source_fs=row['fs'],
            target_fs=config['signals']['target_fs'],
        )

        if not waveform_chunks:
            print(f"No target signals for record {record_id}, skipping.")
            continue

        #
        stay_meds, stay_cats = extractor._stay_meds_cache.get(stay_id, (set(), set()))
        stay_labs = extractor._stay_labs_cache.get((subject_id, hadm_id), set())

        with H5ChunkWriter(
            output_dir=config['paths']['output_dir'],
            subject_id=subject_id,
            hadm_id=hadm_id,
            stay_id=stay_id,
            record_id=record_id,
            chunk_size=int(config['signals']['chunk_duration'] * config['signals']['target_fs']),
            total_chunks=total_chunks,
            target_signals=TARGET_SIGNALS,
            record_signals=list(signal_map.keys()),
        ) as writer:
            # Static features as h5 attributes
            codes = extractor.get_codes(subject_id, hadm_id)
            writer.write_static(codes=codes, demographics=meta)

            # Dynamic features per chunk 
            for chunk_id, (chunk_data, chunk_starttime) in enumerate(
                zip(waveform_chunks, chunk_timestamps)
            ):
                ehr_dict = extractor.get_features(
                subject_id=subject_id,
                hadm_id=hadm_id,
                stay_id=stay_id,
                patientweight=weight_kg,
                chunk_starttime=chunk_starttime,
            )
                writer.write_chunk(chunk_id, chunk_starttime, chunk_data, signal_map, ehr_dict)
                #print(f"[{record_id}] chunk {chunk_id}/{total_chunks}")
        print(f'Done with {record_id}')
        index_rows.append({
            'filepath':        writer.filepath,
            'subject_id':      subject_id,
            'hadm_id':         hadm_id,
            'stay_id':         stay_id,
            'record_id':       record_id,
            'n_chunks':        total_chunks,
            'codes':           '|'.join(codes),
            **meta,
            **{f'has_{sig}': int(sig in signal_map) for sig in TARGET_SIGNALS},
            **{f'has_{med}': int(med in stay_meds)  for med in INPUT_FEATURES},
            **{f'has_{cat}': int(cat in stay_cats)  for cat in MED_CATEGORIES},
            **{f'has_{lab}': int(lab in stay_labs)  for lab in MED_CATEGORIES},
        })

    index = pd.DataFrame(index_rows)
    index.to_csv(f"{config['output_dir']}/h5_index.csv", index=False)
    return index

if __name__ == '__main__':
    config = load_config()
    to_h5(mimic=4, config=config)