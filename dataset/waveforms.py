import numpy as np
import wfdb
import pandas as pd
from scipy.signal import resample
from utils.constants import DEFAULT_CHUNK_DURATION, MED_MAP, TARGET_FS, SIGNAL_NAME_MAP
import datetime

def build_path(mimic, subject_id, record_id, base_path):
    """Returns (master_path, record_dir) for a given subject + record."""
    if mimic == 4:
        sub_group  = f"p{str(subject_id)[:3]}"
        subject_dir = f"p{subject_id}"
        record_dir  = f"{base_path}/{sub_group}/{subject_dir}/{record_id}"
        master_paths = [f"{record_dir}/{record_id}"]
    if mimic == 3:
        sub_group  = f"p{str(subject_id)[:2]}"
        subject_dir = f"p{subject_id}"
        record_dir  = f"{base_path}/{sub_group}/{subject_dir}"
        record_paths = wfdb.get_record_list(record_dir)
        master_paths = [f"{record_dir}/{s}" for s in record_paths 
                        if s[0] == 'p' and s[-1] != 'n']
    
    return master_paths, record_dir

def build_signal_map(record_signals):
    """
    Map canonical signal names -> column index; for aligning segment data with variable signals present.
    Only map final_signals list for writing to h5. 
    """
    signal_map = {}
    col_idx = 0
    for raw_name in record_signals:
        canonical = SIGNAL_NAME_MAP.get(raw_name, None)
        if canonical and canonical not in signal_map: #if plethR and plethL in same record, picks first one
            signal_map[canonical] = col_idx
            col_idx += 1
    return signal_map

def align_signals(waveform_array, signal_names, signal_map):
    """
    Align the segment data to the same column index via signal_map.
    Signal_map contains only the signals of interest (FINAL_SIGNALS)
    Returns:
        aligned_data: (n_samples, n_signals) array
    """
    #signal_map of all record_signals {'II': 0, 'V': 1, 'AVR': 2, 'ABP': 3, 'RESP': 4}
    n_samples = waveform_array.shape[0]
    n_signals = len(signal_map)
    #Initialize array
    aligned_data = np.full((n_samples, n_signals), np.nan, dtype=np.float32)

    for idx, signal_name in enumerate(signal_names):
        #Normalize signal name
        signal_name = SIGNAL_NAME_MAP.get(signal_name, None)
        if not signal_name:
            continue
        align_col = signal_map[signal_name]
        aligned_data[:, align_col] = waveform_array[:, idx]
        
    return aligned_data

def resample_signals(waveform_array, original_fs, target_fs=TARGET_FS):
    """
    Resample 2D waveform array (n_samples, n_signals) to target_fs
    Returns:
        resampled_data: (new_n_samples, n_signals) resampled array
    """
    
    if original_fs == target_fs:
        return waveform_array
    
    target_size = int(waveform_array.shape[0] * target_fs / original_fs)
    n_signals = waveform_array.shape[1]
    # Resample each signal (column) independently
    resampled_data = np.full((target_size, n_signals), np.nan, dtype=np.float32)
    
    for col_idx in range(n_signals):
        signal_col = waveform_array[:, col_idx]
        
        #Only resample non-NaN data
        valid_mask = ~np.isnan(signal_col)
        if valid_mask.sum() > 0:
            resampled_data[:, col_idx] = resample(signal_col[valid_mask], target_size)

    return resampled_data

def get_record_meta(record_path, record_dir, source_fs, chunk_duration):
    """
    Read wfdb headers.
    Returns metadata needed to init H5ChunkWriter and stream waveforms.
    """
    master_header = wfdb.rdheader(record_path)
    layout_header = wfdb.rdheader(f"{record_dir}/{master_header.seg_name[0]}")
    record_signals = layout_header.sig_name
    signal_map = build_signal_map(record_signals)
    if not signal_map:
        return None
    signal_names = list(signal_map.keys())

    total_samples = master_header.sig_len
    if total_samples <= 0:
        print(f'Invalid sig_len={total_samples} for {record_path}')
        return None
    total_chunks  = int(np.ceil((total_samples / source_fs) / chunk_duration))
    if total_chunks <= 0:
        print(f'Invalid total_chunks={total_chunks} for {record_path}, sig_len={total_samples}')
        return None
    start_timestamp = datetime.datetime.combine(
        master_header.base_date, master_header.base_time
    )
    chunk_timestamps = [
        start_timestamp + pd.Timedelta(seconds=i * chunk_duration)
        for i in range(total_chunks)
    ]

    return {
        'master_header': master_header,
        'record_signals': signal_names,
        'signal_map': signal_map,
        'total_chunks': total_chunks,
        #'chunk_size': chunk_size,
        'chunk_timestamps': chunk_timestamps,
    }


def stream_waveform_chunks(
    master_header, record_dir, source_fs,
    signal_map, chunk_size, chunk_timestamps
):
    """
    Stream one chunk_size chunk of waveform waveform data at a time.

    Yield:
    chunk_id: chunk identifier
    chunk_starttime: start timestamp of this chunk
    chunk_data: (chunk_size, n_signals) float32 array
    """

    buffer = []
    chunk_id = 0

    for seg_name in master_header.seg_name[1:]:
        if seg_name == '~':
            continue
        seg_path = f"{record_dir}/{seg_name}"
        rec = wfdb.rdrecord(seg_path)
        data = align_signals(rec.p_signal, rec.sig_name, signal_map)
        data = resample_signals(data, source_fs)
        buffer.append(data)

        total_buffered = sum(s.shape[0] for s in buffer)
        while total_buffered >= chunk_size:
            concat = np.vstack(buffer)
            chunk_data = concat[:chunk_size, :]
            chunk_starttime = chunk_timestamps[chunk_id]
            yield chunk_id, chunk_starttime, chunk_data

            remaining = concat[chunk_size:, :]
            buffer = [remaining] if remaining.shape[0] > 0 else []
            total_buffered = remaining.shape[0]
            chunk_id += 1

    # Final partial chunk — pad to chunk_size
    if buffer and buffer[0].shape[0] > 0:
        remaining = np.vstack(buffer)
        pad = np.full(
            (chunk_size - remaining.shape[0], remaining.shape[1]),
            np.nan, dtype=np.float32
        )
        yield chunk_id, chunk_timestamps[chunk_id], np.vstack([remaining, pad])


