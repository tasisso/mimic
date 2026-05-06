import numpy as np
import wfdb
import pandas as pd
from scipy.signal import resample
from utils.constants import DEFAULT_CHUNK_DURATION, MED_MAP, TARGET_FS, SIGNAL_NAME_MAP, FINAL_SIGNALS
import datetime

def build_path(mimic, subject_id, record_id, base_path) -> str:
    """Returns (master_path, record_dir) for a given subject + record."""
    if mimic == 4:
        sub_group  = f"p{str(subject_id)[:3]}"
        subject_dir = f"p{subject_id}"
        record_dir  = f"{base_path}/{sub_group}/{subject_dir}/{record_id}"
        master_path = f"{record_dir}/{record_id}"
    return master_path, record_dir

def build_signal_map(record_signals, final_signals=FINAL_SIGNALS):
    """
    Map canonical signal names -> column index; for aligning segment data with variable signals present.
    Only map final_signals list for writing to h5. 
    """
    signal_map = {}
    col_idx = 0
    for raw_name in record_signals:
        canonical = SIGNAL_NAME_MAP.get(raw_name, None)
        if canonical:
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


def extract_waveforms(
    record_path: str,
    record_dir: str,
    source_fs: float,
    target_fs: float = TARGET_FS,
    chunk_duration: float = DEFAULT_CHUNK_DURATION,
) -> tuple[list[np.ndarray], list, dict[str, int], int]:
    """
    Stream all segments for one record into aligned, resampled chunks.
    :param: waveform root directory, 
    :param: record_path: reconstructed path to the record

    Return:
    chunks: list of (chunk_size, n_signals) float32 arrays
    timestamps: list of chunk start datetimes
    signal_map: dictionary mapping of signal type to chunk col index in chunks
    total_chunks: pre-computed total (for H5 dataset init)
    """
    master_header = wfdb.rdheader(record_path)

    layout_path = f"{record_dir}/{master_header.seg_name[0]}"
    layout_header = wfdb.rdheader(layout_path)
    record_signals = layout_header.sig_name

    signal_map = build_signal_map(record_signals)
    if not signal_map:
        print(f'Empty signal map for {record_path}')
        return [], [], {}, 0

    total_samples = master_header.sig_len
    chunk_size = int(chunk_duration * target_fs)
    total_chunks = int(np.ceil((total_samples / source_fs) / chunk_duration))

    start_timestamp = datetime.datetime.combine(
        master_header.base_date, master_header.base_time
    )
    chunk_timestamps = [
        start_timestamp + pd.Timedelta(seconds=i * chunk_duration)
        for i in range(total_chunks)
    ]

    chunks = []
    buffer = []
    chunk_id = 0

    for seg_name in master_header.seg_name[1:]:
        seg_path = f"{record_dir}/{seg_name}"
        rec = wfdb.rdrecord(seg_path)

        data = align_signals(rec.p_signal, rec.sig_name, signal_map)
        data = resample_signals(data, source_fs)
        buffer.append(data)

        total_buffered = sum(s.shape[0] for s in buffer)
        while total_buffered >= chunk_size:
            concat = np.vstack(buffer)
            chunks.append(concat[:chunk_size, :])
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
        chunks.append(np.vstack([remaining, pad]))

    return chunks, chunk_timestamps, signal_map, total_chunks


