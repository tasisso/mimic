"""
h5_writer.py

Handles all HDF5 I/O for waveform + EHR chunk storage.

H5 layout per file:
    /{subject_id}_{stay_id}_{record_id}/
        ├── {SIGNAL_NAME}/      float32 (total_chunks, chunk_size)
        ├── timestamps/         UTF-8   (total_chunks,)
        └── ehr_values/         structured dtype (total_chunks,)
"""

import h5py
import numpy as np
import pandas as pd

from utils.constants import BOLUS_FEATURES

class H5ChunkWriter:
    """
    Write pre-processed waveform + EHR chunks for one (subject, hadm, stay, record)
    into a single HDF5 file.

    Usage
    -----
    writer = H5ChunkWriter(...)
    for chunk_id, chunk_starttime, chunk_data, signal_map, agg_ehr in chunks:
        writer.write_chunk(chunk_id, chunk_starttime, chunk_data, signal_map, agg_ehr)
    writer.close()

    Or use as a context manager:
        with H5ChunkWriter(...) as writer:
            writer.write_chunk(...)
    """

    def __init__(
        self,
        output_dir: str,
        subject_id: int,
        hadm_id: int,
        stay_id: int,
        record_id: int,
        chunk_size: int,
        total_chunks: int,
        record_signals: list[str],
        med_cat_feats: set[str],
        med_feats: set[str],
        lab_feats: set[str],
    ):
        """
        Parameters
        ----------
        output_dir      : directory where .h5 files are written
        subject_id      : MIMIC subject_id
        hadm_id         : MIMIC hadm_id
        stay_id         : MIMIC stay_id
        record_id       : waveform record ID (e.g. '81739927')
        chunk_size      : samples per chunk (e.g. target_fs * chunk_duration)
        total_chunks    : pre-computed number of chunks for this record
        target_signals  : all signals the pipeline cares about (for has_{signal} attrs)
        record_signals  : signals actually present in this record's layout header
        has_labs        : sid,hid labs drawn
        has_meds        : meds of interest present
        """

        self.filepath = f"{output_dir}/{subject_id}_{record_id}.h5"
        self._h5 = h5py.File(self.filepath, 'a')

        self.chunk_size    = chunk_size
        self.total_chunks  = total_chunks
        self.record_signals = record_signals

        self.has_labs = 0 if len(lab_feats) == 0 else 1
        self.has_meds = 0 if len(med_feats) == 0 else 1
        #Feature labels
        self.lab_feats = lab_feats
        self.med_feats = med_feats
        self.med_cat_feats = med_cat_feats

        # Root attrs
        self._h5.attrs['subject_id'] = subject_id
        self._h5.attrs['hadm_id']    = hadm_id
        self._h5.attrs['stay_id']    = stay_id
        self._h5.attrs['record_id']  = record_id

        self._wave_group = self._h5['waveforms'] if 'waveforms' in self._h5 else self._h5.create_group('waveforms')

        self._waveform_datasets = {}
        self._timestamps_dataset = None
        self._ehr_dataset = None

        self._init_datasets()

    #Context managers
    def __enter__(self):
        return self

    def __exit__(self, *_):
        self.close()

    #Writes
    def write_chunk(
        self,
        chunk_id: int,
        timestamp,                      # datetime or ISO string
        signal_data: np.ndarray,        # (chunk_size, n_signals)
        signal_map: dict[str, int],     # signal_name -> column index in signal_data
        ehr_dict: dict[str, float],     # feature_name -> scalar (may be NaN)
    ):
        self._write_timestamp(chunk_id, timestamp)
        self._write_waveforms(chunk_id, signal_data, signal_map)
        self._write_ehr(chunk_id, ehr_dict)
    
    def write_static(self, codes: list):
        
        if len(codes) > 0:
            self._h5.attrs['icd'] = '|'.join(codes)
        else:
            self._h5.attrs['icd'] = ''

    def close(self):
        if self._h5.id.valid:
            self._h5.flush()
            self._h5.close()

    def _init_datasets(self) -> None:
        
        self._init_waveform_datasets()
        self._init_timestamp_dataset()
        self._init_ehr_dataset()

    def _init_waveform_datasets(self) -> None:
        for sig_name in self.record_signals:
            if sig_name not in self._wave_group:
                ds = self._wave_group.create_dataset(
                    sig_name,
                    shape=(self.total_chunks, self.chunk_size),
                    dtype=np.float32,
                    compression="gzip",
                    compression_opts=5,
                )
            else:
                ds = self._wave_group[sig_name]
            self._waveform_datasets[sig_name] = ds

    def _init_timestamp_dataset(self) -> None:
        if "timestamps" not in self._h5:
            self._timestamps_dataset = self._h5.create_dataset(
                "timestamps",
                shape=(self.total_chunks,),
                dtype=h5py.string_dtype(encoding="utf-8"),
            )
        else:
            self._timestamps_dataset = self._h5["timestamps"]

    def _init_ehr_dataset(self) -> None:
        ehr_dtype = self.build_ehr_dtype()
        if "ehr" not in self._h5:
            self._ehr_dataset = self._h5.create_dataset(
                "ehr",
                shape=(self.total_chunks,),
                dtype=ehr_dtype,
                compression="gzip",
            )
            empty = np.full(self.total_chunks, 0, dtype=ehr_dtype)
            for field, (dtype, _) in ehr_dtype.fields.items():
                if np.issubdtype(dtype, np.floating):
                    empty[field] = np.nan
                elif h5py.check_string_dtype(dtype):
                    empty[field] = ""
            self._ehr_dataset[:] = empty
        else:
            self._ehr_dataset = self._h5["ehr"]

    #Per chunk writes (private)
    def _write_waveforms(
        self,
        chunk_id: int,
        signal_data: np.ndarray,
        signal_map: dict[str, int],
    ):
        for sig_name, col_idx in signal_map.items():
            if sig_name in self._waveform_datasets:
                self._waveform_datasets[sig_name][chunk_id] = signal_data[:, col_idx]

    def _write_timestamp(self, chunk_id: int, timestamp) -> None:
        self._timestamps_dataset[chunk_id] = str(timestamp)

    def _write_ehr(self, chunk_id, ehr_dict) -> None:

        row = self._ehr_dataset[chunk_id]
        valid_keys = set(row.dtype.names)

        for key, value in ehr_dict.items():
            if key not in valid_keys:
                print(f'Invalid key: {key}')
                continue
            row[key] = str(value) if h5py.check_string_dtype(row.dtype.fields[key][0]) else value
            #field_dtype = row.dtype.fields[key][0]
            # if h5py.check_string_dtype(field_dtype):
            #     self._ehr_dataset[key, chunk_id] = str(value)
            # else:
            #     self._ehr_dataset[key, chunk_id] = value
        self._ehr_dataset[chunk_id] = row


    def build_ehr_dtype(self) -> np.dtype:
        ehr_dtypes = []

        if self.has_meds:
            # Med category flags
            for category in self.med_cat_feats:
                ehr_dtypes.append((f'{category}_on', np.int32))

            # Per-med features
            for med_name in self.med_feats:
                ehr_dtypes.append((f'{med_name}_ratenorm', np.float32))
                ehr_dtypes.append((f'{med_name}_on', np.int32))
                if med_name in BOLUS_FEATURES:
                    ehr_dtypes.append((f'{med_name}_bolus', np.int32))

            # Derived
            ehr_dtypes.extend([
                ('norepi_eq', np.float32),
                ('vasoactive_on', np.int32),
            ])

        # Labs
        if self.has_labs:
            for lab_name in self.lab_feats:
                ehr_dtypes.append((lab_name, np.float32))
                ehr_dtypes.append((f'{lab_name}_abnorm', np.float32))

        return np.dtype(ehr_dtypes)

def aggregate_ehr(ehr_dict: dict[str, np.ndarray]) -> dict[str, float]:
    """
    Collapse per-second EHR arrays to a single scalar per chunk.
    Uses nanmax so one real value anywhere in the window survives.
    All-NaN arrays produce NaN (preserved for float fields, coerced to 0 for int fields
    at write time inside H5ChunkWriter._write_ehr).
    """
    return {
        key: np.nan if np.all(np.isnan(arr)) else float(np.nanmax(arr))
        for key, arr in ehr_dict.items()
    }


def pad_signal(data: np.ndarray, target_rows: int) -> np.ndarray:
    """Right-pad a (n, signals) array with NaN rows to reach target_rows."""
    deficit = target_rows - data.shape[0]
    if deficit <= 0:
        return data
    pad = np.full((deficit, data.shape[1]), np.nan, dtype=np.float32)
    return np.vstack([data, pad])
