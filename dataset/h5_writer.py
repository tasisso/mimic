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

from utils.constants import SIGNAL_NAME_MAP, INPUT_LABELS, LAB_MAP, MED_CATEGORIES

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
        target_signals: list[str],
        record_signals: list[str],
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
        """
        self.subject_id = subject_id
        self.hadm_id    = hadm_id
        self.stay_id    = stay_id
        self.record_id  = record_id
        self.chunk_size    = chunk_size
        self.total_chunks  = total_chunks
        self.target_signals = target_signals
        self.record_signals = record_signals

        self.filepath = f"{output_dir}/{subject_id}_{stay_id}_{record_id}.h5"
        self._h5       = h5py.File(self.filepath, "a")
        self._group    = self._make_group()

        self._waveform_datasets: dict[str, h5py.Dataset] = {}
        self._timestamps_dataset: h5py.Dataset | None = None
        self._ehr_dataset: h5py.Dataset | None = None

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
    ) -> None:
        """
        Write one chunk. All arrays must already be resampled, aligned, and padded
        to exactly self.chunk_size rows before calling this.
        """
        self._write_waveforms(chunk_id, signal_data, signal_map)
        self._write_timestamp(chunk_id, timestamp)
        self._write_ehr(chunk_id, ehr_dict)
    
    def write_static(self, codes: list, demographics: dict = None):
        """
        Write static per-stay features as group attributes.
        Called once per record, not per chunk.
        """
        
        self._group.attrs['codes'] = codes
        
        if demographics:
            for key, value in demographics.items():
                self._group.attrs[key] = value

    def close(self) -> None:
        if self._h5.id.valid:
            self._h5.flush()
            self._h5.close()

    #Inits
    def _make_group(self) -> h5py.Group:
        group_name = f"{self.subject_id}_{self.stay_id}_{self.record_id}"
        
        if group_name not in self._h5:
            g = self._h5.create_group(group_name)
        else:
            g = self._h5[group_name]

        g.attrs['subject_id'] = self.subject_id
        g.attrs['hadm_id']    = self.hadm_id
        g.attrs['stay_id']    = self.stay_id
        g.attrs['record_id']  = self.record_id

        record_signal_set = set(self.record_signals)
        for sig in self.target_signals:
            g.attrs[f'has_{sig}'] = int(sig in record_signal_set)

        return g

    def _init_datasets(self) -> None:
        """Create all datasets up front with known shapes. Called once in __init__."""
        
        self._init_waveform_datasets()
        self._init_timestamp_dataset()
        self._init_ehr_dataset()

    def _init_waveform_datasets(self) -> None:
        for raw_name in self.record_signals:
            sig_name = SIGNAL_NAME_MAP.get(raw_name)
            if not sig_name:
                continue
            if sig_name not in self._group:
                ds = self._group.create_dataset(
                    sig_name,
                    shape=(self.total_chunks, self.chunk_size),
                    dtype=np.float32,
                    compression="gzip",
                    compression_opts=4,
                )
            else:
                ds = self._group[sig_name]
            self._waveform_datasets[sig_name] = ds

    def _init_timestamp_dataset(self) -> None:
        if "timestamps" not in self._group:
            self._timestamps_dataset = self._group.create_dataset(
                "timestamps",
                shape=(self.total_chunks,),
                dtype=h5py.string_dtype(encoding="utf-8"),
            )
        else:
            self._timestamps_dataset = self._group["timestamps"]

    def _init_ehr_dataset(self) -> None:
        ehr_dtype = build_ehr_dtype()
        if "ehr_values" not in self._group:
            self._ehr_dataset = self._group.create_dataset(
                "ehr_values",
                shape=(self.total_chunks,),
                dtype=ehr_dtype,
                compression="gzip",
            )
            # HDF5 defaults to 0 for everything -> fix float fields to NaN
            empty = np.zeros(self.total_chunks, dtype=ehr_dtype)
            for field, (dtype, _) in ehr_dtype.fields.items():
                if np.issubdtype(dtype, np.floating):
                    empty[field] = np.nan
            self._ehr_dataset[:] = empty
        else:
            self._ehr_dataset = self._group["ehr_values"]

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

    def _write_ehr(self, chunk_id: int, ehr_dict: dict[str, float]) -> None:
        """
        Write scalar EHR values for one chunk.
        - float32 fields: stored as-is (NaN preserved)
        - int32 fields:   NaN -> 0
        """
        row = self._ehr_dataset[chunk_id]
        valid_keys = set(row.dtype.names)

        for key, value in ehr_dict.items():
            if key not in valid_keys:
                print(key)
                continue
            field_dtype = row.dtype.fields[key][0]
            if np.issubdtype(field_dtype, np.floating):
                self._ehr_dataset[key, chunk_id] = value
            else:
                self._ehr_dataset[key, chunk_id] = value if pd.notna(value) else 0


def build_ehr_dtype() -> np.dtype:
    ehr_dtype_list = []

    # Med category flags
    for category in MED_CATEGORIES:
        ehr_dtype_list.append((f'{category}_on', np.int32))

    # Per-med features
    for med_name in INPUT_LABELS:
        ehr_dtype_list.append((f'{med_name}_ratenorm', np.float32))
        ehr_dtype_list.append((f'{med_name}_bolus', np.int32))
        ehr_dtype_list.append((f'{med_name}_on', np.int32))

    # Derived
    ehr_dtype_list.extend([
        ('norepi_eq', np.float32),
        ('vasoactive_on', np.int32),
    ])

    # Labs
    for lab_name in LAB_MAP.values():
        ehr_dtype_list.append((lab_name, np.float32))

    return np.dtype(ehr_dtype_list)

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
