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

from utils.constants import BOLUS_FEATURES, VASOACTIVE

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
        med_categories: set[str],
        meds_with_rate: set[str],
        meds_on_only: set[str],
        lab_labels: set[str],
        has_weight: bool
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

        self.filepath = f"{output_dir}/p{subject_id}_{record_id}.h5"
        self._h5 = h5py.File(self.filepath, 'a')

        self.chunk_size    = chunk_size
        self.total_chunks  = total_chunks
        self.record_signals = record_signals


        self.has_weight = has_weight
        self.has_labs = len(lab_labels) > 0
        self.has_meds = len(med_categories) > 0
        #Feature labels
        self.lab_labels = lab_labels
        self.meds_with_rate = meds_with_rate
        self.meds_on_only = meds_on_only

        self.med_categories = med_categories
        self.has_pressor = 'vasopressor' in med_categories

        # Root attrs
        self._h5.attrs['subject_id'] = subject_id
        self._h5.attrs['hadm_id']    = hadm_id
        self._h5.attrs['stay_id']    = stay_id
        self._h5.attrs['record_id']  = record_id

        self._wave_group = self._h5['waveforms'] if 'waveforms' in self._h5 else self._h5.create_group('waveforms')
        if self.has_labs:
            self._lab_group = self._h5['labs'] if 'labs' in self._h5 else self._h5.create_group('labs')
        else:
            self._lab_group = None
        if self.has_meds:
            self._meds_group = self._h5['inputs'] if 'inputs' in self._h5 else self._h5.create_group('inputs')
        else:
            self._meds_group = None

        self._waveform_datasets = {}
        self._labs_datasets = {}
        self._meds_datasets = {}
        self._timestamps_dataset = None
        self._ehr_dataset = None

        self._lab_ffill_state = {}
        for lab in self.lab_labels:
            self._lab_ffill_state[lab] = {
                'value': np.nan,
                'abnorm': np.nan,
                'last_measured_time': None,
            }

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
        self._write_ehr(chunk_id, ehr_dict, timestamp=timestamp)
    
    # def write_static(self, codes: list):
        
    #     if len(codes) > 0:
    #         self._h5.attrs['icd'] = '|'.join(codes)
    #     else:
    #         self._h5.attrs['icd'] = ''

    def close(self):
        if self._h5.id.valid:
            self._h5.flush()
            self._h5.close()

    def _init_datasets(self) -> None:
        self._init_timestamp_dataset()
        self._init_waveform_datasets()
        self._init_ehr_datasets()

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

    def _init_ehr_datasets(self) -> None:
        med_features, lab_features = self._build_feature_specs()

        if self.has_meds:
            if 'inputs' not in self._h5:
                self._meds_group = self._h5.create_group('inputs')
            for name, dtype in med_features:
                self._meds_datasets[name] = self._init_1d_dataset(self._meds_group, name, dtype)

        if self.has_labs:
            if 'labs' not in self._h5:
                self._lab_group = self._h5.create_group('labs')
            for name, dtype in lab_features:
                self._labs_datasets[name] = self._init_1d_dataset(self._lab_group, name, dtype)

    def _init_1d_dataset(self, group, name, dtype):
        if name not in group:
            fill = np.nan if np.issubdtype(dtype, np.floating) else 0
            ds = group.create_dataset(
                name,
                shape=(self.total_chunks,),
                dtype=dtype,
                compression="gzip",
            )
            ds[:] = np.full(self.total_chunks, fill, dtype=dtype)
        else:
            ds = group[name]
        return ds

    def _build_feature_specs(self):
        med_features = []
        lab_features = []
        if self.has_meds:
            for category in self.med_categories:
                med_features.append((f'{category}_on', np.int32))
            for med_name in self.meds_with_rate:
                if self.has_weight:
                    med_features.append((f'{med_name}_ratenorm', np.float32))
                med_features.append((f'{med_name}_on', np.int32))
            for med_name in self.meds_on_only:
                med_features.append((f'{med_name}_on', np.int32))
            if self.has_weight:
                if self.has_pressor:
                    med_features.append(('norepi_eq', np.float32))

        if self.has_labs:
            for lab_name in self.lab_labels:
                lab_features.append((f'{lab_name}_value', np.float32))
                lab_features.append((f'{lab_name}_abnormal', np.int32))
                lab_features.append((f'{lab_name}_last_drawn_hrs', np.float32))

        return med_features, lab_features

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

    def _write_ehr(self, chunk_id, ehr_dict, timestamp=None) -> None:
        if self.has_meds:
            self._write_meds(chunk_id, ehr_dict)
        if self.has_labs:
            self._write_labs(chunk_id, ehr_dict, timestamp)

    def _write_meds(self, chunk_id, ehr_dict) -> None:
        for name, ds in self._meds_datasets.items():
            if name in ehr_dict:
                ds[chunk_id] = ehr_dict[name]

    def _write_labs(self, chunk_id, ehr_dict, timestamp) -> None:
        chunk_time = pd.Timestamp(timestamp) if timestamp is not None else None

        for lab in self.lab_labels:
            state = self._lab_ffill_state[lab]
            raw_val = ehr_dict.get(lab, np.nan)
            raw_abnorm = ehr_dict.get(f'{lab}_abnorm', np.nan)

            if not np.isnan(raw_val):
                state['value'] = raw_val
                state['abnorm'] = int(raw_abnorm) if not np.isnan(raw_abnorm) else state['abnorm']
                state['last_measured_time'] = chunk_time

            self._labs_datasets[f'{lab}_value'][chunk_id] = state['value']
            self._labs_datasets[f'{lab}_abnormal'][chunk_id] = state['abnorm'] if not np.isnan(state['abnorm']) else 0
            if chunk_time is not None and state['last_measured_time'] is not None:
                self._labs_datasets[f'{lab}_last_drawn_hrs'][chunk_id] = (chunk_time - state['last_measured_time']).total_seconds() / 3600.0



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
