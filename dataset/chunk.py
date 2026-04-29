import numpy as np
import pandas as pd
import os
import wfdb
import datetime
from utils.constants import SIGNAL_NAME_MAP

def write_h5(self, mimic_db=None, ehr_extractor=None, to_csv=False):
        
    #mimic4db -> 'mimic4wdb/0.1.0'
    #1) Open the master header wfdb file to get the waveform start time + record len = end time 
    #2) Try to match the header with an icustay from the table based on start time with some 12hr buffer (will need to load in the full tables)
    #3) Open the layout header to get which signal types are present -> set as attributes on subject_id, hadm_id specific h5 file
    #4) Collect 60s of record data across record chunks, then filter icd_codes, labs, meds tables on start and end timestamps
    #5) Write chunk of waveform data as list of signal chunks, build continuous features 
    
    
    for _, row in self.meta.iterrows():
        subject_id = row['subject_id']
        hadm_id = row['hadm_id']
        stay_id = row['stay_id']
        #record_id = row['record_id']
        #start_timestamp = pd.to_datetime(row['start_timestamp'])
        fs = row['fs']
        total_samples = row['num_samps']
        patientweight = row['patientweight']
        #Format path for accessing in hoffman
        sub_group = '/p' + str(subject_id)[:3] #/p100
        subject_dir = '/p' + str(subject_id) + '/' #/p10094810
        full_path = self.base_path + sub_group + subject_dir
        #print(full_path)
        record_dirs = os.listdir(full_path)
        records_file = os.path.join(full_path, 'RECORDS')

        with open(records_file, 'r') as f:
            records = f.read().strip().split('\n')
        
        for record in records: #Like 81739927/81739927
            record_id = record.split('/')[0]
            record_dir = os.path.join(full_path, record_id)
            master_path = os.path.join(full_path, record)
            #print(master_path) #.../waves/p147/p14759494/87555605/87555605
            #Master header meta
            master_header = wfdb.rdheader(master_path)
            
            #Layout header meta
            layout = master_header.seg_name[0]
            layout_path = os.path.join(record_dir, layout)
            layout_header = wfdb.rdheader(layout_path)
            record_signals = layout_header.sig_name #all signal labels that appear within at least one record segment
            signal_map = {}
            signal_idx = 0
            for signal in record_signals:
                if signal in SIGNAL_NAME_MAP:
                    signal_name = SIGNAL_NAME_MAP[signal]
                    signal_map[signal_name] = signal_idx
                    signal_idx += 1
            #Check for target signals in this record
            #print(signal_map, record_signals)
            if not signal_map:
                continue
            
            #total_samples = sum(master_header.seg_len)
            #Create chunk timestamps for entire record (one start timestamp per chunk)
            total_samples = master_header.sig_len
            total_chunks = int(np.ceil((total_samples / fs) / self.chunk_duration))
            start_timestamp = datetime.datetime.combine(master_header.base_date, master_header.base_time)
            chunk_timestamps = np.array([
                start_timestamp + pd.Timedelta(seconds=i * self.chunk_duration)
                for i in range(total_chunks)
            ])
    
            
            writer = H5ChunkWriter(
                output_dir=self.output_dir,
                subject_id=subject_id,
                hadm_id=hadm_id,
                stay_id=stay_id,
                record_id=record_id,
                target_signals=self.all_signals,
                record_signals=record_signals,
                total_chunks=total_chunks
            )

            offset = 0
            buffer = []
            chunk_size = int(self.chunk_duration * self.target_fs)
            chunk_id = 0
            for seg in master_header.seg_name[1:]: 
                #seg_header = wfdb.rdheader(seg, pn_dir=record_dir)
                segment_path = os.path.join(record_dir, seg)
                rec = wfdb.rdrecord(segment_path)
                #print(f"Collected {segment_path}")
                signal_array = np.array(rec.p_signal, dtype=np.float32)
                
                #Align columns for record signals across buffer
                data = self.align_signals(rec.p_signal, rec.sig_name, signal_map)                 
                data = self.resample_array(data, fs)
                
                buffer.append(data)
                offset += data.shape[0] #number of samples 

                total_buffered = sum([segment_data.shape[0] for segment_data in buffer])
                while total_buffered >= chunk_size:
                    #Concatenate buffered data
                    concat_data = np.vstack(buffer)
                    #Extract 60s waveform chunk
                    chunk_data = concat_data[:chunk_size, :]
                    #Index chunk timestamp
                    chunk_starttime = chunk_timestamps[chunk_id]
                    #Extract 60s ehr features
                    chunk_ehr = ehr_extractor.get_features(subject_id, hadm_id, stay_id, patientweight, chunk_starttime)
                    
                    #Write to H5
                    # Aggregate EHR dict to scalars per chunk
                    agg_ehr = {}
                    for key, value_array in chunk_ehr.items():
                        if np.all(np.isnan(value_array)):
                            #All nan
                            agg_ehr[key] = np.nan
                        else:
                            # Use nanmax to ignore NaN values
                            agg_ehr[key] = np.nanmax(value_array)
                    writer.write_chunk(chunk_id, chunk_starttime, chunk_data, signal_map, agg_ehr)
                    print(f"+ Chunk {chunk_id}")
                    #Remove processed samples
                    remaining_data = concat_data[chunk_size:, :]
                    buffer = [remaining_data] if remaining_data.shape[0] > 0 else []
                    
                    total_buffered = remaining_data.shape[0]
                    chunk_id += 1
                
            #Handle remaining data
            if buffer and buffer[0].shape[0] > 0:
                chunk_starttime = chunk_timestamps[chunk_id]
                remaining_data = np.vstack(buffer)
                remaining_ehr = ehr_extractor.get_features(subject_id, hadm_id, stay_id, patientweight, chunk_starttime)
                #Pad with NaN to match chunk_size
                n_remaining = remaining_data.shape[0]
                n_padding = chunk_size - n_remaining
                
                # Create padding array filled with NaN
                padding = np.full(
                    (n_padding, remaining_data.shape[1]),
                    np.nan,
                    dtype=np.float32
                )
                padded_data = np.vstack([remaining_data, padding])
                
                #Pad each key in ehr feature dict
                padded_ehr = {}
                for ehr_key, ehr_val in remaining_ehr.items():
                    if isinstance(ehr_val, np.ndarray):
                        # Pad each array with appropriate fill value
                        if 'ratenorm' in ehr_key or 'norepi' in ehr_key or ehr_key in list(ehr_extractor.lab_map.values()):
                            pad_value = np.nan
                        else:
                            pad_value = 0

                        padding_array = np.full(n_padding, pad_value, dtype=ehr_val.dtype)
                        padded_ehr[ehr_key] = np.concatenate([ehr_val, padding_array])
                    else:
                        padded_ehr[ehr_key] = ehr_val
                #Add final chunk
                agg_ehr = {}
                for key, value_array in padded_ehr.items():
                    if np.all(np.isnan(value_array)):
                        #All nan
                        agg_ehr[key] = np.nan
                    else:
                        # Use nanmax to ignore NaN values
                        agg_ehr[key] = np.nanmax(value_array)
                writer.write_chunk(chunk_id, chunk_starttime, padded_data, signal_map, agg_ehr)
            print("Final chunk done")
            writer.close()

class H5ChunkWriter:
    """
    H5 structure with structured arrays per chunk:
    
    /file
    ├── waveforms (structured dataset)
    │   └── row 0: {ECG_II: [7500 samples], ABP: [7500 samples], ...}
    │   └── row 1: {ECG_II: [7500 samples], ABP: [7500 samples], ...}
    │
    ├── timestamps (structured dataset)
    │   └── row 0: {start_time: '2183-04-28T17:47:59', num_samples: 7500}
    │   └── row 1: {start_time: '2183-04-28T17:48:59', num_samples: 7500}
    │
    └── ehr_values (structured dataset)
        └── row 0: {norepinephrine_rate: 5.2, propofol_rate: 10.0, ...}
        └── row 1: {norepinephrine_rate: 5.5, propofol_rate: 10.5, ...}
    """
    
    def __init__(self, output_dir, subject_id, hadm_id, stay_id, record_id, target_signals, record_signals, total_chunks):
        self.filepath = f"{output_dir}/{subject_id}_{hadm_id}_{stay_id}_{record_id}.h5"
        self.subject_id = subject_id
        self.hadm_id = hadm_id
        self.stay_id = stay_id
        self.record_id = record_id
        self.target_signals = target_signals
        self.record_signals = record_signals
        self.total_chunks = total_chunks
        
        # Open or create H5 file
        self.h5_file = h5py.File(self.filepath, 'a')
        
        subject_group_name = f'subject_{subject_id}'
        if subject_group_name not in self.h5_file:
            self.subject_group = self.h5_file.create_group(subject_group_name)
        else:
            self.subject_group = self.h5_file[subject_group_name]

        #Hadm group
        hadm_group_name = f'hadm_{hadm_id}'
        if hadm_group_name not in self.subject_group:
            self.hadm_group = self.subject_group.create_group(hadm_group_name)
        else:
            self.hadm_group = self.subject_group[hadm_group_name]

        #Stay group (record_id = stay_id)
        stay_group_name = f'stay_{stay_id}'
        if stay_group_name not in self.hadm_group:
            self.stay_group = self.hadm_group.create_group(stay_group_name)
        else:
            self.stay_group = self.hadm_group[stay_group_name]
        
        record_group_name = f'record_{record_id}'
        if record_group_name not in self.stay_group:
            self.record_group = self.stay_group.create_group(record_group_name)
        else:
            self.record_group = self.stay_group[record_group_name]
        
        # Store metadata
        self.record_group.attrs['subject_id'] = subject_id
        self.record_group.attrs['hadm_id'] = hadm_id
        self.record_group.attrs['stay_id'] = stay_id
        self.record_group.attrs['record_id'] = record_id
        for signal in target_signals:
            if signal in record_signals:
                self.record_group.attrs[f'has_{signal}'] = 1
            else:
                self.record_group.attrs[f'has_{signal}'] = 0
        #self.record_group.attrs['created'] = datetime.now().isoformat()
        
        self.chunk_counter = 0
    
    def init_datasets(self, chunk_size):
        """
        Create structured datasets for waveforms, timestamps, and EHR values
        Call this once before writing any chunks
        """

        self.chunk_size = chunk_size
        
        # ============================================
        # 1. WAVEFORMS - structured array
        # ============================================
        self.waveform_datasets = {}
        for sig_name in self.record_signals:
            sig_name = SIGNAL_NAME_MAP.get(sig_name, None)
            if not sig_name:
                continue
            if sig_name not in self.record_group:
                self.waveform_datasets[sig_name] = self.record_group.create_dataset(
                    sig_name,
                    shape=(self.total_chunks, chunk_size),
                    dtype=np.float32,
                    compression='gzip',
                    compression_opts=4
                )
            else:
                self.waveform_datasets[sig_name] = self.record_group[sig_name]
       
        
        # ============================================
        # 2. TIMESTAMPS - structured array
        # ============================================
#         timestamp_dtype = np.dtype([
#             ('start_time', h5py.special_dtype(vlen=str)),
#             ('num_samples', np.int32)
#         ])
        
        if 'timestamps' not in self.record_group:
            self.timestamps_dataset = self.record_group.create_dataset(
                'timestamps',
                shape=(self.total_chunks,),
                dtype= h5py.string_dtype(encoding='utf-8'),
#                 compression='gzip'
            )
        else:
            self.timestamps_dataset = self.record_group['timestamps']
        
        # ============================================
        # 3. EHR VALUES - structured array
        # ============================================
        # Build dtype from EHR features dynamically
        ehr_dtype_list = []

        #Add meds features
        for med_name in ALL_MEDS:
            if med_name not in SPARSE_MEDS:
                ehr_dtype_list.append((f'{med_name}_ratenorm', np.float32))
            ehr_dtype_list.append((f'{med_name}_bolus', np.int32))
            ehr_dtype_list.append((f'{med_name}_on', np.int32))
        #Add derived features
        ehr_dtype_list.extend([
            ('norepi_eq', np.float32),
            ('pressor_on', np.int32),
            ('dilator_on', np.int32),
        ])
        
        # Add labs
        for lab_name in LAB_MAP:
            ehr_dtype_list.append((lab_name, np.float32))

        # Add icd codes
        for code in CODES:
            ehr_dtype_list.append((code, np.int32))

        ehr_dtype = np.dtype(ehr_dtype_list)
        
        if 'ehr_values' not in self.record_group:
            self.ehr_dataset = self.record_group.create_dataset(
                'ehr_values',
                shape=(self.total_chunks,),
                dtype=ehr_dtype,
                compression='gzip'
            )
        else:
            self.ehr_dataset = self.record_group['ehr_values']
    
    def write_chunk(self, chunk_id, timestamp, signal_data, signal_map, ehr_dict):
        """
        Write one 60s chunk
        
        Args:
            chunk_id: chunk number
            timestamps: (n_samples,) array of datetime objects
            signal_data: (n_samples, n_signals) numpy array
            ehr_dict: {feature_name: scalar_value, ...}
        """
        
        if self.chunk_counter == 0:
            # Initialize on first write
            self.init_datasets(signal_data.shape[0])
            
#         print(f"\nChunk {self.chunk_counter} incoming data:")
#         print(f"  signal_data shape: {signal_data.shape}")
#         print(f"  signal_data dtype: {signal_data.dtype}")
#         print(f"  signal_data min/max: {np.nanmin(signal_data):.4f} / {np.nanmax(signal_data):.4f}")
#         print(f"  signal_data sum: {np.sum(signal_data):.4f}")
#         print(f"  signal_data[0:5, 0] (first signal, first 5 samples): {signal_data[0:5, 0]}")
        
        # ============================================
        # Write waveforms (one row = one 60s chunk)
        # ============================================
        for name_key, col_val in signal_map.items():
            self.waveform_datasets[name_key][self.chunk_counter] = signal_data[:, col_val]

#             sig_col = signal_data[:, col_idx]
            
#             waveform_row[sig_name] = sig_col.copy()
            
#             written_back = waveform_row[sig_name]
        
        # ============================================
        # Write timestamps metadata
        # ============================================
        self.timestamps_dataset[self.chunk_counter] = str(timestamp)
        #self.timestamps_dataset['start_time', self.chunk_counter] = str(timestamps[0])
        #self.timestamps_dataset['num_samples', self.chunk_counter] = len(timestamps)
        
        # ============================================
        # Write EHR values (one row = one 60s chunk)
        # ============================================
        ehr_row = self.ehr_dataset[self.chunk_counter]
        for ehr_key, ehr_val in ehr_dict.items():
            if ehr_key not in ehr_row.dtype.names:
                continue
            dtype = ehr_row.dtype.fields[ehr_key][0]
            
            if dtype == np.float32:
                #Floats: keep NaN
                self.ehr_dataset[ehr_key, self.chunk_counter] = ehr_val
            else:
                #Integers: use 0 for missing
                self.ehr_dataset[ehr_key, self.chunk_counter] = ehr_val if pd.notna(ehr_val) else 0
        
        self.chunk_counter += 1
#         self.h5_file.flush()
    
    def close(self):
        self.h5_file.flush()
        self.h5_file.close()