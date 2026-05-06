import wfdb
import pandas as pd
import os
import datetime
from utils.constants import TARGET_SIGNALS, SIGNAL_NAME_MAP

def get_physionet_dirs(physionet_dir):
    """
    Returns: Sub directories for physionet url path as list 
    """
    # Top-level gives you the 00/, 01/, ... subdirs
    dirs = wfdb.get_record_list(physionet_dir)
    return dirs

def format_meta(df, signals):
    # convert sample counts to recording hours and clean up column typing
    df = df.rename(columns={'which_mimic': 'mimic'})
    int_cols = ['mimic'] + [col for col in df.columns if col.endswith('id')]
    for col in int_cols:
        df[col] = pd.to_numeric(df[col]).astype('Int32')

    #convert n_samples to signal hours
    sample_cols = [col for col in df.columns if col.endswith('_samples')]
    for col in sample_cols:
        hr_col = col.replace('_samples', '_hrs')
        df[hr_col] = (df[col] / df['fs'] / 3600).round(1)

    df['start_timestamp'] = pd.to_datetime(df['start_timestamp'])
    df['end_timestamp'] = df['start_timestamp'] + pd.to_timedelta(df['record_len'] / df['fs'], unit='s')
    df['record_hrs'] = (df['record_len'] / df['fs'] / 3600).round(1)

    df = df[int_cols + ['fs', 'start_timestamp', 'end_timestamp', 'record_hrs'] + [col for col in df.columns if col.endswith('_hrs')]]

    return df

class PhysioNet:
    def __init__(self, which_mimic):
        self.which_mimic = which_mimic
        if which_mimic == 3:
            self.root_path = 'mimic3wdb-matched/1.0'
        elif which_mimic == 4:
            self.root_path = 'mimic4wdb/0.1.0'
        
    


    def extract_metadata(self):
        '''
        Reads master, layout, and segment .hea PhysioNet metadata and gets signal sample count
        
        :param self: Description
        '''
        dirs = get_physionet_dirs(self.root_path)
        meta_list = []
        count = 0
        #list of subject level dictionaries containing list of values per key corresponding to different recordings
        for subject_path in dirs: #Like waves/p100/p10014354/
            subject_id = subject_path.split('/')[-2][1:]
            subject_dir = os.path.join(self.root_path, subject_path)
            
            #MIMIC-III
            if self.which_mimic == 3:
                record_paths = wfdb.get_record_list(subject_dir)
                master_headers = [s for s in record_paths if s[0] == 'p' and s[-1] != 'n'] # O(len(record_paths)) 

                for header in master_headers:
                    if count % 5000 == 0: 
                        print(f'{count} - at {subject_dir}')
                        pd.DataFrame(meta_list).to_csv('/Users/troysisson/project/jlab/data/m3_checkpoint.csv', index=False)
                    try:
                        master_header = wfdb.rdheader(header, pn_dir=subject_dir)
                    except Exception as e:
                        print(f"Failed to read header {header}: {e}")
                        continue
                    metadata = {}

                    ###FROM MASTER GET total recording samples,  
                    master_header = wfdb.rdheader(header, pn_dir=subject_dir)
                    count += 1

                    segments = master_header.seg_name
                    segment_lengths = master_header.seg_len
                    fs = master_header.fs
                    total_samples = master_header.sig_len
                    record_len = total_samples
                    start_timestamp = datetime.datetime.combine(master_header.base_date, master_header.base_time)

                    layout = segments[0] #'3544749_layout'
                    record_id = layout.split('_')[0]
                    
                    
                    
                    
                    layout_header = wfdb.rdheader(layout, pn_dir=subject_dir)
                    signal_names = layout_header.sig_name
                    #IF there is no layout.hea, skip the recording
                    #Check if layout is None
                    if not signal_names:
                        print(f'Skipping {subject_dir} record {record_id}: Empty layout header')
                        continue
                    record_signals = [SIGNAL_NAME_MAP[name] for name in signal_names if name in SIGNAL_NAME_MAP]
                    #IF there are no target signal, skip the recording
                    if not record_signals:
                        print(f'Skipping {subject_dir} record {record_id}: No target signals')
                        continue
                    #Initialize signal counts for record
                    signal_samples = {}
                    for signal in record_signals:
                        signal_samples[signal] = 0
                    
                    for j, segment in enumerate(segments):
                        if j == 0: #skip layout segment
                            continue
                        if segment == '~': #Segment sample gap
                            total_samples -= segment_lengths[j]
                            continue
                        try:
                            segment_header = wfdb.rdheader(segment, pn_dir=subject_dir)
                            sig_names = segment_header.sig_name
                            if not sig_names:
                                total_samples -= segment_lengths[j]
                                continue
                            segment_signals = [SIGNAL_NAME_MAP[name] for name in sig_names if name in SIGNAL_NAME_MAP]
                            
                            for signal in segment_signals:
                                signal_samples[signal] += segment_header.sig_len
                        except Exception as e:
                            # print(f"Error reading segment {segment}: {e}")
                            total_samples -= segment_lengths[j]
                            continue

                    #Fill dict
                    metadata['which_mimic'] = int(self.which_mimic)
                    metadata['subject_id'] = subject_id
                    #metadata['hadm_id'] = [hadm_id]
                    metadata['record_id'] = record_id
                    metadata['fs'] = fs
                    metadata['record_len'] = record_len
                    metadata['start_timestamp'] = start_timestamp
                    metadata['total_samples'] = total_samples
                    for signal in TARGET_SIGNALS:
                        samples = signal_samples.get(signal, None)
                        if samples:
                            metadata[f'{signal}_samples'] = samples
                        else:
                            metadata[f'{signal}_samples'] = 0
            
            #MIMIC-IV
            elif self.which_mimic == 4:
                record_paths = get_physionet_dirs(subject_dir)
                
                for record_path in record_paths: #Like 81739927/81739927
                    record_path = record_path.split('/')[0]
                    record_dir = subject_dir + record_path
                    master_header = wfdb.rdheader(record_path, pn_dir=record_dir)
                    #print(master_header.__dict__)
                    metadata = {}
                    
                    segments = master_header.seg_name
                    segment_lengths = master_header.seg_len

                    layout = segments[0] #'3544749_0000'
                    record_id = layout.split('_')[0]
                    fs = master_header.fs
                    start_timestamp = datetime.datetime.combine(master_header.base_date, master_header.base_time)
                    total_samples = master_header.sig_len
                    record_len = total_samples
                    layout_header = wfdb.rdheader(layout, pn_dir=record_dir)
                    signal_names = layout_header.sig_name
                    #Check if layout is None
                    if not signal_names:
                        print(f'Skipping {subject_dir} record {record_id}: Empty layout header')
                        continue
                    record_signals = [SIGNAL_NAME_MAP[name] for name in signal_names if name in SIGNAL_NAME_MAP]
                    if not record_signals:
                        print(f'Skipping {subject_dir} record {record_id}: No target signals')
                        continue
                    #Initialize signal counts for record
                    signal_samples = {}
                    for signal in record_signals:
                        signal_samples[signal] = 0
                    
                    for j, segment in enumerate(segments):
                        if j == 0: #skip layout segment
                            continue
                        if segment == '~': #Segment sample gap
                            total_samples -= segment_lengths[j]
                            continue
                        try:
                            segment_header = wfdb.rdheader(segment, pn_dir=record_dir)
                            sig_names = segment_header.sig_name
                            if not sig_names:
                                total_samples -= segment_lengths[j]
                                continue
                            segment_signals = [SIGNAL_NAME_MAP[name] for name in sig_names if name in SIGNAL_NAME_MAP]
                            
                            for signal in segment_signals:
                                signal_samples[signal] += segment_header.sig_len
                        except Exception as e:
                            # print(f"Error reading segment {segment}: {e}")
                            total_samples -= segment_lengths[j]
                            continue

                    hadm_id = None
                    comments = master_header.comments
                    if comments:
                        for comment in comments:
                            # if 'subject_id' in comment:
                            #     subject_id = comments.split(' ')[-1]
                            if 'hadm_id' in comment:
                                hadm_id = comment.split(' ')[-1]

                    record_id = master_header.record_name
                    fs = master_header.fs
                    start_timestamp = datetime.datetime.combine(master_header.base_date, master_header.base_time)
                    
                    
                    #Fill dict
                    metadata['which_mimic'] = self.which_mimic
                    metadata['subject_id'] = subject_id
                    metadata['hadm_id'] = hadm_id
                    metadata['record_id'] = record_id
                    metadata['fs'] = fs
                    metadata['record_len'] = record_len
                    metadata['start_timestamp'] = start_timestamp
                    metadata['total_samples'] = total_samples
                    for signal in TARGET_SIGNALS:
                        samples = signal_samples.get(signal, None)
                        if samples:
                            metadata[f'{signal}_samples'] = samples
                        else:
                            metadata[f'{signal}_samples'] = 0
                    
            meta_list.append(metadata)

        return meta_list
