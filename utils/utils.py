import pandas as pd
import os
import yaml

def load_config(path='../configs/config.yaml'):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def load_tbl(filename, source, dirs, **kwargs):
    path = get_path(filename, source, dirs)
    df = pd.read_csv(path, **kwargs)
    df.columns = df.columns.str.lower() #mimic3 columns are all caps
    return df

def get_path(filename, source, dirs):
    '''find file path handling case differences between mimic versions'''
    if source not in dirs:
        raise KeyError(f'source {source} not in dirs: {list(dirs.keys())}')
    
    dir = dirs[source]
    stem, *exts = filename.split('.')
    ext = '.' + '.'.join(exts)
    
    for name in [filename, stem.lower() + ext]:
        path = os.path.join(dir, name)
        if os.path.exists(path):
            return path
    
    raise FileNotFoundError(f'{filename} not found in {dir}')


def build_dirs(config, mimic):
    '''build directory lookup dict from config'''
    clinical_root = config['paths'][mimic]['clinical_root']
    
    if mimic == 3:
        icu_dir = hosp_dir = clinical_root
    else:
        icu_dir = os.path.join(clinical_root, 'icu')
        hosp_dir = os.path.join(clinical_root, 'hosp')
    
    return {
        'derived': config['paths']['derived'],
        'icu': icu_dir,
        'hosp': hosp_dir,
        'waveforms': config['paths'][mimic]['waveforms_root']
    }