import pandas as pd
from preprocessing.wave_meta import WaveMeta
from preprocessing.demographics import get_demographics
from preprocessing.weight import get_stay_weight
from preprocessing.labs import get_labs
from preprocessing.inputs import get_inputs
from preprocessing.icd import get_icd
from preprocessing.cohort import match_icustays, filter_signal
from utils.utils import load_tbl, build_dirs

class MIMIC_EHR:
    def __init__(self, config, mimic):
        self.mimic = mimic
        self.config = config
        self.dirs = build_dirs(config, mimic)
        self.signals = config['signals']

    def _load(self, filename, source='derived'):
        return load_tbl(filename, source, self.dirs)
    
    def save(self):
        path = self.dirs['derived']
        pd.to_csv(self.inputs,f'{path}mimic{self.mimic}_inputs')
        pd.to_csv(self.labs, f'{path}mimic{self.mimic}_labs')
        pd.to_csv(self.icd, f'{path}mimic{self.mimic}_icd')


    def preprocess(self, verbose=True):
        icustays = self._load('ICUSTAYS.csv.gz', source='icu')
        admissions = self._load('ADMISSIONS.csv.gz', source='hosp')
        patients = self._load('PATIENTS.csv.gz', source='hosp', )
        wave_meta = self._load(f'wavemeta_mimic{self.mimic}.csv')
        
        # 1) Match recordings to icustays and filter signal
        icu_cohort = match_icustays(wave_meta, icustays)
        ppg_ii_cohort = filter_signal(icu_cohort)
        # 2) Add 'age', 'age_group', 'gender', 'ethnicity', 'ethnicity_group', cols
        cohort = get_demographics(ppg_ii_cohort, admissions, patients)
        # 3) Add weight (kg) -> 'patientweight' col and
        cohort = get_stay_weight(self.dirs, cohort)
        # filter on valid patientweight
        weights_cohort = cohort[~cohort['patientweight'].isna()]
              
        # labs, inputs, icd
        self.labs = get_labs(self.dirs, weights_cohort)
        self.inputs = get_inputs(self.dirs, weights_cohort)
        self.icd = get_icd(self.dirs, weights_cohort)

        if verbose:
            print(f'''
            -------------
            ICU matched (having either ii OR ppg): {icu_cohort.shape[0]} records
            With at least 30 min of ii AND ppg: {ppg_ii_cohort.shape[0]} records
            With valid weight: {weights_cohort['subject_id'].shape[0]} records
            Final cohort subjects: {weights_cohort['subject_id'].nunique()} subjects
            Records with labs: {self.labs['record_id'].nunique()} records
            Records with inputs: {self.inputs['record_id'].nunique()} records
            Records with icd: {self.icd['record_id'].nunique()} records
            MIMIC{self.mimic} preprocessed
            ''')

#Extract waveform metadata from Physionet database



