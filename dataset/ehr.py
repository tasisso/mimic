import pandas as pd
import numpy as np
from preprocessing.wave_meta import PhysioNet
from preprocessing.demographics import get_demographics
from preprocessing.biometrics import get_stay_weight_height
from preprocessing.labs import get_labs
from preprocessing.inputs import get_inputs
from preprocessing.icd import get_icd
from preprocessing.cohort import match_icustays
from utils.utils import load_tbl, build_dirs

from utils.constants import DEFAULT_CHUNK_DURATION, VASOACTIVE

class MIMIC_EHR:
    '''
    Preprocesses raw MIMIC tables and saves normalized tables per MIMIC version.
        Steps:
            1) Join icustays table with extracted waveform metadata on subject_ids and filter
                by overlapping (waveform, icustay) timestamps
            2) Add cohort demographic features and weight
            3) Normalize labs, inputs, icd tables based on this cohort
            4) Save intermediate tables for faster lookup when building dataset from cohort rows
    '''
    def __init__(self, config, mimic):
        self.mimic = mimic
        self.config = config
        self.dirs = build_dirs(config, mimic)

    def _load(self, filename, source='derived', **kwargs):
        return load_tbl(filename, source, self.dirs, **kwargs)
    
    def save(self):
        path = self.config['paths'][f'mimic{self.mimic}']['derived']
        self.inputs.to_csv(f'{path}/inputs.csv', index=False)
        self.labs.to_csv(f'{path}/labs.csv', index=False)
        self.icd.to_csv(f'{path}/icd.csv', index=False)
        self.cohort.to_csv(f'{path}/cohort.csv', index=False)


    def preprocess(self, verbose=True):
        icustays = self._load('ICUSTAYS.csv.gz', source='icu')
        admissions = self._load('ADMISSIONS.csv.gz', source='hosp')
        patients = self._load('PATIENTS.csv.gz', source='hosp', )
        wave_meta = self._load('wavemeta.csv', index_col=0)
        
        # 1) Match recordings to icustays 
        cohort = match_icustays(wave_meta, icustays)
        # 2) Add 'age', 'age_group', 'gender', 'ethnicity', 'ethnicity_group', cols
        cohort = get_demographics(cohort, admissions, patients)
        # 3) Add weight (kg) -> 'patientweight' col and
        cohort = get_stay_weight_height(self.dirs, cohort)
        self.cohort = cohort
        # labs, inputs, icd
        self.labs = get_labs(self.dirs, cohort)
        self.inputs = get_inputs(self.dirs, cohort)
        self.icd = get_icd(self.config, self.dirs, cohort)

        if verbose:
            print(f'''
            -------------
            Total records: {cohort['record_id'].nunique()}
            Records with labs: {self.labs['record_id'].nunique()} records
            Records with inputs: {self.inputs['record_id'].nunique()} records
            Subjects with icd: {self.icd['subject_id'].nunique()} subjects
            MIMIC{self.mimic} preprocessed
            ''')


class ehrExtractor:
    """
    Efficient per-chunk EHR feature extraction, indexed by stay_id at construction time.
    """

    def __init__(self, inputs, labs, codes):

        inputs = inputs.copy()
        inputs['starttime'] = pd.to_datetime(inputs['starttime'])
        inputs['endtime'] = pd.to_datetime(inputs['endtime'])
        labs = labs[labs['valuenum'].notna()].copy()
        labs['charttime'] = pd.to_datetime(labs['charttime'])

        #Raw dataframes
        self.inputs_by_stay = inputs.groupby('stay_id')
        self.labs_by_stay = labs.groupby(['subject_id', 'hadm_id'])
        self.codes_by_stay = codes.groupby(['subject_id', 'hadm_id'])

        #Lab and med features      
        self.stay_input_labels = {} 
        for stay_id, group in self.inputs_by_stay:
            continuous = group[group['ordercategorydescription'].isin(['Continuous Med', 'Continuous IV'])]

            meds_with_rate = set(continuous['med_name'].unique())
            meds_on_only = set(group['med_name'].unique()) - meds_with_rate
            
            # cats = set(
            #     group['resolved_categories'].explode().unique()
            # )
            cats = set(
                group['categories_str']
                .str.split('|')
                .explode()
                .unique()
            )
            if cats & VASOACTIVE:
                cats.add('vasoactive')

            self.stay_input_labels[stay_id] = (meds_with_rate, meds_on_only, cats)

        self.stay_lab_labels = {}
        for (subject_id, hadm_id), group in self.labs_by_stay:
            labs = set(group['lab_name'].unique())
            self.stay_lab_labels[(subject_id, hadm_id)] = labs
        
        #feature dict
        self.ehr = {}

    def get_features(self, subject_id, hadm_id, stay_id, chunk_starttime, lookback):
        chunk_start = pd.Timestamp(chunk_starttime)
        chunk_end = chunk_start + pd.Timedelta(seconds=DEFAULT_CHUNK_DURATION)
        _, _, stay_cats = self.stay_input_labels.get(stay_id, (set(), set(), set()))
        stay_labs = self.stay_lab_labels.get((subject_id, hadm_id), set())
        ehr = {}
        if len(stay_cats) > 0:
            ehr = self._fill_meds(ehr, stay_id, chunk_start, chunk_end)
        if len(stay_labs) > 0:
            ehr = self._fill_labs(ehr, subject_id, hadm_id, chunk_start, chunk_end, lookback)
        
        return ehr
    
    def get_codes(self, subject_id, hadm_id):
        try:
            stay_codes = self.codes_by_stay.get_group((subject_id, hadm_id))
            icd9 = stay_codes[stay_codes['icd_version'] == 9]['vers_code'].dropna().tolist()
            icd10 = stay_codes[stay_codes['icd_version'] == 10]['icd10_code'].dropna().tolist()
            icd10_truncated = set({code[:3] for code in icd10 if len(code) >= 3})
            return icd9, icd10, icd10_truncated
        except KeyError:
            return [], [], []


    def _fill_meds(self, ehr, stay_id, chunk_start, chunk_end):
        stay_inputs = self.inputs_by_stay.get_group(stay_id)

        chunk_inputs = stay_inputs[
            (stay_inputs['starttime'] <= chunk_end) &
            (stay_inputs['endtime'] > chunk_start)
        ]

        for _, row in chunk_inputs.iterrows():

            med_name = row['med_name']

            rate = row['rate']
            rate_norm = row['ratenorm']
            amount = row['amount']
            categories = row['categories_str'].split('|')

            #For titration, take maximum rate within chunk window
            if pd.notna(rate_norm) and rate_norm > 0:
                current = ehr.get(f'{med_name}_ratenorm', np.nan)
                ehr[f'{med_name}_ratenorm'] = rate_norm if np.isnan(current) else max(current, rate_norm)

            if (pd.notna(amount) and amount > 0) or (pd.notna(rate) and rate > 0):
                ehr[f'{med_name}_on'] = 1
                for category in categories:
                    ehr[f'{category}_on'] = 1

        ehr = self._add_derived(ehr)
        return ehr

    def _fill_labs(self, ehr, subject_id, hadm_id, chunk_start, chunk_end, lookback=None):

        stay_labs = self.labs_by_stay.get_group((subject_id, hadm_id))

        window_start = lookback if lookback else chunk_start
        chunk_labs = stay_labs[
            (stay_labs['charttime'] >= window_start) &
            (stay_labs['charttime'] <  chunk_end)
        ].sort_values('charttime').groupby('label').last().reset_index()
        for _, row in chunk_labs.iterrows():

            lab = row['lab_name']
            if pd.notna(row['valuenum']):
                ehr[lab] = row['valuenum']
                ehr[f"{lab}_abnorm"] = 1 if row['flag'] == 'abnormal' else 0

        return ehr
    # def normalize_rate(rate, rate_uom, weight):
    #     if pd.isna(rate) or pd.isna(weight):
    #         return np.nan
    #     conversions = {
    #         'mcg/kg/min': lambda r: r,
    #         'mcgkgmin':   lambda r: r,
    #         'mcg/kg/hour': lambda r: r / 60,
    #         'mcgkghr':    lambda r: r / 60,
    #         'mcgmin':     lambda r: r / weight,
    #         'mcg/min':    lambda r: r / weight,
    #         'mcg/hour':   lambda r: r / weight / 60,
    #         'mcghr':      lambda r: r / weight / 60,
    #         'ng/kg/min':  lambda r: r / 1000,

    #         'mg/kg/hour': lambda r: r * 1000 / 60,
    #         'mgkghr':     lambda r: r * 1000 / 60,
    #         'mg/hour':    lambda r: r * 1000 / weight / 60,
    #         'mghr':       lambda r: r * 1000 / weight / 60,
    #         'mg/min':     lambda r: r * 1000 / weight,
    #         'mgmin':      lambda r: r * 1000 / weight,

    #         'gm/min':     lambda r: r * 1e6 / weight,
    #         'grams/min':  lambda r: r * 1e6 / weight,
    #         'grams/hour': lambda r: r * 1e6 / weight / 60,

    #         #Units measurements not weight-normalized
    #         'units/hour': lambda r: r / 60,
    #         'Uhr':        lambda r: r / 60,
    #         'units/min':  lambda r: r,
    #         'Umin':       lambda r: r,

    #         # ml/min — Fentanyl Base (MIMIC-III)
    #         'ml/min': lambda r: r * 50 / weight # mcg/min, assuming 50 mcg/ml concentration
    #     }
    #     fn = conversions.get(rate_uom)
    #     if fn is None:
    #         print(f"Unhandled rate UOM: {rate_uom}")
    #         return np.nan
    #     return fn(rate)


    @staticmethod
    def _add_derived(ehr):
        if any(ehr.get(f'{cat}_on') == 1 for cat in VASOACTIVE):
            ehr['vasoactive_on'] = 1
        
        #Compute norepinephrine equalivent and vasoactive_on flag 
        NEE_FACTORS = {
            'norepinephrine': 1.0,
            'epinephrine': 1.0,
            'dopamine': 0.01,
            'phenylephrine': 0.06,
            'vasopressin': 2.5,
            'angiotensin_ii': 2.5,   # 0.0025 ng/kg/min so 1000 ng->mcg
        }
        norepi_eq = sum(
            np.nan_to_num(ehr[f'{drug}_ratenorm'], nan=0.0) * factor
            for drug, factor in NEE_FACTORS.items()
            if f'{drug}_ratenorm' in ehr
        )
        ehr['norepi_eq'] = norepi_eq if norepi_eq > 0 else np.nan

        return ehr

