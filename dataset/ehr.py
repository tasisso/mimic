import pandas as pd
import numpy as np
from preprocessing.wave_meta import WaveMeta
from preprocessing.demographics import get_demographics
from preprocessing.weight import get_stay_weight
from preprocessing.labs import get_labs
from preprocessing.inputs import get_inputs
from preprocessing.icd import get_icd
from preprocessing.cohort import match_icustays, filter_signal
from utils.utils import load_tbl, build_dirs

from utils.constants import DEFAULT_CHUNK_DURATION, MEDS, MED_CATEGORIES, MED_MAP, TARGET_SIGNALS, LAB_MAP

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
        path = self.dirs['derived']
        self.inputs.to_csv(f'{path}mimic{self.mimic}_inputs.csv', index=False)
        self.labs.to_csv(f'{path}mimic{self.mimic}_labs.csv', index=False)
        self.icd.to_csv(f'{path}mimic{self.mimic}_icd.csv', index=False)
        self.cohort.to_csv(f'{path}mimic{self.mimic}_cohort.csv', index=False)


    def preprocess(self, verbose=True):
        icustays = self._load('ICUSTAYS.csv.gz', source='icu')
        admissions = self._load('ADMISSIONS.csv.gz', source='hosp')
        patients = self._load('PATIENTS.csv.gz', source='hosp', )
        wave_meta = self._load(f'wavemeta_mimic{self.mimic}.csv', index_col=0)
        
        # 1) Match recordings to icustays 
        cohort = match_icustays(wave_meta, icustays)
        # 2) Add 'age', 'age_group', 'gender', 'ethnicity', 'ethnicity_group', cols
        cohort = get_demographics(cohort, admissions, patients)
        # 3) Add weight (kg) -> 'patientweight' col and
        cohort = get_stay_weight(self.dirs, cohort)

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
        self.med_categories = MED_CATEGORIES
        self.medication_map = MED_MAP
        self.all_meds = list(MEDS.keys())
        self.lab_map = LAB_MAP

        inputs = inputs.copy()
        inputs['starttime'] = pd.to_datetime(inputs['starttime'])
        inputs['endtime'] = pd.to_datetime(inputs['endtime'])
        labs = labs.copy()
        labs['charttime'] = pd.to_datetime(labs['charttime'])

        # Build indexes
        self.inputs_by_stay = inputs.groupby('stay_id')
        self.labs_by_stay = labs.groupby(['subject_id', 'hadm_id'])
        self.codes_by_stay = codes.groupby(['subject_id', 'hadm_id'])

    def get_features(self, subject_id, hadm_id, stay_id, patientweight, chunk_starttime):
        chunk_start = pd.Timestamp(chunk_starttime)
        chunk_end = chunk_start + pd.Timedelta(seconds=DEFAULT_CHUNK_DURATION)

        #ehr = self._init_ehr()
        ehr = self._fill_meds(ehr, stay_id, patientweight, chunk_start, chunk_end)
        ehr = self._fill_labs(ehr, subject_id, hadm_id, chunk_start, chunk_end)
        
        return ehr

    # def _init_ehr(self, subject_id, hadm_id, stay_id) -> dict:
    #     ehr = {}
    #     try:
    #         med_labels = self.inputs_by_stay.get_group(stay_id)['label'].unique()
    #     except KeyError:
    #         med_labels = []
    #         print(f'No inputs of interest for this stay_id:{stay_id}')
    #     stay_meds = set(
    #         MED_MAP[label][0] for label in med_labels if label in MED_MAP
    #     )
    #     try:
    #         lab_labels = self.labs_by_stay.gget_group((subject_id, hadm_id))['label'].unique()
    #     except KeyError:
    #         lab_labels = []
    #         print(f'No labs of interest for this (subject,hadm):{(subject_id, hadm_id)}')
    #     stay_labs = set(
    #         MED_MAP[label][0] for label in lab_labels if label in LAB_MAP
    #     )
    #     for category in self.med_categories:
    #         ehr[f'{category}_on'] = 0
    #     for med in stay_meds:
    #         ehr[f'{med}_ratenorm'] = np.nan
    #         ehr[f'{med}_bolus'] = 0
    #         ehr[f'{med}_on'] = 0
    #     for lab in stay_labs:
    #         ehr[lab] = np.nan
        
    #     #Derived 
    #     ehr['norepi_eq'] = 0.0
    #     ehr['vasoactive_on'] = 0
    #     return ehr

    def _fill_meds(self, ehr, stay_id, weight, chunk_start, chunk_end):
        try:
            stay_inputs = self.inputs_by_stay.get_group(stay_id)
        except KeyError:
            return ehr

        chunk_inputs = stay_inputs[
            (stay_inputs['starttime'] < chunk_end) &
            (stay_inputs['endtime'] > chunk_start)
        ]

        for _, row in chunk_inputs.iterrows():
            med_name, category = self.medication_map.get(row['label'])
            if not med_name:
                continue

            rate = row['rate']
            rateuom = row['rateuom']
            amount = row['amount']
            route = row['ordercategorydescription']

            #For titration, take maximum rate within chunk window
            if pd.notna(rate) and route in ('Continuous Med', 'Continuous IV'):
                rate_norm = self.normalize_rate(rate, rateuom, weight)
                if pd.notna(rate_norm):
                    current = ehr[f'{med_name}_ratenorm']
                    ehr[f'{med_name}_ratenorm'] = rate_norm if np.isnan(current) else max(current, rate_norm)

            if pd.notna(amount) and route in ('Bolus', 'Drug Push'):
                ehr[f'{med_name}_bolus'] = 1

            if pd.notna(amount) or pd.notna(rate):
                ehr[f'{category}_on'] = 1
                ehr[f'{med_name}_on'] = 1

        ehr = self._add_derived(ehr)
        return ehr

    def _fill_labs(self, ehr, subject_id, hadm_id, chunk_start, chunk_end):
        try:
            stay_labs = self.labs_by_stay.get_group((subject_id, hadm_id))
        except KeyError:
            return ehr

        chunk_labs = stay_labs[
            (stay_labs['charttime'] >= chunk_start) &
            (stay_labs['charttime'] <  chunk_end)
        ]
        for _, row in chunk_labs.iterrows():
            lab_name = self.lab_map.get(row['label'])
            if lab_name and pd.notna(row['valuenum']):
                ehr[lab_name] = row['valuenum']
        return ehr

    def get_codes(self, subject_id, hadm_id):
        try:
            stay_codes = self.codes_by_stay.get_group((subject_id, hadm_id))
        except KeyError:
            return []

        present = stay_codes['icd10_code'].values
        
        return present

    @staticmethod
    def normalize_rate(self, rate, rate_uom, weight):
        if pd.isna(rate) or pd.isna(weight):
            return np.nan
        conversions = {
            'mcg/kg/min': lambda r: r,
            'ng/kg/min':  lambda r: r / 1000,
            'mg/kg/hour': lambda r: r * 1000 / 60,
            'mcg/hour':   lambda r: r / weight / 60,
            'mg/hour':    lambda r: r * 1000 / weight / 60,
            'mg/min':     lambda r: r * 1000 / weight,
            'units/hour': lambda r: r / 60,
        }
        fn = conversions.get(rate_uom)
        if fn is None:
            print(f"Unhandled rate UOM: {rate_uom}")
            return rate
        return fn(rate)

    @staticmethod
    def _add_derived(ehr):
        #Compute norepinephrine equalivent and vasoactive_on flag 
        NEE_FACTORS = {
            'norepinephrine': 1.0,
            'epinephrine': 1.0,
            'dopamine': 0.01,
            'phenylephrine': 0.06,
            'vasopressin': 2.5,
            'angiotensin_ii': 2.5,   # 0.0025 ng/kg/min so 1000 ng->mcg
        }
        norepi_eq = 0.0
        for drug, factor in NEE_FACTORS.items():
            norepi_eq  += np.nan_to_num(ehr[f'{drug}_ratenorm'], nan=0.0) * factor

        ehr['norepi_eq']  = norepi_eq
        if ehr['vasopressor_on'] or ehr['vasodilator_on']:
            ehr['vasoactive_on'] = 1

        return ehr

