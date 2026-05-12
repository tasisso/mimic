import numpy as np
import pandas as pd
from scipy.signal import resample
from utils.constants import DEFAULT_CHUNK_DURATION, MED_MAP, TARGET_FS, SIGNAL_NAME_MAP, FINAL_SIGNALS
import datetime


class ehrExtractor:
    """
    EHR feature extraction indexed by stay_id
    """
    
    def __init__(self, cohort, inputs, labs, codes, to_csv=False):
        """
        Load CSVs and build stay_id indexes
        """
        self.mimic = cohort['mimic'].iloc[0]
        # Load full CSVs
        self.cohort = cohort
        self.inputs = inputs
        self.labs = labs
        self.codes = codes
        
        # Convert timestamps to datetime
        self.inputs['starttime'] = pd.to_datetime(self.inputs['starttime'])
        self.inputs['endtime'] = pd.to_datetime(self.inputs['endtime'])
        self.labs['charttime'] = pd.to_datetime(self.labs['charttime'])
        
        # Build stay_id indexes (single pass)
        
        self.inputs_by_stay = self.inputs.groupby('stay_id')
        self.labs_by_stay = self.labs.groupby(['subject_id', 'hadm_id'])
        self.codes_by_stay = self.codes.groupby(['subject_id', 'hadm_id'])
        
        self.medication_map = MED_MAP

        #Medication colunms to drop after adding derived features
        self.sparse_meds = SPARSE_MEDS
        
        #All medications
        self.all_meds = ALL_MEDS

        self.lab_map = LAB_MAP
    
    def get_features(self, subject_id, hadm_id, stay_id, patientweight, chunk_starttime):
        """
        Extract EHR features for one 60s chunk
        
        Args:
            stay_id: stay identifier
            timestamps: continuous timestamps for chunk
        
        Returns:
            ehr_dict: {feature_name: value}
        """
        #Initialize 60 seconds worth of features
        n_samples = TARGET_FS * DEFAULT_CHUNK_DURATION
        
        ehr_dict = {}
        
        #MEDS FEATURES
        for med_name in self.all_meds:
            ehr_dict[f'{med_name}_ratenorm'] = np.full(n_samples, np.nan, dtype=np.float32)
            ehr_dict[f'{med_name}_bolus'] = np.zeros(n_samples, dtype=np.int32)
            ehr_dict[f'{med_name}_on'] = np.zeros(n_samples, dtype=np.int32)
        
        #Get weight for ratenorm
        weight = patientweight

        #LABS FEATURES
        for lab_name in self.lab_map.values():
            ehr_dict[f'{lab_name}'] = np.full(n_samples, np.nan, dtype=np.float32)
        
        #CODE FEATURES
        for code in self.codes:
            ehr_dict[f'{code}'] = np.zeros(n_samples, dtype=np.int32)
        
        # ============================================
        # INPUTS (medications)
        # ============================================
        chunk_start = chunk_starttime
        chunk_end = chunk_start + pd.Timedelta(seconds=60)
        try:
            stay_inputs = self.inputs_by_stay.get_group(stay_id)
            chunk_inputs = stay_inputs[
                (stay_inputs['starttime'] >= chunk_start) &
                (stay_inputs['endtime'] < chunk_end)
            ]
            #Look for inputs for this 60s
            if len(chunk_inputs) > 0:

                for _, row in chunk_inputs.iterrows():
                    med_label = row['label']
                    #Map medication label to standardized name
                    if med_label not in self.medication_map:
                        continue
                    med_name = self.medication_map[med_label]

                    #Filter inputs on titration range
                    starttime = row['starttime']
                    endtime = row['endtime']
                    med_mask = (timestamps >= starttime) & (timestamps < endtime)
                    if not med_mask.any():
                        continue

                    order_category = row['ordercategorydescription']
                    rate = row['rate']
                    rateuom = row['rateuom']
                    amount = row['amount']
                    #Fill ratenorm and derived feature values
                    if pd.notna(rate) and order_category in ['Continuous Med', 'Continuous IV']:
                        rate_norm = self.normalize_rate(rate, rateuom, weight)
                        if pd.notna(rate_norm) and med_name not in self.sparse_meds:
                            ehr_dict[f'{med_name}_ratenorm'][med_mask] = rate_norm
                    ehr_dict = self.add_derived(ehr_dict) #norepi_eq, pressor_on, dilator_on
                    #Fill bolus flag
                    if pd.notna(amount) and order_category in ['Bolus', 'Drug Push']:
                        ehr_dict[f'{med_name}_bolus'][med_mask] = 1
                    #Fill active flag
                    if (pd.notna(amount) or pd.notna(rate)):
                        ehr_dict[f'{med_name}_on'][med_mask] = 1
        except KeyError:
            # No inputs for this stay
            print(f'No inputevents for stay_{stay_id}')
        
        
           
        
        # ============================================
        # LABS
        # ============================================
        try:
            stay_labs = self.labs_by_stay.get_group((subject_id, hadm_id))

            chunk_labs = stay_labs[
                (stay_labs['charttime'] >= chunk_start) &
                (stay_labs['charttime'] < chunk_end)
            ]
            if len(chunk_labs) > 0:
                for _, row in chunk_labs.iterrows():
                    lab_label = row['label']
                    #Map medication label to standardized name
                    if lab_label not in self.lab_map:
                        continue
                    charttime = pd.Timestamp(row["charttime"])
                    valuenum = row["valuenum"]
                    ehr_dict[lab_name][:] = valuenum
        except KeyError:
            print(f'No labs for this (sid, hid) pair: ({subject_id},{hadm_id})')
            
        
        

            
        
        # ============================================
        # ICD CODES
        # ============================================
        try:
            stay_codes = self.codes_by_stay.get_group((subject_id, hadm_id))
            if len(stay_codes) > 0:
                for code in self.codes:
                    if code in stay_codes['icd_code']:
                        ehr_dict[code][:] = 1
        except KeyError:
            print(f'No icd codes for this (sid, hid) pair: ({subject_id},{hadm_id})')
        #Fill missing features with NaN
        return ehr_dict
    
    def normalize_rate(self, rate, rate_uom, weight):
        if pd.isna(rate) or pd.isna(weight):
            return np.nan
        if rate_uom == "mcg/kg/min":
            return rate   
        elif rate_uom == "ng/kg/min":
            return rate / 1000     # convert ng → mcg
        elif rate_uom == "mg/kg/hour":
            return rate * 1000 / 60
        elif rate_uom == "mcg/hour":
            return rate / weight / 60
        elif rate_uom == "mg/hour":
            return rate * 1000 / weight / 60   # mg/hr → mcg/kg/min
        elif rate_uom == "mg/min":
            return rate * 1000 / weight
        elif rate_uom == "units/hour":
            return rate / 60 
        else:
            print(f"unhandled uom: {rate_uom}!")  # alert
            return rate
        
    def add_derived(ehr_dict):
        norepi_eq = np.nan
        for med_cat in med_categories:
            f"{med_cat}_on" = 0
        NEE_FACTORS = {
            'norepinephrine': 1.0,
            'epinephrine': 1.0,
            'dopamine': 0.01,
            'phenylephrine': 0.06,
            'vasopressin': 2.5,
            'angiotensin': 0.0025 * 1000 #normalized ng to mcg in ratenorm
        }
        dilators = ['nicardipine',
            'diltiazem',
            'hydralazine',
            'nitroglycerin',
            'nitroprusside',
            'labetalol',
            'isuprel']
    
        # Sum up equivalents from all vasopressors
        for drug, factor in NEE_FACTORS.items():
            rate_key = f"{drug}_ratenorm"
            on_key = f"{drug}_on"
            rates = ehr_dict[rate_key]
            
            #Replace NaN with 0 for calculation
            rates_filled = np.nan_to_num(rates, nan=0.0)
            norepi_eq += rates_filled * factor

            pressor_on = np.maximum(pressor_on, ehr_dict[on_key])

        for drug in dilators:
            on_key = f"{drug}_on"

            dilator_on = np.maximum(dilator_on, ehr_dict[on_key])


        ehr_dict['norepi_eq'] = norepi_eq
        ehr_dict['pressor_on'] = pressor_on
        ehr_dict['dilator_on'] = dilator_on
        
        return ehr_dict