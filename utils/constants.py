
from pathlib import Path
REPO_PATH = str(Path(__file__).resolve().parents[0])

#Signals
DEFAULT_CHUNK_DURATION = 60.0 #seconds

SIGNAL_VARIANTS = {
    'ABP':   ['ABP', 'ART'],
    'PLETH': ['PLETH', 'PLETH L', 'PLETH R', 'PLETHl', 'PLETHr', 'PLTHPR', 'Pleth'],
    'AVR':   ['AVR', 'aVR'],
    'AVL': ['AVL', 'aVL'],
    'I':     ['I'],
    'II':    ['II'],
    'III':   ['III'],
    'V':     ['V'],
    'MCL1':  ['MCL1', 'MCL'],
    'RESP':  ['RESP', 'Resp'],
    'PAP':   ['PAP'],
    'CVP':   ['CVP'],
    'ICP': ['ICP'],
}

SIGNAL_NAME_MAP = {
    variant: canonical
    for canonical, variants in SIGNAL_VARIANTS.items()
    for variant in variants
}

TARGET_SIGNALS = list(SIGNAL_VARIANTS.keys())

#All records to include 
FINAL_SIGNALS = ['II', 'PLETH', 'ABP']

MIN_SIGNAL_HRS = 0.5
TARGET_FS = 125.0


#EHR
WEIGHT_ITEMIDS = [580, 581, 733, 763, 3580, 3581, 3582, 3583, 3692, 3693, 3723, 45271, 227854, 226846, 224639, 226512, 226531]
WEIGHT_MIN = 20 
WEIGHT_MAX = 500 #kg

# labs
LAB_MAP = {
    "Hematocrit": "hematocrit",
    "Platelet Count": "platelets",
    "Creatinine": "creatinine",
    "Potassium": "potassium",
    "Hemoglobin": "hemoglobin",
    "White Blood Cells": "wbc",
    "MCHC": "mchc",
    "Red Blood Cells": "rbc",
    "MCV": "mcv",
    "MCH": "mch",
    "RDW": "rdw",
    "Urea Nitrogen": "bun",
    "Sodium": "sodium",
    "Chloride": "chloride",
    "Bicarbonate": "bicarbonate",
    "Anion Gap": "anion_gap",
    "Glucose": "glucose",
    "Magnesium": "magnesium",
    "Calcium, Total": "calcium",
    "Phosphate": "phosphate",
    "INR(PT)": "inr",
    "PT": "pt",
    "PTT": "ptt",
    "Basophils": "basophils",
    "Neutrophils": "neutrophils",
    "Monocytes": "monocytes",
    "Eosinophils": "eosinophils",
    "Lymphocytes": "lymphocytes",
    "RDW-SD": "rdw_sd",
    "H": "h",
    "L": "l",
    "I": "i",
    "Alanine Aminotransferase (ALT)": "alt",
    "Asparate Aminotransferase (AST)": "ast",
    "Lactate": "lactate",
    "Alkaline Phosphatase": "alp",
    "Bilirubin, Total": "bilirubin",
    "pH": "ph",
    "Albumin": "albumin",
    "Base Excess": "base_excess",
    "pO2": "po2",
    "Calculated Total CO2": "tco2",
    "pCO2": "pco2",
    "Absolute Neutrophil Count": "anc",
    "Absolute Eosinophil Count": "aec",
    "Absolute Monocyte Count": "amc",
    "Absolute Basophil Count": "abc",
    "Absolute Lymphocyte Count": "alc",
    "Creatine Kinase (CK)": "ck",
    "Immature Granulocytes": "ig"
}

TOP_20_LABS = ['Potassium', 'Sodium', 'Chloride', 'Hematocrit', 'Glucose',
       'Creatinine', 'Urea Nitrogen', 'Bicarbonate', 'Anion Gap', 'Hemoglobin',
       'Platelet Count', 'White Blood Cells', 'Red Blood Cells', 'MCH', 'RDW',
       'MCHC', 'MCV', 'Magnesium', 'Phosphate', 'Calcium, Total']

LAB_LABELS = list(LAB_MAP.keys())

# inputs
MEDS = {
    # VASOPRESSORS
    'norepinephrine': {
        'category': 'vasopressor',
        'variants': ['Levophed-k', 'Levophed', 'Norepinephrine'],
    },
    'epinephrine': {
        'category': 'vasopressor',
        'variants': ['Epinephrine-k', 'Epinephrine'],
    },
    'dopamine': {
        'category': 'vasopressor',
        'variants': ['Dopamine', 'Dopamine Drip'],
    },
    'phenylephrine': {
        'category': 'vasopressor',
        'variants': ['Neosynephrine-k', 'Neosynephrine', 'Phenylephrine',
                     'Phenylephrine (50/250)', 'Phenylephrine (200/250)'],
    },
    'vasopressin': {
        'category': 'vasopressor',
        'variants': ['Vasopressin'],
    },
    'octreotide': {
        'category': 'vasopressor',
        'variants': ['Sandostatin', 'OCTREOTIDE', 'Octreotide', 'octreotide'],
    },
    'isoproterenol': {
        'category': 'vasopressor',
        'variants': ['Isuprel'],
    },

    # INOTROPES
    'milrinone': {
        'category': 'inotrope',
        'variants': ['Milrinone'],
    },
    'dobutamine': {
        'category': 'inotrope',
        'variants': ['Dobutamine'],
    },
    'amrinone': {
        'category': 'inotrope',
        'variants': ['Amrinone'],
    },
    'aminophylline': {
        'category': 'inotrope',
        'variants': ['Aminophylline'],
    },
    'atropine': {
        'category': 'inotrope',
        'variants': ['Atropine'],
    },

    # VASODILATORS
    'nitroprusside': {
        'category': 'vasodilator',
        'variants': ['Nitroprusside'],
    },
    'nitroglycerin': {
        'category': 'vasodilator',
        'variants': ['Nitroglycerine-k', 'Nitroglycerine', 'Nitroglycerin'],
    },
    'nicardipine': {
        'category': 'vasodilator',
        'variants': ['Nicardipine', 'nicardipine gtt', 'Nicardipine 40mg/200'],
    },
    'labetalol': {
        'category': 'vasodilator',
        'variants': ['Labetolol', 'Labetalol'],
    },
    'diltiazem': {
        'category': 'vasodilator',
        'variants': ['Diltiazem'],
    },
    'esmolol': {
        'category': 'vasodilator',
        'variants': ['Esmolol'],
    },
    'nesiritide': {
        'category': 'vasodilator',
        'variants': ['Natrecor', 'Nesiritide'],
    },
    'epoprostenol': {
        'category': 'vasodilator',
        'variants': ['FLOLAN'],
    },
    'prostaglandin': {
        'category': 'vasodilator',
        'variants': ['Prostaglandin'],
    },
    'metoprolol': {
        'category': 'vasodilator',
        'variants': ['Metoprolol'],
    },
    'hydralazine': {
        'category': 'vasodilator',
        'variants': ['Hydralazine'],
    },
    'verapamil': {
        'category': 'vasodilator',
        'variants': ['Verapamil'],
    },
    'angiotensin_ii': {
        'category': 'vasodilator',
        'variants': ['Angiotensin II (Giapreza)'],
    },
    'furosemide': {
        'category': 'vasodilator',
        'variants': ['Furosemide (Lasix)', 'Lasix'],
    },
    'fenoldopam': {
        'category': 'vasodilator',
        'variants': ['FENOLDOPAM 10MG/250C', 'Fendolapam'], 
    },

    # ANTIARRHYTHMICS
    'amiodarone': {
        'category': 'antiarrhythmic',
        'variants': ['Amiodarone', 'Amiodarone 600/500', 'Amiodarone 450/250'],
    },
    'lidocaine': {
        'category': 'antiarrhythmic',
        'variants': ['Lidocaine'],
    },
    'procainamide': {
        'category': 'antiarrhythmic',
        'variants': ['Procainamide'],
    },
    'adenosine': {
        'category': 'antiarrhythmic',
        'variants': ['Adenosine'],
    },

    # SEDATIVES & ANALGESICS
    'propofol': {
        'category': 'sedative_analgesic',
        'variants': ['Propofol'],
    },
    'fentanyl': {
        'category': 'sedative_analgesic',
        'variants': ['Fentanyl', 'Fentanyl (Conc)', 'Fentanyl Base', 'Fentanyl (Concentrate)', 'Fentanyl Drip'],
    },
    'midazolam': {
        'category': 'sedative_analgesic',
        'variants': ['Midazolam', 'Midazolam (Versed)'],
    },
    'lorazepam': {
        'category': 'sedative_analgesic',
        'variants': ['Ativan', 'Lorazepam (Ativan)'],
    },
    'morphine': {
        'category': 'sedative_analgesic',
        'variants': ['Morphine Sulfate'],
    },
    'hydromorphone': {
        'category': 'sedative_analgesic',
        'variants': ['Dilaudid', 'Hydromorphone (Dilaudid)'],
    },
    'dexmedetomidine': {
        'category': 'sedative_analgesic',
        'variants': ['Precedex', 'Dexmedetomidine (Precedex)', 'Precedex (mcg/kg/hr)', 'PRECEDEX CC/HR'],
    },
    'pentobarbital': {
        'category': 'sedative_analgesic',
        'variants': ['Pentobarbitol'],
    },
    'meperidine': {
        'category': 'sedative_analgesic',
        'variants': ['demerol', 'Meperidine (Demerol)'],
    },
    'ketamine': {
        'category': 'sedative_analgesic',
        'variants': ['Ketamine'],
    },
    'methadone': {
        'category': 'sedative_analgesic',
        'variants': ['Methadone Hydrochloride'],
    },
    'diazepam': {
        'category': 'sedative_analgesic',
        'variants': ['Diazepam (Valium)'],
    },
    'haloperidol': {
        'category': 'sedative_analgesic',
        'variants': ['Haloperidol (Haldol)'],
    },
    'bupivacaine': {
        'category': 'sedative_analgesic',
        'variants': ['bupivacaine'],
    },
    'epidural': {
        'category': 'sedative_analgesic',
        'variants': ['Epidural', 'epidural'],
    },
    'acetaminophen_iv': {
        'category': 'sedative_analgesic',
        'variants': ['Acetaminophen-IV'],
    },
    # NEUROMUSCULAR BLOCKERS
    'cisatracurium': {
        'category': 'nm_blocker',
        'variants': ['Cisatracurium', 'CISATRICURIUM', 'cisatricurium',
                    'Cisat mcg/kg/hr', 'Cisat mcg/kg/min', 'NIMBEX', 'nimbex'],
    },
    'vecuronium': {
        'category': 'nm_blocker',
        'variants': ['Vecuronium'],
    },
    'pancuronium': {
        'category': 'nm_blocker',
        'variants': ['Pancuronium'],
    },
    'atracurium': {
        'category': 'nm_blocker',
        'variants': ['Atracurium'],
    },
    'doxacurium': {
        'category': 'nm_blocker',
        'variants': ['Doxacurium'],
    },
}

# flat lookup: raw label → (normalized_name, category)
MED_MAP = {
    variant: (name, entry['category'])
    for name, entry in MEDS.items()
    for variant in entry['variants']
}
MED_CATEGORIES = ['vasopressor', 'vasodilator', 'inotrope', 'antiarrhythmic', 'sedative_analgesic']
INPUT_LABELS = list(MED_MAP.keys())