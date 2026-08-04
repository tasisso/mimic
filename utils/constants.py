
from pathlib import Path
REPO_PATH = str(Path(__file__).resolve().parents[0])

#Signals
DEFAULT_CHUNK_DURATION = 60.0 #seconds

SIGNAL_VARIANTS = {
    'ABP':   ['ABP'],
    'ART':   ['ART'],
    'PLETH': ['PLETH', 'PLETH L', 'PLETH R', 'PLETHl', 'PLETHr', 'PLTHPR', 'Pleth'],
    'AVR':   ['AVR', 'aVR'],
    'AVL':   ['AVL', 'aVL'],
    'I':     ['I'],
    'II':    ['II'],
    'III':   ['III'],
    'V':     ['V'],
    'MCL1':  ['MCL1', 'MCL'],
    'RESP':  ['RESP', 'Resp'],
    'PAP':   ['PAP'],
    'CVP':   ['CVP'],
    'ICP':   ['ICP'],
}

SIGNAL_NAME_MAP = {
    variant: canonical
    for canonical, variants in SIGNAL_VARIANTS.items()
    for variant in variants
}

TARGET_SIGNALS = list(SIGNAL_VARIANTS.keys())
TARGET_FS = 125.0

#EHR
WEIGHT_ITEMIDS = [580, 581, 762, 763, 3693, 3723, 3580, 226512, 226531, 224639]
HEIGHT_ITEMIDS = [920, 1394, 4187, 3486, 3485, 4188, 226707, 226730]

WEIGHT_MIN = 20 
WEIGHT_MAX = 500 #kg

# labs
LAB_MAP = {
    "Hematocrit": "hematocrit",
    "Platelet Count": "platelets",
    "Creatinine": "creatinine",
    "Potassium": "potassium",
    "Hemoglobin": "hemoglobin",
    "White Blood Cells": "white_blood_cells",
    "MCHC": "mchc",
    "Red Blood Cells": "red_blood_cells",
    "MCV": "mcv",
    "MCH": "mch",
    "RDW": "rdw",
    "Urea Nitrogen": "urea_nitrogen",
    "Sodium": "sodium",
    "Chloride": "chloride",
    "Bicarbonate": "bicarbonate",
    "Anion Gap": "anion_gap",
    "Glucose": "glucose",
    "Magnesium": "magnesium",
    "Calcium, Total": "calcium_total",
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
    "Alkaline Phosphatase": "alkaline_phosphatase",
    "Bilirubin, Total": "bilirubin_total",
    "pH": "ph",
    "Albumin": "albumin",
    "Base Excess": "base_excess",
    "pO2": "po2",
    "Calculated Total CO2": "tco2",
    "pCO2": "pco2",
    "Absolute Neutrophil Count": "absolute_neutrophil",
    "Absolute Eosinophil Count": "absolute_eosinophil",
    "Absolute Monocyte Count": "absolute_monocyte",
    "Absolute Basophil Count": "absolute_basophil",
    "Absolute Lymphocyte Count": "absolute_lymphocyte",
    "Creatine Kinase (CK)": "ck",
    "Immature Granulocytes": "immature_granulocytes"
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
        'category': ['vasopressor'],
        'variants': ['Levophed-k', 'Levophed', 'Norepinephrine'],
    },
    'epinephrine': {
        'category': ['vasodilator', 'positive_inotrope', 'vasopressor'],
        'variants': ['Epinephrine-k', 'Epinephrine'],
    },
    'dopamine': {
        'category': ['vasodilator', 'positive_inotrope', 'vasopressor'],
        'variants': ['Dopamine', 'Dopamine Drip'],
    },
    'phenylephrine': {
        'category': ['vasopressor'],
        'variants': ['Neosynephrine-k', 'Neosynephrine', 'Phenylephrine',
                     'Phenylephrine (50/250)', 'Phenylephrine (200/250)'],
    },
    'vasopressin': {
        'category': ['vasopressor'],
        'variants': ['Vasopressin'],
    },
    # 'octreotide': {
    #     'category': ['vasopressor'],
    #     'variants': ['Sandostatin', 'OCTREOTIDE', 'Octreotide', 'octreotide'],
    # },
    'angiotensin_ii': {
        'category': ['vasopressor'],
        'variants': ['Angiotensin II (Giapreza)'],
    },

    # POSITIVE INOTROPES
    'milrinone': {
        'category': ['positive_inotrope'],
        'variants': ['Milrinone'],
    },
    'dobutamine': {
        'category': ['vasodilator', 'positive_inotrope', 'vasopressor'],
        'variants': ['Dobutamine'],
    },
    'amrinone': {
        'category': ['positive_inotrope'],
        'variants': ['Amrinone'],
    },
    # 'aminophylline': {
    #     'category': ['positive_inotrope'],
    #     'variants': ['Aminophylline'],
    # },
    'atropine': {
        'category': ['positive_inotrope'],
        'variants': ['Atropine'],
    },
    'isoproterenol': {
        'category': ['positive_inotrope'],
        'variants': ['Isuprel'],
    },
    'digoxin': {
        'category': ['positive_inotrope', 'antiarrhythmic'],
        'variants': ['Digoxin (Lanoxin)'],
    },

    # ANTIHYPERTENSIVES
    'nitroprusside': {
        'category': ['vasodilator'],
        'variants': ['Nitroprusside'],
    },
    'nitroglycerin': {
        'category': ['vasodilator'],
        'variants': ['Nitroglycerine-k', 'Nitroglycerine', 'Nitroglycerin'],
    },
    'nicardipine': {
        'category': ['vasodilator'],
        'variants': ['Nicardipine', 'nicardipine gtt', 'Nicardipine 40mg/200'],
    },
    'labetalol': {
        'category': ['vasodilator', 'negative_inotrope'],
        'variants': ['Labetolol', 'Labetalol'],
    },
    'diltiazem': {
        'category': ['vasodilator', 'negative_inotrope'],
        'variants': ['Diltiazem'],
    },
    'esmolol': {
        'category': ['negative_inotrope'],
        'variants': ['Esmolol'],
    },
    'nesiritide': {
        'category': ['vasodilator'],
        'variants': ['Natrecor', 'Nesiritide'],
    },
    'epoprostenol': {
        'category': ['vasodilator'],
        'variants': ['FLOLAN', 'Epoprostenol (Veletri)'],
    },
    'prostaglandin': {
        'category': ['vasodilator'],
        'variants': ['Prostaglandin'],
    },
    'metoprolol': {
        'category': ['negative_inotrope'],
        'variants': ['Metoprolol'],
    },
    'hydralazine': {
        'category': ['vasodilator'],
        'variants': ['Hydralazine'],
    },
    'verapamil': {
        'category': ['vasodilator', 'negative_inotrope'],
        'variants': ['Verapamil'],
    },
    'fenoldopam': {
        'category': ['vasodilator'],
        'variants': ['FENOLDOPAM 10MG/250C', 'Fendolapam'], 
    },

    # ANTIARRHYTHMICS
    'amiodarone': {
        'category': ['antiarrhythmic'],
        'variants': ['Amiodarone', 'Amiodarone 600/500', 'Amiodarone 450/250'],
    },
    'lidocaine': {
        'category': ['antiarrhythmic'],
        'variants': ['Lidocaine'],
    },
    'procainamide': {
        'category': ['antiarrhythmic'],
        'variants': ['Procainamide'],
    },
    'adenosine': {
        'category': ['antiarrhythmic'],
        'variants': ['Adenosine'],
    },


    # SEDATIVES & ANALGESICS
    'propofol': {
        'category': ['analgesic'],
        'variants': ['Propofol'],
    },
    'fentanyl': {
        'category': ['analgesic'],
        'variants': ['Fentanyl', 'Fentanyl (Conc)', 'Fentanyl Base', 'Fentanyl (Concentrate)', 'Fentanyl Drip'],
    },
    'midazolam': {
        'category': ['analgesic'],
        'variants': ['Midazolam', 'Midazolam (Versed)'],
    },
    'lorazepam': {
        'category': ['analgesic'],
        'variants': ['Ativan', 'Lorazepam (Ativan)'],
    },
    'morphine': {
        'category': ['analgesic'],
        'variants': ['Morphine Sulfate'],
    },
    'hydromorphone': {
        'category': ['analgesic'],
        'variants': ['Dilaudid', 'Hydromorphone (Dilaudid)'],
    },
    'dexmedetomidine': {
        'category': ['analgesic'],
        'variants': ['Precedex', 'Dexmedetomidine (Precedex)', 'Precedex (mcg/kg/hr)', 'PRECEDEX CC/HR'],
    },
    'pentobarbital': {
        'category': ['analgesic'],
        'variants': ['Pentobarbitol'],
    },
    'meperidine': {
        'category': ['analgesic'],
        'variants': ['demerol', 'Meperidine (Demerol)'],
    },
    'ketamine': {
        'category': ['analgesic'],
        'variants': ['Ketamine'],
    },
    'methadone': {
        'category': ['analgesic'],
        'variants': ['Methadone Hydrochloride'],
    },
    'diazepam': {
        'category': ['analgesic'],
        'variants': ['Diazepam (Valium)'],
    },
    'haloperidol': {
        'category': ['analgesic'],
        'variants': ['Haloperidol (Haldol)'],
    },
    'bupivacaine': {
        'category': ['analgesic'],
        'variants': ['bupivacaine'],
    },
    'epidural': {
        'category': ['analgesic'],
        'variants': ['Epidural', 'epidural'],
    },
    'acetaminophen_iv': {
        'category': ['analgesic'],
        'variants': ['Acetaminophen-IV'],
    },
    # NEUROMUSCULAR BLOCKERS
    'cisatracurium': {
        'category': ['nm_blocker'],
        'variants': ['Cisatracurium', 'CISATRICURIUM', 'cisatricurium',
                    'Cisat mcg/kg/hr', 'Cisat mcg/kg/min', 'NIMBEX', 'nimbex'],
    },
    'vecuronium': {
        'category': ['nm_blocker'],
        'variants': ['Vecuronium'],
    },
    'pancuronium': {
        'category': ['nm_blocker'],
        'variants': ['Pancuronium'],
    },
    'atracurium': {
        'category': ['nm_blocker'],
        'variants': ['Atracurium'],
    },
    'doxacurium': {
        'category': ['nm_blocker'],
        'variants': ['Doxacurium'],
    },
    #DIURETICS
    'furosemide': {
        'category': ['diuretic'],
        'variants': ['Furosemide (Lasix)', 'Lasix', 'Furosemide (Lasix) 250/50'],
    },
    'bumetanide': {
        'category': ['diuretic'],
        'variants': ['Bumetanide (Bumex)'],
    },
    'mannitol': {
        'category': ['diuretic'],
        'variants': ['Mannitol'],
    },
}

DOSE_DEPENDENT = {
    'dopamine': [
        (0.0, 3.0, ['vasodilator']),
        (3.0, 5.0, ['positive_inotrope']),
        (5.0, float('inf'), ['vasopressor']),
    ],
    'epinephrine': [
        (0, 0.1, ['vasodilator']),
        (0.1, float('inf'), ['vasopressor', 'positive_inotrope']),
    ],
    'dobutamine': [
        (0, 5, ['vasodilator', 'positive_inotrope']),
        (5, float('inf'), ['vasopressor', 'positive_inotrope'])
    ],
}

# flat lookup: raw label → (normalized_name, category)
MED_MAP = {
    variant: (name, entry['category'])
    for name, entry in MEDS.items()
    for variant in entry['variants']
}
MED_CATEGORIES = {'vasopressor', 'vasodilator', 'positive_inotrope', 'negative_inotrope', 'vasoactive',
                  'antiarrhythmic', 'analgesic', 'nm_blocker', 'diuretic'}
VASOACTIVE = {'vasopressor', 'positive_inotrope', 'negative_inotrope', 'vasodilator'}

INPUT_LABELS = list(MED_MAP.keys())
INPUT_FEATURES = list(MEDS.keys())

BOLUS_FEATURES = set([
    'amiodarone', 'diltiazem', 'lidocaine', 'adenosine', 'digoxin',
    'verapamil', 'metoprolol', 'atropine', 'hydralazine', 'labetalol',
    'furosemide', 'bumetanide', 'fentanyl', 'morphine', 'hydromorphone',
    'meperidine', 'lorazepam', 'midazolam', 'diazepam', 'haloperidol',
    'propofol', 'acetaminophen_iv', 'methadone', 'cisatracurium', 'vecuronium',
])


#ICD10 appearing in >= 5% of all subjects
COMMON_CODES = ['I10', 'I169', 'I2510', 'I4891', 'I509', 'I50814', 'E785', 'E784', 'N179', 'E119', 'J9600', 'J9690', 'K219', 'N390', 'D649', 'D62', 'E039', 'J449', 'E7800', 'A419', 
                'E7801', 'E872', 'Z7901', 'J189', 'Z87891', 'I129', 'F17200', 'F329', 'D696', 'R6520', 'E871', 'I214', 'I252', 'N189', 'Z66', 'J690', 'Z9861', 'N170', 'I350', 'I351', 
                'K9161', 'H59111', 'E3602', 'H9521', 'H59112', 'H9522', 'G9732', 'H59113', 'J9561', 'N9962', 'J9562', 'D7802', 'M96810', 'M96811', 'D7801', 'N9961', 'H59123', 'H59119', 
                'H59122', 'H59121', 'H59129', 'G9731', 'E3601', 'I97410', 'K9162', 'L7602', 'I97418', 'I97411', 'I358', 'I9742', 'I359', 'I352', 'L7601', 'G4733', 'R001', 'I340', 'Z794', 
                'R6521', 'Z951', 'I348', 'I9581', 'J918', 'I952', 'I9789', 'I9788', 'I97790', 'I97710', 'E875', 'I2720', 'I498', 'E870', 'I2723', 'I2722', 'I2729', 'I2721', 'I2789', 
                'I2724', 'F419', 'I472', 'J45909', 'J45998', 'M109', 'J9811', 'I959', 'J9819', 'E669', 'I619', 'I120', 'G936', 'M810', 'Z515', 'F05', 'I469', 'I200', 'I428', 'N400', 
                'I425', 'R570', 'R569', 'E1165', 'Z8673', 'K91840', 'N99820', 'I97611', 'I97610', 'M96831', 'M96830', 'H59319', 'H9542', 'H9541', 'D7822', 'H59323', 'H59322', 'D7821',
                'J95830', 'E89811', 'E89810', 'J95831', 'L7621', 'H59329', 'G9752', 'H59311', 'N99821', 'G9751', 'I97620', 'I97618', 'H59312', 'H59313', 'K91841', 'L7622', 'H59321',
                'R188', 'N186', 'Z86718', 'K7200', 'K762', 'E46', 'I5033', 'E876', 'I97621', 'L7632', 'G9762', 'K91870', 'F1020', 'N99841', 'I97638', 'H59342', 'H59333', 'H59341',
                'H59331', 'K91871', 'H59349', 'H59343', 'H9552', 'I5023', 'E89821', 'J95860', 'J95861', 'I97631', 'E89820', 'I97630', 'L7631', 'H59339', 'M96840', 'M96841', 'G9761',
                'D7832', 'D7831', 'H9551', 'H59332', 'N99840', 'I4892', 'I5022', 'E1342', 'E0942', 'E0842', 'E1142', 'E1042', 'K7030', 'A0472', 'I739', 'R0902', 'K7460', 'F1010',
                'E11319', 'K7469', 'K740', 'A0471', 'Z950', 'E1140', 'D72829', 'T829XXA', 'E8339', 'M1990', 'R7881', 'Z8546', 'K7290', 'T814XXA', 'I5032', 'Z006', 'G935', 'K766',
                'E6601', 'K922', 'B182', 'E861', 'K6811', 'E873', 'J95851', 'D631', 'R339', 'Z853', 'R791', 'T888XXA', 'I209', 'M159', 'I208', 'K5900', 'D689', 'E8332', 'E8330',
                'E8331', 'Z96659', 'L03119', 'L03129', 'K567', 'D688', 'J441', 'G9340', 'K5289', 'I2109', 'K560', 'Z8249', 'I2119', 'N183', 'I2699', 'G92', 'W19XXXA', 'I259',
                'I255', 'E8351', 'I2589', 'E860', 'T82817A', 'R197', 'L89149', 'L89139', 'L89159', 'I6340', 'D509', 'I9751', 'K9171', 'D7811', 'H59219', 'E3611', 'E3612', 'G9748',
                'L7612', 'G9749', 'T82818A', 'L7611', 'K9172', 'H59229', 'N9971', 'H9531', 'J9571', 'M96820', 'I9752', 'D7812', 'H9532', 'J9572', 'N9972', 'M96821', 'E8770',
                'I609', 'G40909', 'D638', 'Z7952', 'E8779', 'E1129', 'Z7951', 'K5229', 'R4701', 'K8590', 'S065X0A', 'K8591', 'G40901', 'K7291', 'K8592', 'F341', 'I4901', 'Z923',
                'E874', 'J95811', 'I080', 'I6350', 'H409', 'W1849XA', 'I071', 'F068', 'I442', 'Z95810', 'R1310', 'B952', 'I078', 'I072', 'G911', 'T827XXA', 'G8929', 'G8190',
                'Z96649', 'Z781', 'S2249XA', 'I5021', 'D500', 'K5730', 'T82897A', 'F319', 'T82857A', 'R400', 'R578', 'T82847A', 'R401', 'T82867A', 'T82827A', 'T82837A',
                'B9561', 'C7931', 'E1065', 'R130', 'I6529', 'Z992', 'T82868A', 'J95821', 'R571', 'T82858A', 'E851', 'E8589', 'E8582', 'E8581', 'T82898A', 'E1169', 'J439',
                'T82848A', 'T82828A', 'K449', 'T82838A', 'R000', 'Z85828', 'I25810', 'Z85038', 'F10239', 'T8172XA', 'T82855A', 'B1920', 'J15211', 'C787', 'Z9981',
                'F060', 'M069', 'C7951', 'B9620', 'F0390', 'Z98890', 'C7952', 'J8410', 'B9689', 'G931', 'Z9181', 'Z952', 'N289', 'Z9221', 'J984', 'J8489', 'I69998',
                'M949', 'K55021', 'K55011', 'M899', 'E11311']