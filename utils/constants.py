
from pathlib import Path
REPO_PATH = str(Path(__file__).resolve().parents[0])

#Signals
DEFAULT_CHUNK_DURATION = 60.0 #seconds

SIGNAL_VARIANTS = {
    'ABP':   ['ABP', 'ART'],
    'PLETH': ['PLETH', 'PLETH L', 'PLETH R', 'PLETHl', 'PLETHr', 'PLTHPR', 'Pleth'],
    'AVR':   ['AVR', 'aVR'],
    'I':     ['I'],
    'II':    ['II'],
    'III':   ['III'],
    'V':     ['V'],
    'MCL1':  ['MCL1', 'MCL'],
    'RESP':  ['RESP', 'Resp'],
    'PAP':   ['PAP'],
    'CVP':   ['CVP'],
}

SIGNAL_NAME_MAP = {
    variant: canonical
    for canonical, variants in SIGNAL_VARIANTS.items()
    for variant in variants
}

TARGET_SIGNALS = list(SIGNAL_VARIANTS.keys())

FINAL_SIGNALS = ['ii', 'pleth']

MIN_SIGNAL_HRS = 0.5


#EHR
WEIGHT_ITEMIDS = [580, 581, 733, 763, 3580, 3581, 3582, 3583, 3692, 3693, 3723, 45271, 227854, 226846, 224639, 226512, 226531]
WEIGHT_MIN = 20 
WEIGHT_MAX = 500 #kg

LAB_LABELS = ['Potassium', 'Sodium', 'Chloride', 'Hematocrit', 'Glucose',
       'Creatinine', 'Urea Nitrogen', 'Bicarbonate', 'Anion Gap', 'Hemoglobin',
       'Platelet Count', 'White Blood Cells', 'Red Blood Cells', 'MCH', 'RDW',
       'MCHC', 'MCV', 'Magnesium', 'Phosphate', 'Calcium, Total']

INPUT_LABELS = []

vasopressors_cv = [
    'Levophed-k', 'Levophed',                          # norepinephrine
    'Epinephrine-k',                                    # epinephrine
    'Dopamine',                                         # dopamine
    'Neosynephrine-k', 'Neosynephrine',                # phenylephrine
    'Vasopressin',                                      # vasopressin
    'Sandostatin', 'OCTREOTIDE',                        # octreotide
]

inotropes_cv = [
    'Milrinone',                                        # milrinone
    'Dobutamine',                                       # dobutamine
    'Amrinone',                                         # amrinone
    'Aminophylline',                                    # aminophylline
]

vasodilators_cv = [
    'Nitroprusside',                                    # nitroprusside
    'Nitroglycerine-k', 'Nitroglycerine',              # nitroglycerin
    'Nicardipine', 'nicardipine gtt',                  # nicardipine
    'Labetolol',                                        # labetalol
    'Diltiazem',                                        # diltiazem
    'Esmolol',                                          # esmolol
    'Natrecor',                                         # nesiritide
    'FLOLAN',                                           # epoprostenol
    'Prostaglandin',                                    # prostaglandin
]

antiarrhythmics_cv = [
    'Amiodarone',                                       # amiodarone
    'Lidocaine',                                        # lidocaine
    'Procainamide',                                     # procainamide
]

sedatives_cv = [
    'Propofol',                                         # propofol
    'Fentanyl', 'Fentanyl (Conc)', 'Fentanyl Base',   # fentanyl
    'Midazolam',                                        # midazolam
    'Ativan',                                           # lorazepam
    'Morphine Sulfate',                                 # morphine
    'Dilaudid',                                         # hydromorphone
    'Precedex',                                         # dexmedetomidine
    'Pentobarbitol',                                    # pentobarbital
    'demerol',                                          # meperidine
    'Epidural', 'epidural',                             # epidural
    'bupivacaine',                                      # bupivacaine
]

hemodynamic_cv = list(set(
    vasopressors_cv + inotropes_cv + vasodilators_cv + 
    antiarrhythmics_cv + sedatives_cv
))

#inputs_mv
vasopressors_mv = [
    'Norepinephrine',                                   # norepinephrine
    'Epinephrine',                                      # epinephrine
    'Dopamine',                                         # dopamine
    'Phenylephrine',                                    # phenylephrine
    'Vasopressin',                                      # vasopressin
    'Octreotide',                                       # octreotide'
]

inotropes_mv = [
    'Milrinone',                                        # milrinone
    'Dobutamine',                                       # dobutamine
    'Atropine',                                         # atropine
]

vasodilators_mv = [
    'Nitroprusside',                                    # nitroprusside
    'Nitroglycerin',                                    # nitroglycerin
    'Nicardipine',                                      # nicardipine
    'Labetalol',                                        # labetalol
    'Metoprolol',                                       # metoprolol
    'Diltiazem',                                        # diltiazem
    'Esmolol',                                          # esmolol
    'Hydralazine',                                      # hydralazine
    'Verapamil',                                        # verapamil
    'Nesiritide',                                       # nesiritide
]

antiarrhythmics_mv = [
    'Amiodarone', 'Amiodarone 600/500',                # amiodarone
    'Lidocaine',                                        # lidocaine
    'Procainamide',                                     # procainamide
    'Adenosine',                                        # adenosine
    'Diltiazem',                                        # diltiazem (dual use)
    'Esmolol',                                          # esmolol (dual use)
    'Verapamil',                                        # verapamil (dual use)
]

sedatives_mv = [
    'Propofol',                                         # propofol
    'Fentanyl', 'Fentanyl (Concentrate)',              # fentanyl
    'Midazolam (Versed)',                               # midazolam
    'Lorazepam (Ativan)',                               # lorazepam
    'Morphine Sulfate',                                 # morphine
    'Hydromorphone (Dilaudid)',                         # hydromorphone
    'Dexmedetomidine (Precedex)',                       # dexmedetomidine
    'Ketamine',                                         # ketamine
    'Meperidine (Demerol)',                             # meperidine
    'Methadone Hydrochloride',                          # methadone
    'Diazepam (Valium)',                                # diazepam
    'Haloperidol (Haldol)',                             # haloperidol
]

hemodynamic_mv = list(set(
    vasopressors_mv + inotropes_mv + vasodilators_mv +
    antiarrhythmics_mv + sedatives_mv
))