from dataset.ehr import MIMIC_EHR
from utils.utils import load_config

def main():
    config = load_config(path='/u/project/jchiang/tsisson/mimic/configs/config.yaml')

    mimic4 = MIMIC_EHR(config, 4)
    mimic4.preprocess()
    mimic4.save()

    mimic3 = MIMIC_EHR(config, 3)
    mimic3.preprocess()
    mimic3.save()

if __name__ == '__main__':
    main()