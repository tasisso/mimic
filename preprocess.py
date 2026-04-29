from dataset.mimic import MIMIC_EHR
from utils.utils import load_config

def main():
    config = load_config(path='/u/project/jchiang/tsisson/mimic/configs/config.yaml')
    mimic4 = MIMIC_EHR(config, 4)

    mimic4.preprocess()

    mimic4.save()

if __name__ == '__main__':
    main()