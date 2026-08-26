import argparse
import os
import time
from collections import Counter

import numpy as np
import pandas as pd
import torch
from sklearn.model_selection import GroupKFold
from skmultilearn.model_selection import iterative_train_test_split
from torch.utils.data import DataLoader

from icu_dataset import ICUDataset
from models.resnet import ResNet50
from train import train_one_epoch, evaluate

MED_LABELS = [
    'propofol', 'fentanyl', 'norepinephrine', 'dexmedetomidine', 'vasopressin',
    'phenylephrine', 'furosemide', 'amiodarone', 'nitroglycerin', 'nicardipine',
    'epinephrine', 'midazolam', 'hydromorphone', 'acetaminophen', 'morphine',
    'metoprolol', 'hydralazine', 'lorazepam',
]
MED_CATEGORIES = [
    'vasopressor', 'antiarrhythmic', 'vasoactive', 'negative_inotrope',
    'diuretic', 'vasodilator', 'positive_inotrope', 'analgesic',
]

# categories used for stratification (exclude nm_blocker and vasoactive)
_STRAT_CATEGORIES = [
    'vasopressor', 'antiarrhythmic', 'negative_inotrope',
    'diuretic', 'vasodilator', 'positive_inotrope', 'analgesic',
]

ICD_MIN_COUNT = 15  # minimum admissions for an ICD code to be included in stratification


def _pipe_binarize(series, vocab):
    """Convert pipe-delimited string column to binary matrix over vocab."""
    mat = np.zeros((len(series), len(vocab)), dtype=np.int32)
    vocab_idx = {v: i for i, v in enumerate(vocab)}
    for row_i, val in enumerate(series):
        if pd.isna(val):
            continue
        for token in str(val).split('|'):
            token = token.strip()
            if token in vocab_idx:
                mat[row_i, vocab_idx[token]] = 1
    return mat


def _eligible_icd_codes(metadata, min_count):
    """Return ICD codes appearing in at least min_count admissions."""
    counts = Counter()
    for val in metadata['icd10_truncated'].dropna():
        for code in str(val).split('|'):
            counts[code.strip()] += 1
    return sorted(c for c, n in counts.items() if n >= min_count)


def stratified_holdout(metadata, holdout_frac=0.13, icd_min_count=ICD_MIN_COUNT, random_state=42):
    """
    Split metadata into (train_cv_df, holdout_df) using iterative_train_test_split
    stratified jointly on eligible ICD codes and input categories (excl. nm_blocker/vasoactive).
    Subject-level: one row per subject_id (take first recording per subject for splitting,
    then map back to all recordings).
    """
    # collapse to one row per subject so the split is at subject level
    subj = metadata.groupby('subject_id').first().reset_index()

    icd_vocab  = _eligible_icd_codes(subj, icd_min_count)
    cat_vocab  = _STRAT_CATEGORIES

    icd_mat  = _pipe_binarize(subj['icd10_truncated'],   icd_vocab)
    cat_mat  = _pipe_binarize(subj['input_categories'],  cat_vocab)
    label_mat = np.hstack([icd_mat, cat_mat])

    idx = np.arange(len(subj)).reshape(-1, 1)
    train_idx, _, test_idx, _ = iterative_train_test_split(
        idx, label_mat, test_size=holdout_frac
    )
    train_subjects = set(subj.iloc[train_idx.ravel()]['subject_id'])
    test_subjects  = set(subj.iloc[test_idx.ravel()]['subject_id'])

    train_df   = metadata[metadata['subject_id'].isin(train_subjects)].reset_index(drop=True)
    holdout_df = metadata[metadata['subject_id'].isin(test_subjects)].reset_index(drop=True)

    print(f"Stratified split: {len(train_df)} train  |  {len(holdout_df)} holdout")
    print(f"  ICD codes stratified on: {len(icd_vocab)}  |  categories: {cat_vocab}")
    assert len(train_subjects & test_subjects) == 0, "Subject leakage in holdout split"
    return train_df, holdout_df


def build_dataset(metadata, signals, data_dir):
    return ICUDataset(
        metadata=metadata,
        signals=signals,
        med_labels=MED_LABELS,
        med_categories=MED_CATEGORIES,
        task='input_classification',
        data_dir=data_dir,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--metadata',       default='data/metadata.csv')
    parser.add_argument('--data_dir',       default='./data/data')
    parser.add_argument('--signals',        nargs='+', default=['PLETH', 'II', 'ABP'])
    parser.add_argument('--k',              type=int,   default=5)
    parser.add_argument('--epochs',         type=int,   default=10)
    parser.add_argument('--batch_size',     type=int,   default=32)
    parser.add_argument('--lr',             type=float, default=1e-3)
    parser.add_argument('--checkpoint_dir', default='checkpoints/cv')
    parser.add_argument('--holdout_frac',   type=float, default=0.13)
    parser.add_argument('--device',         default='cuda' if torch.cuda.is_available() else 'cpu')
    args = parser.parse_args()

    os.makedirs(args.checkpoint_dir, exist_ok=True)
    device = torch.device(args.device)

    metadata = pd.read_csv(args.metadata)
    metadata = metadata[metadata['has_inputs'] == 1].reset_index(drop=True)

    train_cv_meta, holdout_meta = stratified_holdout(
        metadata, holdout_frac=args.holdout_frac
    )
    holdout_meta.to_csv(os.path.join(args.checkpoint_dir, 'holdout_meta.csv'), index=False)

    print(f"\nCV pool: {len(train_cv_meta)} files  |  K={args.k}  |  device={device}")

    n_labels   = len(MED_LABELS) + len(MED_CATEGORIES)
    fold_auroc = []
    fold_auprc = []

    gkf = GroupKFold(n_splits=args.k)
    for fold, (train_idx, val_idx) in enumerate(gkf.split(train_cv_meta, groups=train_cv_meta['subject_id'])):
        train_df = train_cv_meta.iloc[train_idx]
        val_df   = train_cv_meta.iloc[val_idx]

        # sanity check — no patient leakage
        assert len(set(train_df['subject_id']) & set(val_df['subject_id'])) == 0

        print(f"\n{'='*60}")
        print(f"Fold {fold+1}/{args.k}  |  train={len(train_df)} files  val={len(val_df)} files")
        print(f"{'='*60}")

        train_ds = build_dataset(train_df, args.signals, args.data_dir)
        val_ds   = build_dataset(val_df,   args.signals, args.data_dir)

        train_loader = DataLoader(train_ds, batch_size=args.batch_size, num_workers=0, pin_memory=False)
        val_loader   = DataLoader(val_ds,   batch_size=args.batch_size, num_workers=0, pin_memory=False)

        model     = ResNet50(in_channels=len(args.signals), classes=n_labels).to(device)
        optimizer = torch.optim.Adam(model.parameters(), lr=args.lr)

        for epoch in range(1, args.epochs + 1):
            t0         = time.time()
            train_loss = train_one_epoch(model, train_loader, optimizer, device, pos_weight=None)
            print(f"  epoch {epoch:3d}/{args.epochs} | train loss {train_loss:.4f} | {time.time()-t0:.0f}s")

        val_loss, macro_auroc, macro_auprc = evaluate(
            model, val_loader, device,
            pos_weight=None,
            med_labels=MED_LABELS,
            med_categories=MED_CATEGORIES,
        )

        print(f"\nFold {fold+1} summary | val loss {val_loss:.4f} | AUROC {macro_auroc:.3f} | AUPRC {macro_auprc:.3f}")

        torch.save({
            'fold': fold + 1,
            'model': model.state_dict(),
            'val_loss': val_loss,
            'macro_auroc': macro_auroc,
            'macro_auprc': macro_auprc,
        }, os.path.join(args.checkpoint_dir, f'fold_{fold+1}.pt'))

        fold_auroc.append(macro_auroc)
        fold_auprc.append(macro_auprc)

    # ── summary ─────────────────────────────────────────────────────────────
    auroc_arr = np.array(fold_auroc)
    auprc_arr = np.array(fold_auprc)
    print(f"\n{'='*60}")
    print(f"{args.k}-fold cross-validation results")
    print(f"{'='*60}")
    for i, (au, ap) in enumerate(zip(fold_auroc, fold_auprc)):
        print(f"  fold {i+1}: AUROC {au:.3f}  AUPRC {ap:.3f}")
    print(f"  {'─'*40}")
    print(f"  mean:  AUROC {auroc_arr.mean():.3f} ± {auroc_arr.std():.3f}  "
          f"AUPRC {auprc_arr.mean():.3f} ± {auprc_arr.std():.3f}")


if __name__ == '__main__':
    main()
