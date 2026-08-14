import argparse
import os
import time
from tqdm import tqdm

import pandas as pd
import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader
from sklearn.metrics import roc_auc_score, average_precision_score
import numpy as np

from icu_dataset import ICUDataset
from models.resnet import ResNet50

def masked_bce_loss(logits, targets, pos_weight=None):
    mask = targets != -1                          # (B, n_labels) bool
    pw = pos_weight.unsqueeze(0).expand_as(targets)[mask] if pos_weight is not None else None
    logits  = logits[mask]
    targets = targets[mask]
    return F.binary_cross_entropy_with_logits(logits, targets, pos_weight=pw)

def train_one_epoch(model, loader, optimizer, device, pos_weight):
    model.train()
    total_loss = 0.0
    n_batches = 0

    pbar = tqdm(loader, desc='train', leave=False)
    for waveform, targets in pbar:
        waveform = waveform.to(device)
        targets  = targets.to(device)

        optimizer.zero_grad()
        logits = model(waveform)
        loss   = masked_bce_loss(logits, targets, pos_weight)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
        optimizer.step()

        total_loss    += loss.item()
        n_batches     += 1
        
        pbar.set_postfix({'loss': f'{total_loss / n_batches:.4f}'})

    avg_loss = total_loss / max(n_batches, 1)
    return avg_loss

def masked_metrics(logits, targets):
    mask    = targets != -1
    preds   = (torch.sigmoid(logits) > 0.5).float()
    correct = (preds[mask] == targets[mask]).float().mean().item()
    
    # of all positive predictions, how many are right
    pos_mask = (preds == 1) & mask
    precision = (targets[pos_mask] == 1).float().mean().item() if pos_mask.sum() > 0 else 0.0
    
    # of all true positives, how many did we catch
    true_pos_mask = (targets == 1) & mask
    recall = (preds[true_pos_mask] == 1).float().mean().item() if true_pos_mask.sum() > 0 else 0.0

    return correct, precision, recall



def compute_auroc_auprc(all_logits, all_targets, label_names):
    """
    all_logits:  (N, n_labels) tensor
    all_targets: (N, n_labels) tensor with -1 for masked
    """
    probs   = torch.sigmoid(all_logits).numpy()
    targets = all_targets.numpy()

    auroc_scores = {}
    auprc_scores = {}

    for i, name in enumerate(label_names):
        # mask out -1 entries for this label
        mask = targets[:, i] != -1
        y_true = targets[mask, i]
        y_prob = probs[mask, i]

        # need at least one positive and one negative
        if y_true.sum() == 0 or (1 - y_true).sum() == 0:
            auroc_scores[name] = float('nan')
            auprc_scores[name] = float('nan')
            continue

        auroc_scores[name] = roc_auc_score(y_true, y_prob)
        auprc_scores[name] = average_precision_score(y_true, y_prob)

    # macro average ignoring nan labels
    valid_auroc = [v for v in auroc_scores.values() if not np.isnan(v)]
    valid_auprc = [v for v in auprc_scores.values() if not np.isnan(v)]

    macro_auroc = np.mean(valid_auroc)
    macro_auprc = np.mean(valid_auprc)

    return auroc_scores, auprc_scores, macro_auroc, macro_auprc

@torch.no_grad()
def evaluate(model, loader, device, pos_weight, med_labels, med_categories):
    model.eval()
    total_loss = 0
    n_batches  = 0
    
    all_logits  = []
    all_targets = []

    pbar = tqdm(loader, desc='val', leave=False)
    for waveform, targets in pbar:
        waveform = waveform.to(device)
        targets  = targets.to(device)

        logits = model(waveform)
        loss   = masked_bce_loss(logits, targets, pos_weight)
        total_loss += loss.item()
        n_batches  += 1

        all_logits.append(logits.cpu())
        all_targets.append(targets.cpu())
        pbar.set_postfix({'loss': f'{total_loss / n_batches:.4f}'})

    all_logits  = torch.cat(all_logits,  dim=0)
    all_targets = torch.cat(all_targets, dim=0)

    label_names = med_labels + med_categories
    auroc_scores, auprc_scores, macro_auroc, macro_auprc = compute_auroc_auprc(
        all_logits, all_targets, label_names
    )

    # per label
    for name in label_names:
        auroc = auroc_scores[name]
        auprc = auprc_scores[name]
        if not np.isnan(auroc):
            print(f"  {name:30s}  AUROC {auroc:.3f}  AUPRC {auprc:.3f}")

    print(f"\n  macro AUROC {macro_auroc:.3f}  macro AUPRC {macro_auprc:.3f}")

    return total_loss / n_batches, macro_auroc, macro_auprc


def build_dataset(metadata, signals, med_labels, med_categories, task, data_dir):
    return ICUDataset(
        metadata=metadata,
        signals=signals,
        med_labels=med_labels,
        med_categories=med_categories,
        task=task,
        data_dir=data_dir
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--metadata',      default='data/metadata.csv')
    parser.add_argument('--data_dir',      default='./data/data')
    parser.add_argument('--task',          default='input_classification',
                        choices=['input_classification', 'category_classification'])
    parser.add_argument('--signals',       nargs='+', default=['PLETH', 'II', 'ABP'])
    parser.add_argument('--epochs',        type=int,   default=20)
    parser.add_argument('--batch_size',    type=int,   default=32)
    parser.add_argument('--lr',            type=float, default=1e-3)
    parser.add_argument('--num_workers',   type=int,   default=0)
    parser.add_argument('--checkpoint_dir', default='checkpoints')
    parser.add_argument('--device',        default='cuda' if torch.cuda.is_available() else 'cpu')
    args = parser.parse_args()

    os.makedirs(args.checkpoint_dir, exist_ok=True)

    # ── labels ──────────────────────────────────────────────────────────────
    # med_labels = [
    #     'diltiazem', 'cisatracurium', 'lorazepam', 'nitroglycerin', 'fentanyl',
    #     'midazolam', 'meperidine', 'ketamine', 'esmolol', 'metoprolol',
    #     'angiotensin_ii', 'propofol', 'mannitol', 'hydromorphone', 'nicardipine',
    #     'dopamine', 'labetalol', 'vasopressin', 'nitroprusside', 'epinephrine',
    #     'dexmedetomidine', 'acetaminophen_iv', 'epoprostenol', 'haloperidol',
    #     'norepinephrine', 'digoxin', 'amiodarone', 'dobutamine', 'furosemide',
    #     'milrinone', 'bumetanide', 'morphine', 'hydralazine', 'phenylephrine',
    # ]
    med_categories = [
        'vasopressor', 'antiarrhythmic', 'vasoactive', 'negative_inotrope',
        'diuretic', 'vasodilator', 'positive_inotrope', 'analgesic',
    ]
    med_labels = [
        'propofol', 'fentanyl', 'norepinephrine', 'dexmedetomidine', 'vasopressin',
        'phenylephrine',
        'furosemide',
        'amiodarone',
        'nitroglycerin',
        'nicardipine',
        'epinephrine',
        'midazolam',
        'hydromorphone',
        'acetaminophen',
        'morphine',
        'metoprolol',
        'hydralazine',
        'lorazepam']

    # ── data ────────────────────────────────────────────────────────────────
    metadata = pd.read_csv(args.metadata)
    metadata = metadata[metadata['has_inputs'] == 1].reset_index(drop=True)

    if 'split' in metadata.columns:
        train_df = metadata[metadata['split'] == 'train']
    else:
        train_df = metadata

    print(f"Train files: {len(train_df)}")

    # ── datasets & loaders ──────────────────────────────────────────────────
    train_ds = build_dataset(train_df, args.signals, med_labels, med_categories, args.task, data_dir=args.data_dir)

    train_loader = DataLoader(train_ds, batch_size=args.batch_size,
                              num_workers=0, pin_memory=False)

    n_labels = train_ds.n_input_targets + train_ds.n_category_targets
    print(f"n_labels={n_labels}  (inputs={train_ds.n_input_targets}, categories={train_ds.n_category_targets})")

    # ── model ───────────────────────────────────────────────────────────────
    device = torch.device(args.device)
    # pos_weight = torch.full((n_labels,), 3.0).to(device)
    pos_weight = None
   
    model  = ResNet50(in_channels=len(args.signals), classes=n_labels).to(device)
    n_params = sum(p.numel() for p in model.parameters()) / 1e6
    print(f"Model params: {n_params:.1f}M  |  device: {device}")

    optimizer = torch.optim.Adam(model.parameters(), lr=args.lr)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode='min', factor=0.5, patience=3
    )

    # ── training loop ───────────────────────────────────────────────────────
    best_train_loss = float('inf')

    for epoch in range(1, args.epochs + 1):
        t0 = time.time()

        train_loss = train_one_epoch(model, train_loader, optimizer, device, pos_weight)
        scheduler.step(train_loss)

        elapsed = time.time() - t0
        print(
            f"epoch {epoch:3d}/{args.epochs} | "
            f"train loss {train_loss:.4f} | "
            f"{elapsed:.0f}s"
        )

        if train_loss < best_train_loss:
            best_train_loss = train_loss
            torch.save({
                'epoch': epoch,
                'model': model.state_dict(),
                'train_loss': train_loss,
            }, os.path.join(args.checkpoint_dir, 'best.pt'))
            print(f"  → checkpoint saved (train_loss={train_loss:.4f})")

    print(f"\nDone. Best train loss: {best_train_loss:.4f}")


if __name__ == '__main__':
    main()
