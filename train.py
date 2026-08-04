import argparse
import os
import time

import pandas as pd
import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader

from icu_dataset import ICUDataset
from models.resnet import ResNet50
from overfit import masked_bce_loss


def train_one_epoch(model, loader, optimizer, device):
    model.train()
    total_loss = 0.0
    total_correct = 0
    total_valid = 0
    n_batches = 0

    for waveform, targets in loader:
        waveform = waveform.to(device)
        targets  = targets.to(device)

        optimizer.zero_grad()
        logits = model(waveform)
        loss   = masked_bce_loss(logits, targets)
        loss.backward()
        optimizer.step()

        with torch.no_grad():
            mask = targets != -1
            preds = (torch.sigmoid(logits) > 0.5).float()
            total_correct += (preds[mask] == targets[mask]).sum().item()
            total_valid   += mask.sum().item()
            total_loss    += loss.item()
            n_batches     += 1

    avg_loss = total_loss / max(n_batches, 1)
    acc      = total_correct / max(total_valid, 1)
    return avg_loss, acc


@torch.no_grad()
def evaluate(model, loader, device):
    model.eval()
    total_loss = 0.0
    total_correct = 0
    total_valid = 0
    n_batches = 0

    for waveform, targets in loader:
        waveform = waveform.to(device)
        targets  = targets.to(device)

        logits = model(waveform)
        loss   = masked_bce_loss(logits, targets)

        mask = targets != -1
        preds = (torch.sigmoid(logits) > 0.5).float()
        total_correct += (preds[mask] == targets[mask]).sum().item()
        total_valid   += mask.sum().item()
        total_loss    += loss.item()
        n_batches     += 1

    avg_loss = total_loss / max(n_batches, 1)
    acc      = total_correct / max(total_valid, 1)
    return avg_loss, acc


def build_dataset(metadata, signals, med_labels, med_categories, task):
    return ICUDataset(
        metadata=metadata,
        signals=signals,
        med_labels=med_labels,
        med_categories=med_categories,
        task=task,
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--metadata',      default='data/metadata.csv')
    parser.add_argument('--task',          default='input_classification',
                        choices=['input_classification', 'category_classification'])
    parser.add_argument('--signals',       nargs='+', default=['PLETH', 'II', 'ABP'])
    parser.add_argument('--epochs',        type=int,   default=20)
    parser.add_argument('--batch_size',    type=int,   default=32)
    parser.add_argument('--lr',            type=float, default=1e-3)
    parser.add_argument('--num_workers',   type=int,   default=4)
    parser.add_argument('--val_split',     type=float, default=0.1,
                        help='Fraction of files held out for validation')
    parser.add_argument('--checkpoint_dir', default='checkpoints')
    parser.add_argument('--device',        default='cuda' if torch.cuda.is_available() else 'cpu')
    args = parser.parse_args()

    os.makedirs(args.checkpoint_dir, exist_ok=True)

    # ── labels ──────────────────────────────────────────────────────────────
    med_labels = [
        'diltiazem', 'cisatracurium', 'lorazepam', 'nitroglycerin', 'fentanyl',
        'midazolam', 'meperidine', 'ketamine', 'esmolol', 'metoprolol',
        'angiotensin_ii', 'propofol', 'mannitol', 'hydromorphone', 'nicardipine',
        'dopamine', 'labetalol', 'vasopressin', 'nitroprusside', 'epinephrine',
        'dexmedetomidine', 'acetaminophen_iv', 'epoprostenol', 'haloperidol',
        'norepinephrine', 'digoxin', 'amiodarone', 'dobutamine', 'furosemide',
        'milrinone', 'bumetanide', 'morphine', 'hydralazine', 'phenylephrine',
    ]
    med_categories = [
        'vasopressor', 'antiarrhythmic', 'vasoactive', 'negative_inotrope',
        'diuretic', 'vasodilator', 'positive_inotrope', 'analgesic', 'nm_blocker',
    ]

    # ── data split ──────────────────────────────────────────────────────────
    metadata = pd.read_csv(args.metadata)
    metadata = metadata[metadata['has_inputs'] == 1].reset_index(drop=True)

    if 'split' in metadata.columns:
        train_df = metadata[(metadata['has_inputs'] == 1) & (metadata['split'] == 'train')]
        val_df   = metadata[(metadata['has_inputs'] == 1) & (metadata['split'] == 'train')]
    else:
        n_val    = max(1, int(len(metadata) * args.val_split))
        val_df   = metadata.iloc[:n_val]
        train_df = metadata.iloc[n_val:]

    print(f"Train files: {len(train_df)}  |  Val files: {len(val_df)}")

    # ── datasets & loaders ──────────────────────────────────────────────────
    train_ds = build_dataset(train_df, args.signals, med_labels, med_categories, args.task)
    val_ds   = build_dataset(val_df,   args.signals, med_labels, med_categories, args.task)

    train_loader = DataLoader(train_ds, batch_size=args.batch_size,
                              num_workers=args.num_workers, pin_memory=True)
    val_loader   = DataLoader(val_ds,   batch_size=args.batch_size,
                              num_workers=args.num_workers, pin_memory=True)

    n_labels = train_ds.n_input_targets + train_ds.n_category_targets
    print(f"n_labels={n_labels}  (inputs={train_ds.n_input_targets}, categories={train_ds.n_category_targets})")

    # ── model ───────────────────────────────────────────────────────────────
    device = torch.device(args.device)
    model  = ResNet50(in_channels=len(args.signals), classes=n_labels).to(device)
    n_params = sum(p.numel() for p in model.parameters()) / 1e6
    print(f"Model params: {n_params:.1f}M  |  device: {device}")

    optimizer = torch.optim.Adam(model.parameters(), lr=args.lr)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode='min', factor=0.5, patience=3, verbose=True
    )

    # ── training loop ───────────────────────────────────────────────────────
    best_val_loss = float('inf')
    for epoch in range(1, args.epochs + 1):
        t0 = time.time()

        train_loss, train_acc = train_one_epoch(model, train_loader, optimizer, device)
        val_loss,   val_acc   = evaluate(model, val_loader, device)
        scheduler.step(val_loss)

        elapsed = time.time() - t0
        print(
            f"epoch {epoch:3d}/{args.epochs} | "
            f"train loss {train_loss:.4f}  acc {train_acc:.3f} | "
            f"val loss {val_loss:.4f}  acc {val_acc:.3f} | "
            f"{elapsed:.0f}s"
        )

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            ckpt_path = os.path.join(args.checkpoint_dir, 'best.pt')
            torch.save({'epoch': epoch, 'model': model.state_dict(),
                        'val_loss': val_loss, 'val_acc': val_acc}, ckpt_path)
            print(f"  → saved checkpoint (val_loss={val_loss:.4f})")

    print(f"\nDone. Best val loss: {best_val_loss:.4f}")


if __name__ == '__main__':
    main()
