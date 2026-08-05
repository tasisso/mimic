import argparse
import os
import time

import pandas as pd
import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader

from icu_dataset import ICUDataset
from models.resnet import ResNet50

def masked_bce_loss(logits, targets, pos_weight=None):
    mask = targets != -1                          # (B, n_labels) bool
    logits  = logits[mask]
    targets = targets[mask]
    return F.binary_cross_entropy_with_logits(logits, targets, pos_weight=pos_weight)

def train_one_epoch(model, loader, optimizer, device, pos_weight):
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
        loss   = masked_bce_loss(logits, targets, pos_weight)
        loss.backward()
        torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
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
def evaluate(model, loader, device, n_input_targets, n_category_targets, pos_weight):
    model.eval()
    total_loss = 0
    n_batches  = 0
    
    all_logits  = []
    all_targets = []

    with torch.no_grad():
        for waveform, targets in loader:
            waveform = waveform.to(device)
            targets  = targets.to(device)

            logits = model(waveform)
            loss   = masked_bce_loss(logits, targets, pos_weight)
            total_loss += loss.item()
            n_batches  += 1

            all_logits.append(logits.cpu())
            all_targets.append(targets.cpu())

    all_logits  = torch.cat(all_logits,  dim=0)  # (N, n_labels)
    all_targets = torch.cat(all_targets, dim=0)  # (N, n_labels)

    # split by label group
    input_logits    = all_logits[:,  :n_input_targets]
    input_targets   = all_targets[:, :n_input_targets]
    cat_logits      = all_logits[:,  n_input_targets:]
    cat_targets     = all_targets[:, n_input_targets:]

    def masked_acc(logits, targets):
        mask  = targets != -1
        preds = (torch.sigmoid(logits) > 0.5).float()
        return (preds[mask] == targets[mask]).float().mean().item()


    overall_acc  = masked_acc(all_logits,    all_targets)

    input_acc, input_prec, input_rec = masked_metrics(input_logits, input_targets)
    cat_acc,   cat_prec,   cat_rec   = masked_metrics(cat_logits,   cat_targets)

    return total_loss / n_batches, overall_acc, input_acc, input_prec, input_rec, cat_acc, cat_prec, cat_rec


def build_dataset(metadata, signals, med_labels, med_categories, task, data_dir):
    return ICUDataset(
        metadata=metadata,
        signals=signals,
        med_labels=med_labels,
        med_categories=med_categories,
        task=task,
        data_dir=data_dir
    )

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
        val_df   = metadata[(metadata['has_inputs'] == 1) & (metadata['split'] == 'val')]
    else:
        n_val    = max(1, int(len(metadata) * args.val_split))
        val_df   = metadata.iloc[:n_val]
        train_df = metadata.iloc[n_val:]

    print(f"Train files: {len(train_df)}  |  Val files: {len(val_df)}")

    # ── datasets & loaders ──────────────────────────────────────────────────
    train_ds = build_dataset(train_df, args.signals, med_labels, med_categories, args.task, data_dir=args.data_dir)
    val_ds   = build_dataset(val_df,   args.signals, med_labels, med_categories, args.task, data_dir=args.data_dir)

    train_loader = DataLoader(train_ds, batch_size=args.batch_size,
                              num_workers=args.num_workers, pin_memory=True)
    val_loader   = DataLoader(val_ds,   batch_size=args.batch_size,
                              num_workers=args.num_workers, pin_memory=True)

    n_labels = train_ds.n_input_targets + train_ds.n_category_targets
    print(f"n_labels={n_labels}  (inputs={train_ds.n_input_targets}, categories={train_ds.n_category_targets})")

    # ── model ───────────────────────────────────────────────────────────────
    device = torch.device(args.device)
    pos_weight = torch.tensor([19.0]).to(device)
    print(f'Positive weight: {pos_weight}')
    model  = ResNet50(in_channels=len(args.signals), classes=n_labels).to(device)
    n_params = sum(p.numel() for p in model.parameters()) / 1e6
    print(f"Model params: {n_params:.1f}M  |  device: {device}")

    optimizer = torch.optim.Adam(model.parameters(), lr=args.lr)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode='min', factor=0.5, patience=3
    )

    # ── training loop ───────────────────────────────────────────────────────
    best_val_loss = float('inf')
    for epoch in range(1, args.epochs + 1):
        t0 = time.time()

        train_loss, train_acc = train_one_epoch(model, train_loader, optimizer, device, pos_weight)
        val_loss, overall_acc, input_acc, input_prec, input_rec, cat_acc, cat_prec, cat_rec = evaluate(
            model, val_loader, device, 
            len(med_labels), 
            len(med_categories),
            pos_weight
        )
        scheduler.step(val_loss)

        elapsed = time.time() - t0
        print(
            f"epoch {epoch:3d}/{args.epochs} | "
            f"train loss {train_loss:.4f} | "
            f"val loss {val_loss:.4f} | \n"
            f"  input    acc {input_acc:.3f}  prec {input_prec:.3f}  rec {input_rec:.3f} | \n"
            f"  category acc {cat_acc:.3f}  prec {cat_prec:.3f}  rec {cat_rec:.3f}"
            f"{elapsed:.0f}s"
        )

        if val_loss < best_val_loss:
            best_val_loss = val_loss
            ckpt_path = os.path.join(args.checkpoint_dir, 'best.pt')
            torch.save({'epoch': epoch, 'model': model.state_dict(),
                        'val_loss': val_loss, 'val_acc': overall_acc}, ckpt_path)
            print(f"  → saved checkpoint (val_loss={val_loss:.4f})")

    print(f"\nDone. Best val loss: {best_val_loss:.4f}")


if __name__ == '__main__':
    main()
