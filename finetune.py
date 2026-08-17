import numpy as np
import pandas as pd
import torch
import argparse
import time
import torch.nn.functional as F
import os
from tqdm import tqdm
from icu_dataset import ICUDatasetICD
from models.resnet import FineTuneModel
from torch.utils.data import DataLoader
from train import compute_auroc_auprc
from sklearn.preprocessing import MultiLabelBinarizer

def finetune(checkpoint_path, checkpoint_dir, icd_class_path, metadata_path, data_dir, signals,
             med_labels, med_categories, epochs=30, batch_size=32,
             lr=1e-3, device='cpu'):

    metadata   = pd.read_csv(metadata_path)
    icd_classes = np.load(icd_class_path, allow_pickle=True)
    n_icd_codes = len(icd_classes)

    # refit binarizer to get icd_matrix aligned with metadata
    icd_lists  = metadata['icd10_truncated'].fillna('').str.split('|').tolist()
    icd_lists  = [[c.strip() for c in codes if c.strip()] for codes in icd_lists]
    mlb        = MultiLabelBinarizer(classes=icd_classes)
    icd_matrix = mlb.fit_transform(icd_lists)

    train_meta = metadata[metadata['split'] == 'train'].reset_index(drop=True)
    # val_meta   = metadata[metadata['split'] == 'val'].reset_index(drop=True)

    train_icd  = icd_matrix[train_meta.index]
    # val_icd    = icd_matrix[val_meta.index]

    train_ds = ICUDatasetICD(train_meta, train_icd, signals, data_dir)
    # val_ds   = ICUDatasetICD(val_meta,   val_icd,   signals, data_dir)

    train_loader = DataLoader(train_ds, batch_size=batch_size, num_workers=0)
    # val_loader   = DataLoader(val_ds,   batch_size=batch_size, num_workers=0)

    model = FineTuneModel(
        pretrained_checkpoint = checkpoint_path,
        n_signals             = len(signals),
        n_med_labels          = len(med_labels),
        n_med_categories      = len(med_categories),
        n_icd_codes           = n_icd_codes,
        freeze_encoder        = True
    ).to(device)

    # only train head parameters
    optimizer = torch.optim.Adam(
        filter(lambda p: p.requires_grad, model.parameters()), lr=lr
    )
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(optimizer, mode='min', factor=0.5, patience=3)

    # best_val_loss = float('inf')
    best_train_loss = float('inf')

    for epoch in range(1, epochs + 1):
        t0 = time.time()

        # train
        model.train()
        total_loss = 0
        n_batches  = 0
        pbar = tqdm(train_loader, desc='train', leave=False)
        for waveform, targets in pbar:
            waveform = waveform.to(device)
            targets  = targets.to(device)
            optimizer.zero_grad()
            logits = model(waveform)
            loss   = F.binary_cross_entropy_with_logits(logits, targets)
            loss.backward()
            torch.nn.utils.clip_grad_norm_(model.parameters(), max_norm=1.0)
            optimizer.step()
            total_loss += loss.item()
            n_batches  += 1
            pbar.set_postfix({'loss': f'{total_loss/n_batches:.4f}'})

        train_loss = total_loss / max(n_batches, 1)

        # evaluate
        # model.eval()
        # val_loss  = 0
        # n_batches = 0
        # all_logits  = []
        # all_targets = []

        # with torch.no_grad():
        #     for waveform, targets in tqdm(val_loader, desc='val', leave=False):
        #         waveform = waveform.to(device)
        #         targets  = targets.to(device)
        #         logits   = model(waveform)
        #         loss     = F.binary_cross_entropy_with_logits(logits, targets)
        #         val_loss += loss.item()
        #         n_batches += 1
        #         all_logits.append(logits.cpu())
        #         all_targets.append(targets.cpu())

        # val_loss   = val_loss / max(n_batches, 1)
        # all_logits  = torch.cat(all_logits,  dim=0)
        # all_targets = torch.cat(all_targets, dim=0)

        # _, _, macro_auroc, macro_auprc = compute_auroc_auprc(
        #     all_logits, all_targets, list(icd_classes)
        # )
        scheduler.step(train_loss)

        # print(f"epoch {epoch:3d}/{epochs} | train {train_loss:.4f} | "
        #       f"val {val_loss:.4f} | AUROC {macro_auroc:.3f} | "
        #       f"AUPRC {macro_auprc:.3f} | {time.time()-t0:.0f}s")
        elapsed = time.time() - t0
        print(
            f"epoch {epoch:3d}/{epochs} | "
            f"train loss {train_loss:.4f} | "
            f"{elapsed:.0f}s"
        )

        # if val_loss < best_val_loss:
        #     best_val_loss = val_loss
        #     torch.save({
        #         'epoch': epoch, 'model': model.state_dict(),
        #         'val_loss': val_loss, 'macro_auroc': macro_auroc,
        #         'macro_auprc': macro_auprc
        #     }, os.path.join(checkpoint_dir, 'best_val_icd.pt'))
        #     print(f"  → saved checkpoint")
        
        if train_loss < best_train_loss:
            best_train_loss = train_loss
            torch.save({
                'epoch': epoch,
                'model': model.state_dict(),
                'train_loss': train_loss,
            }, os.path.join(checkpoint_dir, 'best_train_icd.pt'))
            print(f"  → saved train checkpoint (train_loss={train_loss:.4f})")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--checkpoint_path',   default='checkpoints/best.pt')
    parser.add_argument('--metadata',         default='data/metadata.csv')
    parser.add_argument('--data_dir',         default='./data/data')
    parser.add_argument('--icd_class_path',      default='data/icd_classes.npy')
    parser.add_argument('--checkpoint_dir',   default='checkpoints')
    parser.add_argument('--signals',          nargs='+', default=['PLETH', 'II', 'ABP'])
    parser.add_argument('--epochs',           type=int,   default=30)
    parser.add_argument('--batch_size',       type=int,   default=32)
    parser.add_argument('--lr',               type=float, default=1e-3)
    parser.add_argument('--device',           default='cuda' if torch.cuda.is_available() else 'cpu')
    args = parser.parse_args()

    os.makedirs(args.checkpoint_dir, exist_ok=True)

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

    finetune(
        checkpoint_path = args.checkpoint_path,
        metadata_path   = args.metadata,
        data_dir        = args.data_dir,
        icd_class_path  = args.icd_class_path,
        checkpoint_dir  = args.checkpoint_dir,
        signals         = args.signals,
        med_labels      = med_labels,
        med_categories  = med_categories,
        epochs          = args.epochs,
        batch_size      = args.batch_size,
        lr              = args.lr,
        device          = args.device,
    )

if __name__ == '__main__':
    main()