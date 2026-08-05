import pandas as pd
import torch
import torch.nn.functional as F
from torch.utils.data import DataLoader

# single batch to overfit on
def get_overfit_batch(dataset, batch_size=8, max_search=2000):
    samples = []
    for i, sample in enumerate(dataset):
        waveform, target = sample
        if (target == 1).any():  # look for actual positive labels, not -1
            samples.append(sample)  # only keep samples with at least one active label
            print(target)
        if len(samples) >= batch_size:
            break
        if i >= max_search:
            print(f"Warning: only found {len(samples)} nonzero samples in {max_search} chunks")
            break
    
    waveforms = torch.stack([s[0] for s in samples])
    targets   = torch.stack([s[1] for s in samples])
    return waveforms, targets

def masked_bce_loss(logits, targets):
    mask = targets != -1                          # (B, n_labels) bool
    logits  = logits[mask]
    targets = targets[mask]
    print(f"active labels: {mask.sum().item()} / {mask.numel()}")
    return F.binary_cross_entropy_with_logits(logits, targets)


def overfit(model, dataset, n_steps=500, batch_size=8, lr=1e-3, device='cpu'):
    model = model.to(device)
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)

    # grab a fixed batch and keep reusing it
    waveforms, targets = get_overfit_batch(dataset, batch_size)
    print("waveform NaNs:", torch.isnan(waveforms).sum().item())
    print("target NaNs:", torch.isnan(targets).sum().item())
    print("target sum:", targets.sum().item())
    waveforms = waveforms.to(device)
    targets   = targets.to(device)

    model.train()
    for step in range(n_steps):
        optimizer.zero_grad()
        logits = model(waveforms)                                       # (B, n_labels)
        loss   = masked_bce_loss(logits, targets)
        loss.backward()
        optimizer.step()

        if step % 50 == 0:
            with torch.no_grad():
                mask = targets != -1
                preds = (torch.sigmoid(logits) > 0.5).float()
                acc = (preds[mask] == targets[mask]).float().mean().item()
            print(f"step {step:4d} | loss {loss.item():.4f} | acc {acc:.3f}")


if __name__ == '__main__':
    from icu_dataset import ICUDataset
    from models.resnet import ResNet50

    metadata  = pd.read_csv('./data/metadata.csv')
    train_df  = metadata[(metadata['split'] == 'train') & (metadata['has_inputs'] == 1)]

    dataset = ICUDataset(
        metadata   = train_df,
        signals    = ['PLETH', 'II', 'ABP'],
        med_labels = ['diltiazem', 'cisatracurium', 'lorazepam', 'nitroglycerin', 'fentanyl', 'midazolam', 'meperidine', 'ketamine', 'esmolol', 'metoprolol', 'angiotensin_ii', 'propofol', 'mannitol', 'hydromorphone', 'nicardipine', 'dopamine', 'labetalol', 'vasopressin', 'nitroprusside', 'epinephrine', 'dexmedetomidine', 'acetaminophen_iv', 'epoprostenol', 'haloperidol', 'norepinephrine', 'digoxin', 'amiodarone', 'dobutamine', 'furosemide', 'milrinone', 'bumetanide', 'morphine', 'hydralazine', 'phenylephrine'],
        med_categories = ['vasopressor', 'antiarrhythmic', 'vasoactive', 'negative_inotrope', 'diuretic', 'vasodilator', 'positive_inotrope', 'analgesic', 'nm_blocker'],
        task       = 'input_classification'
    )

    n_labels = dataset.n_input_targets + dataset.n_category_targets
    model    = ResNet50(in_channels=3, classes=n_labels)

    overfit(model, dataset, n_steps=500, batch_size=8, lr=1e-3)