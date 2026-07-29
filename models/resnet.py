import torch
import torch.nn as nn


class ResBlock1d(nn.Module):
    expansion = 1

    def __init__(self, in_channels, out_channels, stride=1, downsample=None):
        super().__init__()
        self.conv1 = nn.Conv1d(in_channels, out_channels, kernel_size=3, stride=stride, padding=1, bias=False)
        self.bn1   = nn.BatchNorm1d(out_channels)
        self.relu  = nn.ReLU(inplace=True)
        self.conv2 = nn.Conv1d(out_channels, out_channels, kernel_size=3, stride=1, padding=1, bias=False)
        self.bn2   = nn.BatchNorm1d(out_channels)
        self.downsample = downsample

    def forward(self, x):
        identity = x
        out = self.relu(self.bn1(self.conv1(x)))
        out = self.bn2(self.conv2(out))
        if self.downsample is not None:
            identity = self.downsample(x)
        return self.relu(out + identity)


class ResBottleneck1d(nn.Module):
    expansion = 4

    def __init__(self, in_channels, out_channels, stride=1, downsample=None):
        super().__init__()
        self.conv1 = nn.Conv1d(in_channels, out_channels, kernel_size=1, bias=False)
        self.bn1   = nn.BatchNorm1d(out_channels)
        self.conv2 = nn.Conv1d(out_channels, out_channels, kernel_size=3, stride=stride, padding=1, bias=False)
        self.bn2   = nn.BatchNorm1d(out_channels)
        self.conv3 = nn.Conv1d(out_channels, out_channels * self.expansion, kernel_size=1, bias=False)
        self.bn3   = nn.BatchNorm1d(out_channels * self.expansion)
        self.relu  = nn.ReLU(inplace=True)
        self.downsample = downsample

    def forward(self, x):
        identity = x
        out = self.relu(self.bn1(self.conv1(x)))
        out = self.relu(self.bn2(self.conv2(out)))
        out = self.bn3(self.conv3(out))
        if self.downsample is not None:
            identity = self.downsample(x)
        return self.relu(out + identity)


class WaveformEncoder(nn.Module):
    """
    ResNet-50-style 1D encoder for multi-channel waveform input.

    Input : (B, n_signals, chunk_size)
    Output: (B, embed_dim)
    """

    _LAYERS = [3, 4, 6, 3]  # ResNet-50 block counts

    def __init__(self, n_signals: int, base_channels: int = 64, embed_dim: int = 512):
        super().__init__()
        self.embed_dim = embed_dim
        block = ResBottleneck1d

        # Wide stem kernel to capture 125 Hz waveform structure before striding
        self.stem = nn.Sequential(
            nn.Conv1d(n_signals, base_channels, kernel_size=15, stride=4, padding=7, bias=False),
            nn.BatchNorm1d(base_channels),
            nn.ReLU(inplace=True),
            nn.MaxPool1d(kernel_size=3, stride=2, padding=1),
        )

        self.in_channels = base_channels
        self.layer1 = self._make_layer(block, base_channels * 1, self._LAYERS[0], stride=1)
        self.layer2 = self._make_layer(block, base_channels * 2, self._LAYERS[1], stride=2)
        self.layer3 = self._make_layer(block, base_channels * 4, self._LAYERS[2], stride=2)
        self.layer4 = self._make_layer(block, base_channels * 8, self._LAYERS[3], stride=2)

        self.pool    = nn.AdaptiveAvgPool1d(1)
        self.project = nn.Linear(base_channels * 8 * block.expansion, embed_dim)

    def _make_layer(self, block, out_channels, n_blocks, stride):
        downsample = None
        if stride != 1 or self.in_channels != out_channels * block.expansion:
            downsample = nn.Sequential(
                nn.Conv1d(self.in_channels, out_channels * block.expansion, kernel_size=1, stride=stride, bias=False),
                nn.BatchNorm1d(out_channels * block.expansion),
            )
        layers = [block(self.in_channels, out_channels, stride=stride, downsample=downsample)]
        self.in_channels = out_channels * block.expansion
        for _ in range(1, n_blocks):
            layers.append(block(self.in_channels, out_channels))
        return nn.Sequential(*layers)

    def forward(self, x):
        x = self.stem(x)
        x = self.layer1(x)
        x = self.layer2(x)
        x = self.layer3(x)
        x = self.layer4(x)
        return self.project(self.pool(x).squeeze(-1))


class EHRPool(nn.Module):
    """
    MLP that projects the flat EHR vector (labs + demographics) into the
    waveform embedding space. NaNs (missing labs) are zeroed before the
    first linear layer.
    """

    def __init__(self, n_features: int, embed_dim: int, hidden_dim: int = 128, dropout: float = 0.1):
        super().__init__()
        self.net = nn.Sequential(
            nn.Linear(n_features, hidden_dim),
            nn.ReLU(inplace=True),
            nn.Dropout(dropout),
            nn.Linear(hidden_dim, embed_dim),
        )

    def forward(self, x):
        return self.net(torch.nan_to_num(x, nan=0.0))


class ICUModel(nn.Module):
    """
    Shared ResNet-50 1D waveform encoder with an optional EHR pool for
    lab / demographic features, followed by a task-specific linear head.

    Parameters
    ----------
    n_signals       : waveform channels (e.g. 3 for PLETH/II/ABP)
    n_outputs       : output labels for the task head
    task            : 'input_cls' | 'category_cls'
    n_ehr_features  : flat EHR vector length; 0 to omit the EHR pool
    base_channels   : ResNet base width (64 → standard ResNet-50 widths)
    embed_dim       : shared embedding dim after encoder and EHR pool
    ehr_fusion      : 'concat' | 'add' | 'none'
                      'concat' doubles head input dim; 'add' requires same dim
    """

    def __init__(
        self,
        n_signals: int,
        n_outputs: int,
        task: str = 'category_cls',
        n_ehr_features: int = 0,
        base_channels: int = 64,
        embed_dim: int = 512,
        ehr_hidden_dim: int = 128,
        ehr_dropout: float = 0.1,
        ehr_fusion: str = 'concat',
    ):
        super().__init__()
        assert task in ('input_cls', 'category_cls'), f"Unknown task: {task}"
        assert ehr_fusion in ('add', 'concat', 'none')

        self.task       = task
        self.ehr_fusion = ehr_fusion

        self.encoder  = WaveformEncoder(n_signals, base_channels=base_channels, embed_dim=embed_dim)
        self.ehr_pool = None

        head_in = embed_dim
        if ehr_fusion != 'none' and n_ehr_features > 0:
            self.ehr_pool = EHRPool(n_ehr_features, embed_dim, hidden_dim=ehr_hidden_dim, dropout=ehr_dropout)
            if ehr_fusion == 'concat':
                head_in = embed_dim * 2

        self.head = nn.Sequential(
            nn.LayerNorm(head_in),
            nn.Linear(head_in, n_outputs),
        )

    def forward(self, waveform: torch.Tensor, ehr: torch.Tensor | None = None) -> torch.Tensor:
        """
        Parameters
        ----------
        waveform : (B, n_signals, chunk_size)
        ehr      : (B, n_ehr_features) or None

        Returns
        -------
        logits : (B, n_outputs)
        """
        z = self.encoder(waveform)

        if self.ehr_pool is not None and ehr is not None:
            z_ehr = self.ehr_pool(ehr)
            z = torch.cat([z, z_ehr], dim=-1) if self.ehr_fusion == 'concat' else z + z_ehr

        return self.head(z)


if __name__ == '__main__':
    B          = 4
    n_signals  = 3
    chunk_size = 7500   # 60 s @ 125 Hz
    n_ehr      = 48 * 3  # 48 labs × (value, abnormal, last_drawn_hrs)
    n_inputs   = 50
    n_cats     = 9

    waveform = torch.randn(B, n_signals, chunk_size)
    ehr      = torch.randn(B, n_ehr)
    ehr[0, :10] = float('nan')  # simulate missing labs

    for task, n_out in [('input_cls', n_inputs), ('category_cls', n_cats)]:
        for fusion in ('none', 'concat', 'add'):
            model = ICUModel(
                n_signals=n_signals,
                n_outputs=n_out,
                task=task,
                n_ehr_features=n_ehr if fusion != 'none' else 0,
                ehr_fusion=fusion,
            )
            n_params = sum(p.numel() for p in model.parameters()) / 1e6
            logits = model(waveform, ehr if fusion != 'none' else None)
            print(f"[{task:<14} | ehr={fusion:<6}]  out={tuple(logits.shape)}  params={n_params:.1f}M")
