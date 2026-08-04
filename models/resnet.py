import torch
import torch.nn as nn

class Bottlrneck(nn.Module):
    def __init__(self,In_channel,Med_channel,Out_channel,downsample=False):
        super(Bottlrneck, self).__init__()
        self.stride = 1
        if downsample == True:
            self.stride = 2

        self.layer = nn.Sequential(
            nn.Conv1d(In_channel, Med_channel, 1, self.stride),
            nn.BatchNorm1d(Med_channel),
            nn.ReLU(),
            nn.Conv1d(Med_channel, Med_channel, 3, padding=1),
            nn.BatchNorm1d(Med_channel),
            nn.ReLU(),
            nn.Conv1d(Med_channel, Out_channel, 1),
            nn.BatchNorm1d(Out_channel),
            nn.ReLU(),
        )

        if In_channel != Out_channel:
            self.res_layer = nn.Conv1d(In_channel, Out_channel,1,self.stride)
        else:
            self.res_layer = None

    def forward(self,x):
        if self.res_layer is not None:
            residual = self.res_layer(x)
        else:
            residual = x
        return self.layer(x)+residual


class ResNet50(nn.Module):
    def __init__(self,in_channels=2,classes=125):
        super(ResNet50, self).__init__()
        self.features = nn.Sequential(
            nn.Conv1d(in_channels,64,kernel_size=7,stride=2,padding=3),
            nn.MaxPool1d(3,2,1),

            Bottlrneck(64,64,256,False),
            Bottlrneck(256,64,256,False),
            Bottlrneck(256,64,256,False),
            #
            Bottlrneck(256,128,512, True),
            Bottlrneck(512,128,512, False),
            Bottlrneck(512,128,512, False),
            Bottlrneck(512,128,512, False),
            #
            Bottlrneck(512,256,1024, True),
            Bottlrneck(1024,256,1024, False),
            Bottlrneck(1024,256,1024, False),
            Bottlrneck(1024,256,1024, False),
            Bottlrneck(1024,256,1024, False),
            Bottlrneck(1024,256,1024, False),
            #
            Bottlrneck(1024,512,2048, True),
            Bottlrneck(2048,512,2048, False),
            Bottlrneck(2048,512,2048, False),

            nn.AdaptiveAvgPool1d(1)
        )
        self.classifer = nn.Sequential(
            nn.Linear(2048,classes)
        )

    def forward(self,x):
        x = self.features(x)
        x = x.view(-1,2048)
        x = self.classifer(x)
        return x

if __name__ == '__main__':
    x = torch.randn(size=(1,1,224))
    # x = torch.randn(size=(1,64,224))
    # model = Bottlrneck(64,64,256,True)
    model = ResNet50(in_channels=1,classes=5)

    output = model(x)
    print(output.shape)




# class EHRPool(nn.Module):
#     """
#     MLP that projects the flat EHR vector (labs + demographics) into the
#     waveform embedding space. NaNs (missing labs) are zeroed before the
#     first linear layer.
#     """

#     def __init__(self, n_features: int, embed_dim: int, hidden_dim: int = 128, dropout: float = 0.1):
#         super().__init__()
#         self.net = nn.Sequential(
#             nn.Linear(n_features, hidden_dim),
#             nn.ReLU(inplace=True),
#             nn.Dropout(dropout),
#             nn.Linear(hidden_dim, embed_dim),
#         )

#     def forward(self, x):
#         return self.net(torch.nan_to_num(x, nan=0.0))


# class ICUModel(nn.Module):
#     """
#     Shared ResNet-50 1D waveform encoder with an optional EHR pool for
#     lab / demographic features, followed by a task-specific linear head.

#     Parameters
#     ----------
#     n_signals       : waveform channels (e.g. 3 for PLETH/II/ABP)
#     n_outputs       : output labels for the task head
#     task            : 'input_cls' | 'category_cls'
#     n_ehr_features  : flat EHR vector length; 0 to omit the EHR pool
#     base_channels   : ResNet base width (64 → standard ResNet-50 widths)
#     embed_dim       : shared embedding dim after encoder and EHR pool
#     ehr_fusion      : 'concat' | 'add' | 'none'
#                       'concat' doubles head input dim; 'add' requires same dim
#     """

#     def __init__(
#         self,
#         n_signals: int,
#         n_outputs: int,
#         task: str = 'category_cls',
#         n_ehr_features: int = 0,
#         base_channels: int = 64,
#         embed_dim: int = 512,
#         ehr_hidden_dim: int = 128,
#         ehr_dropout: float = 0.1,
#         ehr_fusion: str = 'concat',
#     ):
#         super().__init__()
#         assert task in ('input_cls', 'category_cls'), f"Unknown task: {task}"
#         assert ehr_fusion in ('add', 'concat', 'none')

#         self.task       = task
#         self.ehr_fusion = ehr_fusion

#         self.encoder  = WaveformEncoder(n_signals, base_channels=base_channels, embed_dim=embed_dim)
#         self.ehr_pool = None

#         head_in = embed_dim
#         if ehr_fusion != 'none' and n_ehr_features > 0:
#             self.ehr_pool = EHRPool(n_ehr_features, embed_dim, hidden_dim=ehr_hidden_dim, dropout=ehr_dropout)
#             if ehr_fusion == 'concat':
#                 head_in = embed_dim * 2

#         self.head = nn.Sequential(
#             nn.LayerNorm(head_in),
#             nn.Linear(head_in, n_outputs),
#         )

#     def forward(self, waveform: torch.Tensor, ehr: torch.Tensor | None = None) -> torch.Tensor:
#         """
#         Parameters
#         ----------
#         waveform : (B, n_signals, chunk_size)
#         ehr      : (B, n_ehr_features) or None

#         Returns
#         -------
#         logits : (B, n_outputs)
#         """
#         z = self.encoder(waveform)

#         if self.ehr_pool is not None and ehr is not None:
#             z_ehr = self.ehr_pool(ehr)
#             z = torch.cat([z, z_ehr], dim=-1) if self.ehr_fusion == 'concat' else z + z_ehr

#         return self.head(z)


# if __name__ == '__main__':
#     B          = 4
#     n_signals  = 3
#     chunk_size = 7500   # 60 s @ 125 Hz
#     n_ehr      = 48 * 3  # 48 labs × (value, abnormal, last_drawn_hrs)
#     n_inputs   = 50
#     n_cats     = 9

#     waveform = torch.randn(B, n_signals, chunk_size)
#     ehr      = torch.randn(B, n_ehr)
#     ehr[0, :10] = float('nan')  # simulate missing labs

#     for task, n_out in [('input_cls', n_inputs), ('category_cls', n_cats)]:
#         for fusion in ('none', 'concat', 'add'):
#             model = ICUModel(
#                 n_signals=n_signals,
#                 n_outputs=n_out,
#                 task=task,
#                 n_ehr_features=n_ehr if fusion != 'none' else 0,
#                 ehr_fusion=fusion,
#             )
#             n_params = sum(p.numel() for p in model.parameters()) / 1e6
#             logits = model(waveform, ehr if fusion != 'none' else None)
#             print(f"[{task:<14} | ehr={fusion:<6}]  out={tuple(logits.shape)}  params={n_params:.1f}M")
