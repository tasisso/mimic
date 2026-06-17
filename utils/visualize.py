from mpl_toolkits.axes_grid1 import make_axes_locatable
import h5py
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt
from matplotlib.widgets import Slider
def visualize_labs(h5_path, lab_labels, signals= ['II', 'PLETH'], window=1):
    with h5py.File(h5_path, 'r') as f:
        ehr = pd.DataFrame(f['ehr'][:])
        timestamps = [t.decode('utf-8') for t in f['timestamps'][:]]
        waves = {sig: f['waveforms'][sig][:] for sig in signals if sig in f['waveforms']}
    
    present_labs = [lab for lab in lab_labels if lab in ehr.columns]
    n_chunks = len(timestamps)
    n_labs = len(present_labs)
    n_sigs = len(waves)

    # forward fill and compute staleness before normalizing
    chunk_times = pd.Series(pd.to_datetime(timestamps))
    stale = {}
    abnorm = {}
    
    
    fig = plt.figure(figsize=(16, n_labs * 0.6 + 6))
    gs = fig.add_gridspec(n_labs + 1 + n_sigs, 1,
                          height_ratios=[1] * n_labs + [2] + [2] * n_sigs,
                          hspace=1.0)
    plt.subplots_adjust(bottom=0.1, left=0.15)

    spark_axes = []
    for i, lab in enumerate(present_labs):
        ax = fig.add_subplot(gs[i])
        measured = ehr[lab].notna()  # use original col before any normalization
        chunk_ids = measured[measured].index
        last_draw = chunk_times.where(measured).ffill()
        stale[lab] = (chunk_times - last_draw).dt.total_seconds() / 60 / 60
        abnorm[lab] = ehr[f'{lab}_abnorm'].ffill()
        ehr[lab] = ehr[lab].ffill()
        vals = ehr[lab].values
        
        if i == 0:
            ax.set_title('Lab values', pad=5)
        colors = abnorm[lab].map({0: 'steelblue', 1: 'red'}).fillna('steelblue')
        ax.scatter(ehr.index, vals, s=6, color=colors.values, zorder=3)
        ax.set_ylabel(lab, rotation=0, ha='right', fontsize=7, labelpad=4)
        ax.set_xlim(0, n_chunks)
        
        # set yticks to min/max of measured values
        if len(vals) > 0 and not np.isnan(vals).all():
            vmin, vmax = np.nanmin(vals), np.nanmax(vals)
            ax.set_ylim(vmin * 0.9, vmax * 1.1)
            ax.set_yticks([vmin, vmax])
            ax.yaxis.set_major_formatter(plt.FormatStrFormatter('%.1f'))
            ax.yaxis.set_tick_params(labelsize=5)
        
        ax.set_xticklabels([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        spark_axes.append(ax)

    # staleness and abnorm heatmaps unchanged
    stale_ax = fig.add_subplot(gs[n_labs])
    
    divider = make_axes_locatable(stale_ax)
    cax = divider.append_axes("top", size="5%", pad=0.05)
    
    

    # staleness heatmap
    stale_matrix = np.array([stale[lab].values for lab in present_labs])
    im2 = stale_ax.imshow(
        stale_matrix,
        aspect='auto',
        cmap='YlOrRd',
        interpolation='none'
    )
    stale_ax.set_yticks(range(n_labs))
    stale_ax.set_yticklabels(present_labs, fontsize=8)
    stale_ax.set_title('Staleness (hours)', pad=25)
    stale_ax.set_xlabel('Chunk', fontsize=8)
    # chunk number xticks
    tick_step = max(1, n_chunks // 20)
    stale_ax.set_xticks(range(0, n_chunks, tick_step))
    stale_ax.set_xticklabels(range(0, n_chunks, tick_step), fontsize=6)
    plt.colorbar(im2, cax=cax, orientation='horizontal')
    cax.xaxis.set_ticks_position("top")

    # signal axes
    sig_axes = []
    chunk_size = 125 * 60
    time_axis = np.arange(chunk_size) / 125  # seconds
    for j, sig in enumerate(waves):
        ax = fig.add_subplot(gs[n_labs + 1 + j])
        line, = ax.plot(time_axis, waves[sig][0], linewidth=0.5, color='steelblue')
        ax.set_ylabel(sig, rotation=0, ha='right', fontsize=7, labelpad=4)
        if j == n_sigs - 1:
            ax.set_xlabel('Time (s)', fontsize=7)
            ax.set_xlim(0, 60)
            ax.set_xticks(range(0, 61, 10))
            ax.set_xticklabels(range(0, 61, 10), fontsize=6)
        else:
            ax.set_xticks([])
            ax.set_xticklabels([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        sig_axes.append((ax, line, sig))


    slider_ax = plt.axes([0.15, 0.02, 0.75, 0.02])
    slider = Slider(slider_ax, 'Chunk', 0, n_chunks - 1, valinit=0, valstep=1)
    all_axes = spark_axes + [stale_ax]
    vlines = [ax.axvline(0, color='blue', linewidth=1.0, alpha=0.7) for ax in all_axes]

    def update(val):
        chunk_id = int(slider.val)
        for vl in vlines:
            vl.set_xdata([chunk_id, chunk_id])
        for ax, line, sig in sig_axes:
            line.set_ydata(waves[sig][chunk_id])
            ax.relim()
            ax.autoscale_view()
            ax.set_xlim(0, 60)
        fig.suptitle(f'Chunk {chunk_id} — {timestamps[chunk_id]}', y=1.01)
        fig.canvas.draw_idle()

    def on_key(event):
        if event.key == 'right':
            slider.set_val(min(n_chunks - 1, int(slider.val) + window))
        elif event.key == 'left':
            slider.set_val(max(0, int(slider.val) - window))

    slider.on_changed(update)
    fig.canvas.mpl_connect('key_press_event', on_key)
    update(0)
    plt.show()

def visualize_meds(h5_path, med_labels, signals=['II', 'PLETH', 'ABP'], window=1):
    with h5py.File(h5_path, 'r') as f:
        ehr = pd.DataFrame(f['ehr'][:])
        timestamps = [t.decode('utf-8') for t in f['timestamps'][:]]
        waves = {sig: f['waveforms'][sig][:] for sig in signals if sig in f['waveforms']}

    n_chunks = len(timestamps)
    present_meds = [med for med in med_labels if f'{med}_ratenorm' in ehr.columns]
    n_meds = len(present_meds)
    n_sigs = len(waves)


    n_rows = n_meds + n_sigs
    fig, axes = plt.subplots(n_rows, 1, figsize=(16, n_rows * 1.5 + 1), sharex=False)
    plt.subplots_adjust(bottom=0.1, hspace=0.4, left=0.15, right=0.95)
    if n_rows == 1:
        axes = [axes]

    # med rate plots
    med_axes = []
    for i, med in enumerate(present_meds):
        ax = axes[i]
        vals = ehr[f'{med}_ratenorm'].values.copy().astype(float)
        changes = np.where(np.diff(vals) != 0)[0]
        vals[changes] = np.nan
        ax.plot(vals, linewidth=0.8, color='darkorange', drawstyle='steps-post')
        if i == 0:
            ax.set_title('Normalized titration rates', pad=5)
        ax.set_ylabel(med, rotation=0, ha='right', fontsize=7, labelpad=4)
        ax.set_xlim(0, n_chunks)
        ax.set_xticklabels([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        if i == n_meds - 1:
            # no signals below, add chunk ticks here
            tick_step = max(1, n_chunks // 20)
            ax.set_xticks(range(0, n_chunks, tick_step))
            ax.set_xticklabels(range(0, n_chunks, tick_step), fontsize=6)
            ax.set_xlabel('Chunk', fontsize=8)
        else:
            ax.set_xticklabels([])
        med_axes.append(ax)

    # waveform plots - show current chunk only
    sig_axes = []
    chunk_size = 125 * 60
    time_axis = np.arange(chunk_size) / 125  # seconds
    for j, sig in enumerate(waves):
        ax = fig.add_subplot(axes[n_meds + j])
        line, = ax.plot(time_axis, waves[sig][0], linewidth=0.5, color='steelblue')
        ax.set_ylabel(sig, rotation=0, ha='right', fontsize=7, labelpad=4)
        if j == n_sigs - 1:
            ax.set_xlabel('Time (s)', fontsize=7)
            ax.set_xlim(0, 60)
            ax.set_xticks(range(0, 61, 10))
            ax.set_xticklabels(range(0, 61, 10), fontsize=6)
        else:
            ax.set_xticks([])
            ax.set_xticklabels([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        sig_axes.append((ax, line, sig))

    # slider
    slider_ax = plt.axes([0.15, 0.03, 0.8, 0.02])
    slider = Slider(slider_ax, 'Chunk', 0, n_chunks - 1, valinit=0, valstep=1)
    vlines = [ax.axvline(0, color='blue', linewidth=1, alpha=0.5) for ax in med_axes]

    def update(val):
        chunk_id = int(slider.val)

        # update vlines on med plots
        for vl in vlines:
            vl.set_xdata([chunk_id, chunk_id])

        # update waveform for current chunk
        for ax, line, sig in sig_axes:
            line.set_ydata(waves[sig][chunk_id])
            ax.relim()
            ax.autoscale_view()
            ax.set_xlim(0, 60)

        fig.suptitle(f'Chunk {chunk_id} — {timestamps[chunk_id]}', y=1.01)
        fig.canvas.draw_idle()

    def on_key(event):
        if event.key == 'right':
            slider.set_val(min(n_chunks - 1, int(slider.val) + window))
        elif event.key == 'left':
            slider.set_val(max(0, int(slider.val) - window))

    slider.on_changed(update)
    fig.canvas.mpl_connect('key_press_event', on_key)
    update(0)
    plt.show()