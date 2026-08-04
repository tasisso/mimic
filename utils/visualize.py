from mpl_toolkits.axes_grid1 import make_axes_locatable
import h5py
import numpy as np
import pandas as pd 
import matplotlib.pyplot as plt
from matplotlib.widgets import Slider
from matplotlib.lines import Line2D
from matplotlib.colors import ListedColormap


def visualize_labs(h5_path, lab_labels, signals=['II', 'PLETH'], window=1):
    with h5py.File(h5_path, 'r') as f:
        timestamps = [t.decode('utf-8') for t in f['timestamps'][:]]
        waves = {sig: f['waveforms'][sig][:] for sig in signals if sig in f['waveforms']}

        # ALL waveform types in the file, for the availability overview panel
        all_wave_types = list(f['waveforms'].keys())
        wave_avail = {}
        for sig in all_wave_types:
            arr = f['waveforms'][sig][:]  # (n_chunks, chunk_size)
            wave_avail[sig] = np.isnan(arr).mean(axis=1)  # fraction NaN per chunk

        lab_data = {}
        if 'labs' in f:
            for key in f['labs'].keys():
                lab_data[key] = pd.Series(f['labs'][key][:])

    present_labs = [lab for lab in lab_labels if f'{lab}_value' in lab_data]
    n_chunks = len(timestamps)
    n_labs = len(present_labs)
    n_sigs = len(waves)
    n_wave_types = len(all_wave_types)

    tick_step = max(1, n_chunks // 20)

    unit_height = 0.6
    spacer_ratio = 0.3

    heatmap_ratio = max(2, n_labs * 0.5)
    wave_avail_ratio = max(1.5, n_wave_types * 0.4) 

    ratios = (
        [1] * n_labs + [spacer_ratio]
        + [heatmap_ratio] + [spacer_ratio]
        + [wave_avail_ratio] + [spacer_ratio]
        + [2] * n_sigs
    )
    total_ratio_units = sum(ratios)

    top_margin_in = 0.5
    bottom_gap_in = 0.35
    slider_height_in = 0.2
    bottom_pad_in = 0.15
    fixed_margin_in = 1.2

    fig_height = total_ratio_units * unit_height + fixed_margin_in

    bottom_margin_in = bottom_gap_in + slider_height_in + bottom_pad_in
    gs_top = 1 - (top_margin_in + 0.1) / fig_height
    gs_bottom = bottom_margin_in / fig_height
    slider_bottom_frac = bottom_pad_in / fig_height
    slider_height_frac = slider_height_in / fig_height

    fig = plt.figure(figsize=(12, fig_height))
    gs = fig.add_gridspec(len(ratios), 1,
                        height_ratios=ratios,
                        hspace=1.0,
                        top=gs_top,
                        bottom=gs_bottom)
    plt.subplots_adjust(bottom=0.1, left=0.15)

    #row indices
    row_heatmap = n_labs + 1
    row_wave_avail = n_labs + 3
    row_sig_start = n_labs + 5

    spark_axes = []
    for i, lab in enumerate(present_labs):
        ax = fig.add_subplot(gs[i])
        vals = lab_data[f'{lab}_value']
        abnorm = lab_data.get(f'{lab}_abnormal')

        if i == 0:
            ax.set_title('Lab values', pad=5)
        colors = abnorm.map({0: 'steelblue', 1: 'red'}).fillna('steelblue')
        ax.scatter(range(n_chunks), vals, s=3, color=colors.values, zorder=3, alpha=0.6, clip_on=False)
        ax.set_ylabel(lab, rotation=0, ha='right', fontsize=7, labelpad=4)
        ax.set_xlim(0, n_chunks)

        if len(vals) > 0 and not np.isnan(vals).all():
            vmin, vmax = np.nanmin(vals), np.nanmax(vals)

            span = vmax - vmin
            pad = span * 0.1 if span > 0 else (abs(vmax) * 0.1 or 1)
            ax.set_ylim(vmin - pad, vmax + pad)

            decimals = 1
            if vmin != vmax:
                while round(vmin, decimals) == round(vmax, decimals) and decimals < 6:
                    decimals += 1
            ax.set_yticks([vmin, vmax])
            ax.yaxis.set_major_formatter(plt.FormatStrFormatter(f'%.{decimals}f'))
            # ax.set_ylim(vmin * 0.9, vmax * 1.1)
            # ax.set_yticks([vmin, vmax])
            # ax.yaxis.set_major_formatter(plt.FormatStrFormatter(f'%.{decimals}f'))
            ax.yaxis.set_tick_params(labelsize=5)
            

        if i == n_labs - 1:
            ax.set_xticks(range(0, n_chunks, tick_step))
            ax.set_xticklabels(range(0, n_chunks, tick_step), fontsize=6)
            ax.set_xlabel('Chunk', fontsize=8)
        else:
            ax.set_xticks([])
            ax.set_xticklabels([])
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        spark_axes.append(ax)

    legend_elements = [
        Line2D([0], [0], marker='o', color='w', markerfacecolor='steelblue', markersize=5, label='Normal/No Flag'),
        Line2D([0], [0], marker='o', color='w', markerfacecolor='red', markersize=5, label='Abnormal'),
    ]
    spark_axes[0].legend(handles=legend_elements, bbox_to_anchor=(1.0, 2.0), loc='upper right', fontsize=6, framealpha=0.5)

    # Staleness heatmap
    stale_ax = fig.add_subplot(gs[row_heatmap])
    divider = make_axes_locatable(stale_ax)
    cax = divider.append_axes("top", size=0.1, pad=0.05)

    stale_matrix = np.array([lab_data.get(f'{lab}_last_drawn_hrs') for lab in present_labs])
    im2 = stale_ax.imshow(stale_matrix, aspect='auto', cmap='YlOrRd', interpolation='none')
    stale_ax.set_yticks(range(n_labs))
    stale_ax.set_yticklabels(present_labs, fontsize=8)
    stale_ax.set_title('Staleness (hrs)', pad=35)
    stale_ax.set_xlim(0, n_chunks)
    stale_ax.set_xticks(range(0, n_chunks, tick_step))
    stale_ax.set_xticklabels(range(0, n_chunks, tick_step), fontsize=6)
    stale_ax.set_xlabel('Chunk', fontsize=8)
    plt.colorbar(im2, cax=cax, orientation='horizontal')
    cax.xaxis.set_ticks_position("top")

    # NEW: Waveform availability heatmap (all signal types, NaN fraction per chunk)
    wave_avail_ax = fig.add_subplot(gs[row_wave_avail])
    avail_matrix = np.array([wave_avail[sig] for sig in all_wave_types])  # (n_wave_types, n_chunks)
    im3 = wave_avail_ax.imshow(
        avail_matrix,
        aspect='auto',
        cmap='Greys',      # 0 = white (fully present), 1 = black (fully NaN)
        vmin=0, vmax=1,
        interpolation='none'
    )
    wave_avail_ax.set_yticks(range(n_wave_types))
    wave_avail_ax.set_yticklabels(all_wave_types, fontsize=8)
    wave_avail_ax.set_title('Waveform Availability (NaN fraction)', pad=15)
    wave_avail_ax.set_xlabel('Chunk', fontsize=8)
    wave_avail_ax.set_xlim(0, n_chunks)
    wave_avail_ax.set_xticks(range(0, n_chunks, tick_step))
    wave_avail_ax.set_xticklabels(range(0, n_chunks, tick_step), fontsize=6)

    # signal axes (60s intra-chunk view — different x-axis, unrelated to chunk index above)
    sig_axes = []
    chunk_size = 125 * 60
    time_axis = np.arange(chunk_size) / 125
    for j, sig in enumerate(waves):
        ax = fig.add_subplot(gs[row_sig_start + j])
        if j == 0:
            ax.set_title('Waveform Signal', pad=0)
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

    slider_ax = plt.axes([0.15, slider_bottom_frac, 0.75, slider_height_frac])
    slider = Slider(slider_ax, 'Chunk', 0, n_chunks - 1, valinit=0, valstep=1)
    all_axes = spark_axes + [stale_ax, wave_avail_ax]  # vline now spans this panel too
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
        fig.suptitle(f'Chunk {chunk_id} — {timestamps[chunk_id]}', y=1.0)
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

def visualize_meds(h5_path, med_labels, signals=['ABP'], window=1, fs=125):
    with h5py.File(h5_path, 'r') as f:
        timestamps = [t.decode('utf-8') for t in f['timestamps'][:]]
        waves = {sig: f['waveforms'][sig][:] for sig in signals if sig in f['waveforms']}

        med_data = {}
        if 'inputs' in f:
            for key in f['inputs'].keys():
                med_data[key] = f['inputs'][key][:]

        # ALL waveform types in the file, for the availability overview panel
        all_wave_types = list(f['waveforms'].keys())
        wave_avail = {}
        for sig in all_wave_types:
            arr = f['waveforms'][sig][:]  # (n_chunks, chunk_size)
            wave_avail[sig] = np.isnan(arr).mean(axis=1)  # fraction NaN per chunk

    n_chunks = len(timestamps)
    n_sigs = len(waves)
    n_wave_types = len(all_wave_types)

    # rate-having meds (line plots), including norepi_eq as a rate-like series
    rate_labels = [med for med in med_labels if f'{med}_ratenorm' in med_data]
    if 'norepi_eq' in med_data and 'norepi_eq' not in rate_labels:
        rate_labels = rate_labels + ['norepi_eq']
    n_rate = len(rate_labels)

    # all _on flags (meds and/or categories) go into one consolidated heatmap
    on_labels = [med for med in med_data.keys() if med.endswith('_on')]
    n_on = len(on_labels)

    # --- layout ---
    unit_height = 0.6
    spacer_ratio = 0.3

    on_heatmap_ratio = max(2, n_on * 0.4)
    wave_avail_ratio = max(1.5, n_wave_types * 0.4)

    ratios = (
        [1.2] * n_rate + [spacer_ratio]
        + [on_heatmap_ratio] + [spacer_ratio]
        + [wave_avail_ratio] + [spacer_ratio]
        + [2] * n_sigs
    )
    total_ratio_units = sum(ratios)

    top_margin_in = 0.5
    bottom_gap_in = 0.35
    slider_height_in = 0.2
    bottom_pad_in = 0.15
    fixed_margin_in = 1.2

    fig_height = total_ratio_units * unit_height + fixed_margin_in
    bottom_margin_in = bottom_gap_in + slider_height_in + bottom_pad_in
    gs_top = 1 - (top_margin_in + 0.1) / fig_height
    gs_bottom = bottom_margin_in / fig_height
    slider_bottom_frac = bottom_pad_in / fig_height
    slider_height_frac = slider_height_in / fig_height

    fig = plt.figure(figsize=(11, fig_height))
    gs = fig.add_gridspec(len(ratios), 1,
                          height_ratios=ratios,
                          hspace=0.6,
                          top=gs_top,
                          bottom=gs_bottom, left=0.15, right=0.95)

    tick_step = max(1, n_chunks // 20)

    # row indices, computed once
    row_on_heatmap = n_rate + 1
    row_wave_avail = n_rate + 3
    row_sig_start = n_rate + 5

    # --- rate-med line plots (incl. norepi_eq) ---
    rate_axes = []
    for i, med in enumerate(rate_labels):
        ax = fig.add_subplot(gs[i])
        if med == 'norepi_eq':
            vals = med_data[f'{med}'].copy().astype(float)
        else:
            vals = med_data[f'{med}_ratenorm'].copy().astype(float)

        vmin, vmax = np.nanmin(vals), np.nanmax(vals)

        span = vmax - vmin
        pad = span * 0.1 if span > 0 else (abs(vmax) * 0.1 or 1)
        ax.set_ylim(vmin - pad, vmax + pad)

        decimals = 2
        if vmin != vmax:
            while round(vmin, decimals) == round(vmax, decimals) and decimals < 6:
                decimals += 1

        changes = np.where(np.diff(vals) != 0)[0]
        vals[changes] = np.nan
        ax.plot(vals, linewidth=0.8, color='darkorange', drawstyle='steps-post')
        ax.set_ylim(bottom=0)
        ax.set_ylabel(med, rotation=0, ha='right', fontsize=7, labelpad=4)
        ax.set_yticks([vmin, vmax])
        ax.yaxis.set_major_formatter(plt.FormatStrFormatter(f'%.{decimals}f'))
        ax.yaxis.set_tick_params(labelsize=5)
        ax.set_xlim(0, n_chunks)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        if i == 0:
            ax.set_title('Titration Rates', pad=5)
        if i == n_rate - 1:
            ax.set_xticks(range(0, n_chunks, tick_step))
            ax.set_xticklabels(range(0, n_chunks, tick_step), fontsize=6)
            ax.set_xlabel('Chunk', fontsize=8)
        else:
            ax.set_xticks([])
            ax.set_xticklabels([])
        rate_axes.append(ax)

    # --- consolidated _on flags heatmap ---
    on_ax = fig.add_subplot(gs[row_on_heatmap])
    on_matrix = np.array([med_data[f'{med}'] for med in on_labels]).astype(float)
    im_on = on_ax.imshow(
        on_matrix,
        aspect='auto',
        cmap=ListedColormap(['white', 'steelblue']),
        vmin=0, vmax=1,
        interpolation='none'
    )
    on_ax.set_yticks(range(n_on))
    on_ax.set_yticklabels(on_labels, fontsize=7)
    on_ax.set_title('Medication / Category Flags', pad=10)
    on_ax.set_xlim(0, n_chunks)
    on_ax.set_xticks(range(0, n_chunks, tick_step))
    on_ax.set_xticklabels(range(0, n_chunks, tick_step), fontsize=6)
    on_ax.set_xlabel('Chunk', fontsize=8)

    # --- waveform availability heatmap ---
    wave_avail_ax = fig.add_subplot(gs[row_wave_avail])
    avail_matrix = np.array([wave_avail[sig] for sig in all_wave_types])
    im_wave = wave_avail_ax.imshow(
        avail_matrix,
        aspect='auto',
        cmap='Greys',
        vmin=0, vmax=1,
        interpolation='none'
    )
    wave_avail_ax.set_yticks(range(n_wave_types))
    wave_avail_ax.set_yticklabels(all_wave_types, fontsize=8)
    wave_avail_ax.set_title('Waveform Availability (NaN fraction)', pad=10)
    wave_avail_ax.set_xlim(0, n_chunks)
    wave_avail_ax.set_xticks(range(0, n_chunks, tick_step))
    wave_avail_ax.set_xticklabels(range(0, n_chunks, tick_step), fontsize=6)
    wave_avail_ax.set_xlabel('Chunk', fontsize=8)

    # --- signal axes (60s intra-chunk view) ---
    chunk_size = fs * 60
    time_axis = np.arange(chunk_size) / fs
    sig_axes = []
    for j, sig in enumerate(waves):
        ax = fig.add_subplot(gs[row_sig_start + j])
        line, = ax.plot(time_axis, waves[sig][0], linewidth=0.5, color='steelblue')
        ax.set_ylabel(sig, rotation=0, ha='right', fontsize=7, labelpad=4)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        if j == n_sigs - 1:
            ax.set_xlabel('Time (s)', fontsize=7)
            ax.set_xlim(0, 60)
            ax.set_xticks(range(0, 61, 10))
            ax.set_xticklabels(range(0, 61, 10), fontsize=6)
        else:
            ax.set_xticks([])
            ax.spines['bottom'].set_visible(False)
        sig_axes.append((ax, line, sig))
    
    ref_pos = rate_axes[0].get_position() if rate_axes else on_ax.get_position()
    slider_left = ref_pos.x0
    slider_width = ref_pos.width

    #slider_ax = plt.axes([0.15, slider_bottom_frac, 0.75, slider_height_frac])
    slider_ax = plt.axes([slider_left, slider_bottom_frac, slider_width, slider_height_frac])

    slider = Slider(slider_ax, 'Chunk', 0, n_chunks - 1, valinit=0, valstep=1)

    all_axes = rate_axes + [on_ax, wave_avail_ax]
    vlines = [ax.axvline(0, color='blue', linewidth=1, alpha=0.5) for ax in all_axes]

    def update(val):
        chunk_id = int(slider.val)
        for vl in vlines:
            vl.set_xdata([chunk_id, chunk_id])
        for ax, line, sig in sig_axes:
            line.set_ydata(waves[sig][chunk_id])
            ax.relim()
            ax.autoscale_view()
            ax.set_xlim(0, 60)
        fig.suptitle(f'Chunk {chunk_id} — {timestamps[chunk_id]}', y=1.0)
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