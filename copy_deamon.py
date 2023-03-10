import cutax
import straxen
import strax

st = cutax.contexts.xenonnt_online(output_folder='/project2/lgrandi/xenonnt/processed')

dtypes = ["peak_basics", "peak_positions_mlp", "lone_hits", "pulse_counts", "merged_s2s", 
          "peaklet_classification", "peaklet_classification_he", "veto_regions", "pulse_counts_he", 
          "peaklets_he","led_calibration","hitlets_mv","hitlets_nv", "event_basics", "events", 
          "event_info",  "peak_proximity", "peak_positions", "peak_positions_cnn", "peak_positions_gcn", 
          "events_mv", "events_nv", "afterpulses"]

while True:
    # Get online
    st = cutax.contexts.xenonnt_online(output_folder='/project2/lgrandi/xenonnt/processed')
    runs = st.select_runs(available='event_basics').name[:5]
    for run in strax.utils.tqdm(runs):
        for dtype in dtypes:
            try:
                st.copy_to_frontend(run, dtype)
            except:
                print(dtype+' for run '+run+' is not available or already exists in destination.')

    # sleep for half an hour.        
    sleep(1800)

