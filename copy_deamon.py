import cutax
import straxen
import strax
import time

st = cutax.contexts.xenonnt_online(output_folder='/project2/lgrandi/xenonnt/processed')

dtypes = ["peak_basics", "raw_records_aqmon","distinct_channels", 
          "peaklet_classification", "peaklet_classification_he", "veto_regions", "event_pattern_fit", "event_w_bayes_class", 
          "event_basics", "events", "event_info",  "peak_proximity", "peak_positions", "peak_positions_mlp", 
          "peak_positions_cnn", "peak_positions_gcn", "events_mv", "events_nv", "event_positions_nv"] 


while True:
    # Get online
    st = cutax.contexts.xenonnt_online(output_folder='/project2/lgrandi/xenonnt/processed')
    runs = st.select_runs(available='event_basics').name[:1000]
    for run in strax.utils.tqdm(runs):
        for dtype in dtypes:
            try:
                st.copy_to_frontend(run, dtype)
            except:
                print(dtype+' for run '+run+' is not available or already exists in destination.')
    # Get offline radon
    st = cutax.contexts.xenonnt_offline(output_folder='/project2/lgrandi/xenonnt/processed')
    runs = st.select_runs(run_mode=('tpc_radon', 'tpc_radon_hev','tpc_kr83m'), available='event_basics')
    for run in strax.utils.tqdm(runs.name):
        for dtype in dtypes:
            try:
                st.copy_to_frontend(run, dtype)
            except:
                print(dtype+' for run '+run+' is not available or already exists in destination.')
                
    # sleep for half an hour.        
    time.sleep(1800)
