import cutax
import straxen
import strax
import time

st = cutax.contexts.xenonnt_online(output_folder='/project2/lgrandi/xenonnt/processed')

dtypes = ["peak_basics", "raw_records_aqmon","distinct_channels", "veto_interval", "veto_proximity", "afterpulses",
          "peaklet_classification", "peaklet_classification_he", "veto_regions", "event_pattern_fit", "event_w_bayes_class", 
          "event_basics", "events", "event_info",  "peak_proximity", "peak_positions", "peak_positions_mlp", 
          "peak_positions_cnn", "peak_positions_gcn", "events_mv", "events_nv", 'events_nv_info', 'events_mv_info', "event_positions_nv"] 


while True:
    # Get online
    #st = cutax.contexts.xenonnt_online(output_folder='/project2/lgrandi/xenonnt/processed')
    #runs = st.select_runs(available='event_basics').name[:500]
    #for run in strax.utils.tqdm(runs):
    #    for dtype in dtypes:
    #        try:
    #            st.copy_to_frontend(run, dtype)
    #        except:
    #            print(dtype+' for run '+run+' is not available or already exists in destination.')
    # Get offline
    st = cutax.contexts.xenonnt_offline(output_folder='/project2/lgrandi/xenonnt/processed')
    runs = st.select_runs(available='event_basics')
    for run in strax.utils.tqdm(runs.name):
        for dtype in dtypes:
            try:
                st.copy_to_frontend(run, dtype)
            except:
                print(dtype+' for run '+run+' is not available or already exists in destination.')
                
    # sleep for half an hour.        
    time.sleep(1800)
