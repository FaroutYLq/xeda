import cutax
import straxen
import strax
import os

straxen.print_versions()

dtypes = ["peak_basics", "raw_records_aqmon","distinct_channels", "veto_proximity", "afterpulses", "veto_intervals",
          "peaklet_classification", "peaklet_classification_he", "veto_regions", "event_pattern_fit", "event_w_bayes_class", 
          "event_basics", "events", "event_info",  "peak_proximity", "peak_positions", "peak_positions_mlp", 
          "peak_positions_cnn", "peak_positions_gcn", "events_mv", "events_nv", "event_positions_nv"] 
basedir = '/project2/lgrandi/xenonnt/processed/' 

# Context to copy to frontend
st = cutax.contexts.xenonnt_offline(output_folder='/project2/lgrandi/xenonnt/processed')
# Context to test loadability
st_midway = cutax.contexts.xenonnt_offline(_rucio_path='/project/lgrandi/rucio')
st_midway.storage.append(strax.DataDirectory("/project2/lgrandi/xenonnt/processed/", readonly=True))

# Runs available on DaLI
runs = st.select_runs(available='event_basics')
for run in strax.utils.tqdm(runs.name):
	for dtype in dtypes:
		if st_midway.is_stored(run, dtype):
			try:
				loaded = st_midway.get_array(run, dtype)
			except:
				name = st_midway.key_for(run, dtype) 
				print(name,'is corrupted! Removing it...')
				os.system('rm -rf '+basedir+name)
				print('Removed', name)
				print('Now we want to redo copy_to_frontend')
				st.copy_to_frontend(run, dtype)
				print('Copied', name)
