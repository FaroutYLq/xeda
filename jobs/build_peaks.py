import cutax
import sys
from tqdm import tqdm
import numpy as np
import os
import matplotlib.pyplot as plt
import gc
from datetime import datetime
# Get current date
current_date = datetime.now()
# Format the date to the desired format
formatted_date = current_date.strftime('%Y%m%d')

OUTPUT_DIR = '/project2/lgrandi/yuanlq/shared/midway_corrupted/%s/results/'%(formatted_date)
os.makedirs(OUTPUT_DIR, exist_ok=True)

_, directory, index = sys.argv
print("The directory to process:", directory)

st = cutax.xenonnt_offline()

# Just a concept. It is too slow to be run here.
runs = np.load(directory, allow_pickle=True)

peaks_not_loadbale = []
not_equal_length = []

for run in tqdm(runs):
    gc.collect()
    run = str(run).zfill(6)
    if (st.is_stored(run, 'peaklets') and 
        st.is_stored(run, 'lone_hits') and 
        st.is_stored(run, 'merged_s2s') and
        st.is_stored(run, 'peaklet_classification')):
        try:
            peaks = st.get_array(run, 'peaks', keep_columns=('time'))
        except:
            peaks_not_loadbale.append(run)
            peaklets = st.get_array(run, 'peaklets', keep_columns=('time'))
            peaklet_classification = st.get_array(run, 'peaklet_classification', keep_columns=('time'))
            if len(peaklet_classification) != len(peaklets):
                not_equal_length.append(run)
            continue

np.save(OUTPUT_DIR+"peaks_not_loadable_%s.npy"%(index), np.array(peaks_not_loadbale))
np.save(OUTPUT_DIR+"not_equal_length_%s.npy"%(index), np.array(not_equal_length))
