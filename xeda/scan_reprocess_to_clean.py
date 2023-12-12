import straxen
from utilix.io import load_runlist
import cutax
import numpy as np
from tqdm import tqdm
import pandas as pd
import straxen
import pickle
import os
from datetime import datetime
import admix
import utilix

st = cutax.xenonnt_offline()

#with open('/project2/lgrandi/xenonnt/reprocessing_runlist/global_v11/runlists_reprocessing_global_v11.pickle', 'rb') as f:
#    jingqiang = pickle.load(f)
with open('/project2/lgrandi/xenonnt/reprocessing_runlist/global_v12/runlists_reprocessing_global_v12.pickle', 'rb') as f:
    jingqiang = pickle.load(f)
with open('/project2/lgrandi/xenonnt/reprocessing_runlist/global_v11/runlists_reprocessing_global_v11_extended.pickle', 'rb') as f:
    jingqiang_extended = pickle.load(f)
with open('/project2/lgrandi/xenonnt/reprocessing_runlist/global_v13/runlists_reprocessing_global_v13.pickle', 'rb') as f:
    jingqiang13 = pickle.load(f)
straxen.print_versions()

rundb = utilix.rundb.xent_collection()

sr0 = dict()
sr0['runlists']={
    "sr0_bkg": jingqiang['runlists']['sr0_bkg'],
    "sr0_ambe": jingqiang['runlists']['sr0_ambe'],
    "sr0_ted_bkg": jingqiang['runlists']['sr0_ted_bkg'],
    "sr0_rn220": jingqiang['runlists']['sr0_rn220'],
    "sr0_ar37": jingqiang['runlists']['sr0_ar37'],
    "sr0_kr83m": jingqiang['runlists']['sr0_kr83m'],
}
all_sr1 = np.concatenate((jingqiang13['runlists']['sr1_rn220'],jingqiang13['runlists']['sr1_kr83m'],jingqiang13['runlists']['sr1_bkg'],jingqiang13['runlists']['sr1_ybe'],jingqiang13['runlists']['sr1_y88'],jingqiang13['runlists']['sr1_ambe'],jingqiang13['runlists']['sr1_rn222'],jingqiang13['runlists']['th232']))
all_sr0 = np.concatenate((jingqiang['runlists']['sr0_bkg'],
                          jingqiang['runlists']['sr0_ambe'],
                          jingqiang['runlists']['sr0_ted_bkg'],
                          jingqiang['runlists']['sr0_rn220'],
                          jingqiang['runlists']['sr0_ar37'],
                          jingqiang['runlists']['sr0_kr83m']))

osg_all_dtypes = ['pulse_counts', 'veto_regions', 'lone_hits', 'peaklets', 'merged_s2s', 'peaklet_classification', 'peak_basics',
                  'distinct_channels', 'event_basics', 'corrected_areas', 'energy_estimates', 'event_info',
                  'event_pattern_fit', 'event_positions', 'peak_positions_gcn', 'peak_positions_cnn',
                  'peak_proximity', 'peak_positions_mlp', 'event_n_channel', 'peak_s1_positions_cnn',
                  'peak_corrections', 'event_area_per_channel', 'event_ms_naive', 'event_top_bottom_params']
osg_event_dtypes = ['merged_s2s', 'peaklet_classification', 'peak_basics',
                  'distinct_channels', 'event_basics', 'corrected_areas', 'energy_estimates', 'event_info',
                  'event_pattern_fit', 'event_positions', 'peak_positions_gcn', 'peak_positions_cnn',
                  'peak_proximity', 'peak_positions_mlp', 'event_n_channel', 'peak_s1_positions_cnn',
                  'peak_corrections','event_area_per_channel', 'event_ms_naive', 'event_top_bottom_params']
reprox_dtypes = ['event_ms_naive', 'cuts_basic', 'event_top_bottom_params', 'event_area_per_channel']
nv_runlists = ['sr1_bkg', 'th232', 'sr1_ybe', 'sr1_ambe']

def make_did(st, runid, dtype):
    if type(runid) != str:
        runid = str(runid).zfill(6)
    key_for = st.key_for(runid, dtype)
    linhash = str(key_for).split('-')[-1]
    did = 'xnt_'+runid+':'+dtype+'-'+linhash
    return did

def check_progress(st, runlist, dtypes=osg_all_dtypes+reprox_dtypes):
    assert 'peak_basics' in dtypes, "Please include peak_basics in dtypes"
    assert 'event_basics' in dtypes, "Please include event_basics in dtypes"

    field_list = [('runid', int)] + [(dtype, bool) for dtype in dtypes]
    check_dtypes = np.dtype(field_list)

    runs = load_runlist(runlist)
    length = len(runs)
    result = np.zeros(length, dtype=check_dtypes)
    for i in tqdm(range(length)):
        runid = runs[i]
        result['runid'][i] = runid
        for dtype in dtypes:
            result[i][dtype] = st.is_stored(str(runid).zfill(6), dtype)

    print('There are %s runs in this runlist %s:'%(len(runs), runlist))
    for dtype in dtypes:
        percent = int(np.sum(result[dtype])/length*100)
        print('    Available %s: %s percent'%(dtype, percent))

    return pd.DataFrame(result)

def reprox_to_csv(st, to_process_dtypes=('event_ms_naive', 'cuts_basic', 'event_top_bottom_params'),
                  runlist_path='/project/lgrandi/xenonnt/data_management_reprocessing/to_do_runs.csv'):
    required_dtypes = ['peaklets', 'merged_s2s', 'peak_basics',
                      'distinct_channels', 'event_basics',
                      'event_pattern_fit']
    desired_dtypes = required_dtypes
    for dtype in to_process_dtypes:
        desired_dtypes.append(dtype)

    # Run list with reprox dependency available
    available = st.select_runs(available=required_dtypes)
    # Run list with all desired dataypes available already
    finished = st.select_runs(available=desired_dtypes)
    # Merge the two DataFrames on all columns
    merged = pd.merge(available, finished, how='outer', indicator=True)
    # Select only the rows that are in A but not in B
    result = merged.loc[merged['_merge'] == 'left_only', available.columns]

    result.to_csv(runlist_path)

def check_progress_wiki(st, jingqiang, dtypes=['peak_basics', 'event_pattern_fit',
                                          'event_basics', 'event_info', 'distinct_channels',
                                          'cuts_basic', 'event_ms_naive', 'event_area_per_channel'],
                       nv=False):
    #assert 'peak_basics' in dtypes, "Please include peak_basics in dtypes"
    #assert 'event_basics' in dtypes, "Please include event_basics in dtypes"
    field_list = [('runid', int)] + [(dtype, bool) for dtype in dtypes]
    check_dtypes = np.dtype(field_list)

    print('<table>')
    print('^Completion Percentile at Time %s^^^^^^^^^^'%(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    headline="^Data Type^# of Runs^"
    for dtype in dtypes:
        headline += "%s^"%(dtype)
    print(headline)
    for k in jingqiang['runlists'].keys():
        runs = jingqiang['runlists'][k]
        length = len(runs)
        liststr = "|"
        liststr += k
        liststr += '|'
        liststr += str(length)
        liststr += '|'
        result = np.zeros(length, dtype=check_dtypes)
        for i in range(length):
            runid = runs[i]
            result['runid'][i] = runid
            for dtype in dtypes:
                result[i][dtype] = st.is_stored(str(runid).zfill(6), dtype)

        for dt in dtypes:
            dt_percent = int(np.sum(result[dt])/length*100)
            liststr += str(dt_percent)
            liststr += '|'
        print(liststr)
    print('</table>')

import admix
def find_peaks_to_move(runlist=jingqiang13['runlists']['sr1_y88'],
                       save_at='/project2/lgrandi/yuanlq/shared/osg_to_midway/2023102715_sr1_y88_0.npy',
                       output_dir_base = '/project2/lgrandi/yuanlq/shared/midway_corrupted',
                       destination_rse = 'UC_DALI_USERDISK'):
    dids_to_move = []
    incomplete_on_osg = []
    size_gb = 0
    for run in tqdm(runlist):
        run = str(run).zfill(6)
        for dtype in ['peaklets', 'merged_s2s', 'lone_hits']:
            good_to_move = False
            
            did = make_did(st, run, dtype)
            rule_at_dali = None
            try:
                rule_at_osg = admix.rucio.get_rule(did, rse='UC_OSG_USERDISK')
            except:
                continue
            try:
                rule_at_dali = admix.rucio.get_rule(did, rse=destination_rse)
            except:
                continue
            if not (rule_at_dali is None):
                continue
                
            try:
                files = admix.rucio.list_files(did)
                n_files = len(files)
                last_chunk_n = int(admix.rucio.list_files(did)[-2].split('-')[-1])
                if last_chunk_n!=n_files-2:
                    incomplete_on_osg.append(did)
                else:
                    good_to_move = True
            except:
                incomplete_on_osg.append(did)
                continue
            if good_to_move:
                dids_to_move.append(did)
                size_gb += admix.rucio.get_size_mb(did)/1024

    array_dtype = [
        ('dataset_did','O'),
        ('rse', 'O'),
    ]
        
    dids_to_move = np.array(dids_to_move)
    print('Total size %s GB for %s files, with list stored at %s'%(size_gb, len(dids_to_move), save_at))
    if len(incomplete_on_osg):
        incomplete_on_osg = np.array(incomplete_on_osg)
        to_delete_rules = np.zeros(len(incomplete_on_osg), dtype=array_dtype)
        to_delete_rules['dataset_did'] = incomplete_on_osg
        to_delete_rules['rse'] = 'UC_OSG_USERDISK'
        
        print('The following datasets are incomplete and wont be transferred')
        print(incomplete_on_osg)
        today = datetime.now().strftime('%Y%m%d%H%M')
        output_dir = os.path.join(output_dir_base, today)
        
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        print('Your lists are at %s'%(output_dir))
        np.save(os.path.join(output_dir, 'delete_rules.npy'), np.array(to_delete_rules))
        np.save(os.path.join(output_dir, 'delete_rundb.npy'), np.array(incomplete_on_osg))
    
    np.save(save_at, dids_to_move)    
   
    return dids_to_move

from datetime import datetime
import os
import admix
import utilix


def analyze_unfinshed(st, runlist,
                      dtypes_to_check=osg_all_dtypes,
                      dtypes_should_be_finished = ['event_info', 'event_pattern_fit', 'event_basics', 'event_ambience', 'event_shadow'],
                      output_dir_base = '/project2/lgrandi/yuanlq/shared/midway_corrupted',
                      save=False,
                      force_deletion=False):
    print("Checking the following datatype existence")
    print(dtypes_should_be_finished)
    rundb = utilix.rundb.xent_collection()

    dids_rucio_rules_still_around = []
    dids_rucio_rules_at = []
    dids_files_still_around = []
    dids_in_db = []
    empty_rules = []
    missing_rules = []
    n_finished_runs = 0


    for run in tqdm(runlist):
        if type(run)!="str":
            run = str(run).zfill(6)
        if all(st.is_stored(run, dt) for dt in dtypes_should_be_finished):
            n_finished_runs += 1
            if not force_deletion:
                continue
        for dtype in dtypes_to_check:
            did = make_did(st, run, dtype)
            found_rules = False
            found_files = False
            try:
                rules = admix.rucio.list_rules(did)
                if not (rules is None):
                    if len(rules):
                        found_rules = True
            except:
                pass

            try:
                files = admix.rucio.list_files(did)
                if not (files is None):
                    if len(files):
                        found_files = True
            except:
                pass

            if found_rules:
                for rule in rules:
                    dids_rucio_rules_still_around.append(did)
                    location = rule['rse_expression']
                    dids_rucio_rules_at.append(location)

            if found_files:
                dids_files_still_around.append(did)

            if found_rules and not found_files:
                empty_rules.append(did)

            if found_files and not found_rules:
                missing_rules.append(did)

            query = {
                'number': int(run),
                'data': {
                    '$elemMatch': {
                        'did': did
                    }
                }
            }
            doc = rundb.find_one(query)
            if not (doc is None):
                dids_in_db.append(did)



    print('----------')
    print("After checking these datatypes as a marker for completion:")
    print(dtypes_should_be_finished)
    print("%s/%s runs are finished."%(n_finished_runs, len(runlist)))
    print("For the unfinished runs, we are analyzing these datatypes supposed to be delievered by OSG")
    print(dtypes_to_check)
    print('    %s rules are found in rucio, which we want to delete-rule'%(len(dids_rucio_rules_still_around)))
    print('    %s files are found in rucio, which we want to erase'%(len(dids_files_still_around)))
    print('    %s data have data entries in RunDB, which we want to delete the data entry from RunDB'%(len(dids_in_db)))
    print('    %s rules are without any file associated with it, which we want to delete-rule'%(len(empty_rules)))
    print('    %s data whose files are without a rule associated with it, which we want to add-rule and then delete-rule and erase'%(len(missing_rules)))

    array_dtype = [
        ('dataset_did','O'),
        ('rse', 'O'),
    ]
    to_delete_rules = np.zeros(len(dids_rucio_rules_still_around), dtype=array_dtype)
    to_delete_rules['dataset_did'] = np.array(dids_rucio_rules_still_around)
    to_delete_rules['rse'] = np.array(dids_rucio_rules_at)

    today = datetime.now().strftime('%Y%m%d%H%M')
    output_dir = os.path.join(output_dir_base, today)

    if save:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        print('Your lists are at %s'%(output_dir))
        np.save(os.path.join(output_dir, 'delete_rules.npy'), np.array(to_delete_rules))
        np.save(os.path.join(output_dir, 'delete_rundb.npy'), np.array(dids_in_db))
        np.save(os.path.join(output_dir, 'erase_rules.npy'), np.array(missing_rules))

    return (
        to_delete_rules,
        np.array(dids_files_still_around),
        np.array(dids_in_db),
        np.array(empty_rules),
        np.array(missing_rules)
    )

(
    to_delete_rules,
    dids_files_still_around,
    dids_in_db,
    empty_rules,
    missing_rules
) = analyze_unfinshed(st, all_sr1, dtypes_to_check=osg_all_dtypes, save=True)
