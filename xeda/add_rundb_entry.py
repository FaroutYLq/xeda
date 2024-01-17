import numpy as np
from rucio.client.replicaclient import ReplicaClient
from rucio.client.ruleclient import RuleClient
from rucio.client.client import Client
from rucio.client.didclient import DIDClient
from rucio.common.exception import DataIdentifierNotFound
import straxen
import cutax
import argparse
import time
from utilix import DB, uconfig

def look_up(site, runs):
    """
    Look up at what data is on runsDB
    then see if there is an existing copy in rucio
    update runsDB if necessary 
    """

    rule_client = RuleClient()
    did_client = DIDClient()
    client = Client(account="production")


    st = cutax.xenonnt_offline()
    osg_all_dtypes = ['pulse_counts', 'veto_regions', 'veto_intervals', 'lone_hits', 'peaklets', 'merged_s2s', 'peaklet_classification', 'peak_basics', 
                  'distinct_channels', 'event_basics', 'corrected_areas', 'energy_estimates', 'event_info',
                  'event_pattern_fit', 'event_positions', 'peak_positions_gcn', 'peak_positions_cnn', 
                  'peak_proximity', 'peak_positions_mlp', 'event_n_channel', 'peak_s1_positions_cnn', 
                  'peak_corrections', 'event_area_per_channel', 'event_ms_naive', 'event_top_bottom_params', 
                  'event_shadow', 'event_ambience']

    runsDB_API = DB()

    for run in runs:
        print('=== checking this run:',run)
        for dtype in osg_all_dtypes:
            st_hashes = st.key_for(str(run), dtype)
            # hashes are of the form 
            # 51862-corrected_areas-7syf5za2jv
            name = str(st_hashes).split('-', 1)[1]
            scope = "xnt_0"+str(run)
            _r = runsDB_API.get_doc(run)
            data_addendum = None
            for d in _r['data']:
                if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['did'] == scope+':'+name:
                    data_addendum = d
                    break
            if data_addendum is None:
                continue
            # this dtype exists and has a rucio location... fine
            if site not in data_addendum['location']:
                find_it = False
                for mdata in rule_client.list_replication_rules({'scope': scope}):
                    if not scope  in mdata['scope'] or not mdata['name'] in name:
                        continue
                    if mdata['state'] != 'OK':
                        continue
                    # if dtype exits in site, try to update runsdb
                    if mdata["rse_expression"] == site:
                        find_it = True
                        break
                if find_it: 
                    n_files = 0
                    total_size = 0.
                    for file_info in did_client.list_files(scope, mdata['name']):
                        total_size += file_info['bytes']
                        n_files += 1
                    total_size /= 1.e6
                    update_RunsDB(runsDB_API, run, mdata, total_size, n_files)

	

def update_RunsDB(db, run, data, total_size, n_files):
    """
    Update runsDB with the new location of the data
    """
    # using runsDB API
    print('we are updating...')
    # update runDB
    new_data_dict = dict()
    new_data_dict['status'] = 'transferred'
    new_data_dict['location'] = data['rse_expression']
    new_data_dict['creation_time'] = data['created_at'].isoformat()
    new_data_dict['did'] = data['scope']+':'+data['name']
    new_data_dict['host'] = "rucio-catalogue"
    new_data_dict['type'] = data['name'].split('-',1)[0] 
    new_data_dict['protocol'] = 'rucio'
    new_data_dict['meta'] = dict(lineage_hash=data['name'].split('-',1)[1],
                                             file_count=n_files,
                                             size_mb=total_size,
                                             type=data['name'].split('-',1)[0],
                                             host="rucio-catalogue",
                                             did=data['scope']+':'+data['name'],
                                             protocol='rucio'
                                )
    print(new_data_dict)
    # If the data already exist, it won't do anything... this saved the day    
    db.update_data(run, new_data_dict)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--run_list_file', type=str, help='path to run list file, fmt=npz')
    parser.add_argument('--site', type=str, help='RSE where data was transfer but no update in rundbs')
    args = parser.parse_args()
 
    site = args.site  
    _file = args.run_list_file
    runs = np.load(_file, allow_pickle=True)['runs']

    look_up(site, runs)