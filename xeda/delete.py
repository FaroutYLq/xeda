# This script somehow only works well on the old OSG login nodes...

import csv
from rucio.client.client import Client
from rucio.client.client import ReplicaClient
from rucio.client.fileclient import FileClient
from utilix.config import Config
from utilix.rundb import xent_collection
from tqdm import tqdm
import gfal2
import time
import numpy as np
import datetime
import sys


_, did_npy_path = sys.argv


np_load_old = np.load
np.load = lambda *a,**k: np_load_old(*a, allow_pickle=True, **k)

# the datatype is like [('dataset_did','O'),('rse', 'O')]
dids_to_remove = np.load(did_npy_path)
print("datasets to remove: ",len(dids_to_remove))


c = Client()
r = ReplicaClient()
ctx = gfal2.creat_context()



def RemoveDatafield(db, id_field, rem_dict):
    new_entry = dict(rem_dict.items())
    new_entry.update({"at" : datetime.datetime.utcnow(), "by" : "admix"})
    
    db.update_one({"_id" : id_field},
                              {"$pull" : {"data" : rem_dict},
                               "$push" : {"deleted_data" : new_entry}})


db = xent_collection()

# 0 means doesn't require replica. 1 means requires one replica.
rse_redundancy = 0

for did_info in tqdm(dids_to_remove):
    did = did_info['dataset_did']
    rse = did_info['rse']

    print("Checking DID {0} in RSE {1}".format(did,rse))
    scope = did.split(":")[0]
    name = did.split(":")[1]
    rules = list(c.list_did_rules(scope,name))
    rule_to_delete_exists = False
    rule_to_delete_id = ''
    nrules_to_keep_is_ok = 0
    nfiles_to_delete = -1
    one_rule_to_keep_is_bad = False

#    print(did,rse)

    for rule in rules:
        if rule['rse_expression']==rse:
            rule_to_delete_exists = True
            nfiles_to_delete = rule['locks_ok_cnt']
            rule_to_delete_id = rule['id']
            #print('  Checking {0} which is equal to {1}'.format(rule['rse_expression'],rse))

    if not rule_to_delete_exists:
        print("Cannot find rule to delete for dataset {0} in {1}".format(did,rse))
        continue

    for rule in rules:
        if rule['rse_expression']!=rse:
            if rule['state']=='OK' and rule['copies']>=1 and rule['locks_ok_cnt']==nfiles_to_delete and rule['locks_replicating_cnt']==0 and rule['locks_stuck_cnt']==0:
                nrules_to_keep_is_ok = nrules_to_keep_is_ok + 1
            else:
                one_rule_to_keep_is_bad = True

            # reading physical presence of all files
            file_did = {'scope': scope, 'name': name}
            replicas = list(r.list_replicas([file_did],rse_expression=rule['rse_expression']))
            #print('  Checking {0} which is different from {1}'.format(rule['rse_expression'],rse))
            #print(replicas)
            for file in replicas:
                #print('    Checking file ',file['rses'][rule['rse_expression']][0])
                gfal_filename = file['rses'][rule['rse_expression']][0]
                try:
                    gfal_filestat = ctx.lstat(gfal_filename)
                except gfal2.GError:
                    print("gfal2 error on file {0} from did {1} in rse {2}".format(gfal_filename,did,rule['rse_expression']))
                    print("gfal2 error!")
                gfal_filesize = gfal_filestat.st_size
                if file['bytes']!=gfal_filesize:
                    print("Error! File {0} from dataset {1} is absent in {2}".format(gfal_filename,did,rule['rse_expression']))
                    one_rule_to_keep_is_bad = True


    if not rule_to_delete_exists or nrules_to_keep_is_ok < rse_redundancy or one_rule_to_keep_is_bad:
        print("Error! Dataset {0} cannot be deleted in {1}".format(did,rse))
        print("Rule to delete exists: {0}".format(rule_to_delete_exists))
        print("Number of rules to keep is ok: {0}".format(nrules_to_keep_is_ok))
        print("One rule to keep is bad: {0}".format(one_rule_to_keep_is_bad))
        errfile = open('baddatasets.txt', 'a')
        errfile.write('{0}, {1}'.format(did,rse))
        errfile.write("\n")
        errfile.close()
    else:
        print("  Deleting {0} in {1}: {2}".format(did,rse,rule_to_delete_id))
        try:
            c.update_replication_rule(rule_to_delete_id, {'account' : 'production'})
            c.delete_replication_rule(rule_to_delete_id, purge_replicas=True)
            time.sleep(30)
        except:
            print("The rule id %s does not exist?"%(rule_to_delete_id))

        #hash = did.split('-')[-1]
        #dtype = did.split('-')[0].split(':')[-1]
        #number = int(did.split(':')[0].split('_')[-1])

        #print("  Deleting the db entry {0} of the rse {1}".format(did,rse))
        #print("  Run number: {0}".format(number))
        #print("  Data type: {0}".format(dtype))
        #print("  Hash: {0}".format(hash))

        #run = db.find_one({'number' : number})

        #Checks if the datum exists in the DB
        #datum = None
        #for d in run['data']:
        #    if d['type'] == dtype and d['host'] == 'rucio-catalogue' and d['location'] == rse:
        #        datum = d
        #        break
          
        #Delete the datum
        #if datum is not None:
            #RemoveDatafield(db, run['_id'],datum)
            #print(datum)
            #print("Datum deleted in DB.")
        #else:
            #print('There is no datum to delete')




#    print(replicas)
