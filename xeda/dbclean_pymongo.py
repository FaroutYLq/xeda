# This script only works on rundb machine!!
# EXTREMELLY DANGEROUS: DO NOT RUN UNLESS YOU KNOW WHAT YOU ARE DOING!

import pymongo
import numpy as np

delete_dids = np.load("/home/centos/clean_up_rundb/20230903/deletion_trial_dids_osg.npy")
rse = "UC_OSG_USERDISK"
password = "ASK TEAM A"

client = pymongo.MongoClient('mongodb://nt_bookkeeping:%s@xenon1t-daq.lngs.infn.it:27017/xenonnt'%(password))
rundb = client['xenonnt']['runs']
run_docs = rundb.find({}, projection={"number": 1, "data": 1})

ndel = {}
nentries = {}

def keep(d):
    return d.get("did", None) not in delete_dids or d.get("location", None) != rse


for doc in run_docs:
    runid = doc.get("number", None)
    if runid is None:
        continue
    
    data = doc.get("data", None)
    if data is None:
        continue

    new_data = list(filter(keep, data))
    if len(new_data) == len(data):
        continue

    try:
        # This is the dangerous line!
        #rundb.update_one({"number": runid,}, {"$set": {"data": new_data} })
        print("Done for %s"%(runid))
    except:
        print("%s Failed?!"%(runid))
    