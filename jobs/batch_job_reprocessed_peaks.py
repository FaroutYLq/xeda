import numpy as np
import time
import os
import utilix
import glob
import pickle
from utilix.batchq import *
print(utilix.__file__)
from datetime import datetime

# Get current date
current_date = datetime.now()
# Format the date to the desired format
formatted_date = current_date.strftime('%Y%m%d')

class Submit(object):
    '''
        Take maximum number of nodes to use at once
        Submit each group to a node and excute
    '''
    def name(self):
        return self.__class__.__name__

    def execute(self, *args, **kwargs):
        eval('self.{name}(*args, **kwargs)'.format(name = self.name().lower()))

    def submit(self, loop_over=[], max_num_submit=10, nmax=3):
        _start = 0
        self.max_num_submit = max_num_submit
        self.loop_over = loop_over
        self.p = True

        index = _start
        while (index < len(self.loop_over) and index < nmax):
            if (self.working_job() < self.max_num_submit):
                self._submit_single(loop_index=index,
                                    loop_item=self.loop_over[index])

                time.sleep(1.0)
                index += 1

    # check my jobs
    def working_job(self):
        cmd='squeue --user={user} | wc -l'.format(user = 'yuanlq')
        jobNum=int(os.popen(cmd).read())
        return  jobNum -1

    def _submit_single(self, loop_index, loop_item):
        jobname = 'peaks_{:03}'.format(loop_index)
        # Modify here for the script to run
        jobstring = "python ./build_peaks.py %s %s"%(loop_item, loop_index)
        print(jobstring)

        # Modify here for the log name
        utilix.batchq.submit_job(
            jobstring, log='/home/yuanlq/.tmp_job_submission/build_peaks/build_peaks_%s.log'%(loop_index), 
            partition='broadwl', qos='broadwl',
            account='pi-lgrandi', jobname=jobname,
            dry_run=False, mem_per_cpu=25000,
            container='xenonnt-development.simg',
            cpus_per_task=1)

def chunk_runs(runs, chunk_size=100, save_dir='/project2/lgrandi/yuanlq/shared/midway_corrupted/20230826/'):
    n_total_chunks = int(len(runs)/chunk_size)+1
    for chunk_i in range(n_total_chunks):
        np.save(save_dir+'chunk_%s.npy'%(chunk_i), runs[(chunk_size*chunk_i):(chunk_size*(chunk_i+1))])

# The peaks that we will try to load
with open('/project2/lgrandi/xenonnt/reprocessing_runlist/global_v11/runlists_reprocessing_global_v11.pickle', 'rb') as f:
    jingqiang = pickle.load(f)
all_prcocessed=np.concatenate((jingqiang['runlists']['sr1_rn220'],
                jingqiang['runlists']['sr1_kr83m'],
                jingqiang['runlists']['sr1_bkg'],
                jingqiang['runlists']['sr1_ybe'],
                jingqiang['runlists']['sr1_rn222'],
                jingqiang['runlists']['sr1_ambe'],
                jingqiang['runlists']['sr0_bkg'],
                jingqiang['runlists']['sr0_rn220'],))
save_dir = '/project2/lgrandi/yuanlq/shared/midway_corrupted/%s/chunks/'%(formatted_date)
os.makedirs(save_dir, exist_ok=True)
print("Dumping the runlists to %s"%(save_dir))
chunk_runs(all_prcocessed, save_dir=save_dir)


p = Submit()
# Modify here for the runs to process
loop_over = glob.glob(save_dir+'*')
print('Run lists to check completeness: ', len(loop_over))
p.execute(loop_over=loop_over, max_num_submit=200, nmax=10000)
