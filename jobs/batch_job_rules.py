import numpy as np
import time
import os
import utilix
from utilix.batchq import *
from datetime import datetime
from utilix import xent_collection


MAX_RUN_NUMBER = xent_collection().count_documents({})
N_JOBS = 20
USER = os.environ['USER']
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SCRIPT_PATH = os.path.join(DIR_PATH, "../xeda/find_rules.py")
OUTPUT_PATH = '/project2/lgrandi/yuanlq/shared/rucio_scan/'


class Submit(object):
    '''
        Take maximum number of nodes to use at once
        Submit each group to a node and excute
    '''
    def name(self):
        return self.__class__.__name__

    def execute(self, *args, **kwargs):
        eval('self.{name}(*args, **kwargs)'.format(name = self.name().lower()))

    def submit(self, loop_over=[], max_num_submit=20, nmax=3):
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
        cmd='squeue --user={user} | wc -l'.format(user = USER)
        jobNum=int(os.popen(cmd).read())
        return  jobNum -1

    def _submit_single(self, loop_index, loop_item):
        jobname = 'find_rules{:03}'.format(loop_index)
        job_i = loop_item
        # Modify here for the script to run
        jobstring = "python %s %s %s %s"%(SCRIPT_PATH, 
                                          job_i, 
                                          int(MAX_RUN_NUMBER/N_JOBS), 
                                          OUTPUT_PATH)
        print(jobstring)

        # Modify here for the log name
        utilix.batchq.submit_job(
            jobstring=jobstring, 
            log='/home/%s/.tmp_job_submission/find_rules/find_rules_%s_%s.log'%(USER,
                                                                     datetime.now().strftime('%Y%m%d'), 
                                                                     job_i), 
            partition='xenon1t', qos='xenon1t',
            account='pi-lgrandi', jobname=jobname,
            dry_run=False, mem_per_cpu=5000,
            container='xenonnt-2023.11.1.simg',
            cpus_per_task=1)

p = Submit()

# Modify here for the runs
loop_over = np.arange(N_JOBS)
print('Batch of runs to process: ', len(loop_over))
p.execute(loop_over=loop_over, max_num_submit=2000, nmax=10000)

print("Find your outputs at %s"%OUTPUT_PATH)
