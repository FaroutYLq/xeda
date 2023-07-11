import numpy as np
import time
import os, shlex
#from subprocess import Popen, PIPE, STDOUT, TimeoutExpired
import utilix
from utilix.batchq import *
from datetime import datetime


USER = os.environ['USER']
DIR_PATH = os.path.dirname(os.path.realpath(__file__))
SCRIPT_PATH = os.path.join(DIR_PATH, "find_rules.py")


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
        run_id = loop_item
        # Modify here for the script to run
        jobstring = "python %s %s"%(SCRIPT_PATH, run_id)
        print(jobstring)

        # Modify here for the log name
        utilix.batchq.submit_job(
            jobstring, 
            log='/home/%s/.tmp_job_submission/find_rules_%s_%s.log'%(USER, 
                                                                     datetime.now().strftime('%Y%m%d'), 
                                                                     run_id), 
            partition='xenon1t', qos='xenon1t',
            account='pi-lgrandi', jobname=jobname,
            delete_file=True, dry_run=False, mem_per_cpu=5000,
            container='xenonnt-development.simg',
            cpus_per_task=1)

p = Submit()

# Modify here for the runs
loop_over = np.arange(20)

print('Runs to process: ', len(loop_over))

p.execute(loop_over=loop_over, max_num_submit=30, nmax=10000)
