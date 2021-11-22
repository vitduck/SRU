#!/usr/bin/env python3 

import re
import os
import getpass
import subprocess
import itertools
import argparse

from   tabulate    import tabulate
from   datetime    import date, timedelta
from   collections import defaultdict

# start/end date of last month 
current    = date.today()
end_date   = current.replace(day=1) - timedelta(days=1)
start_date = end_date.replace(day=1) 

# cmdline options 
parser = argparse.ArgumentParser(description='SLURM Accounting Utility')
parser.add_argument('--start', type=str, default=str(start_date), help='Starting date')
parser.add_argument('--end'  , type=str, default=str(end_date)  , help='Ending date'  )
opts = parser.parse_args() 

# autovivication 
nested_dict = lambda: defaultdict(nested_dict)
sru_time    = nested_dict()

# slurm statistics
cmd = [
    'sacct', 
        '-n','-X', '-T',
        '-S', opts.start,
        '-E', opts.end,
        '--format=nodelist,partition%20,user%20,alloctres%60,cputimeraw'
]

pout = subprocess.check_output(cmd, stderr=subprocess.PIPE).decode('utf-8').strip()

# parse output of sacct
for line in pout.splitlines():
    if re.search('None assigned', line): 
        continue 
    
    nodelist, partition, user, resource, cputime = line.split()

    # extract billing information
    ncpus, ngpus, nnodes = re.search('cpu=(\d+)(?:.+gpu=(\d+))?.+node=(\d+)', resource).groups()
    
    # cpu-only nodes 
    if not ngpus: 
        ngpus = 0

    # initialization dict
    if not sru_time[partition][user]:
        sru_time[partition][user] = {'count':0, 'nnodes':0, 'ncpus':0, 'ngpus':0, 'cputime':0, 'gputime':0, 'elapsed':0}
    
    sru_time[partition][user]['count'  ] += 1
    sru_time[partition][user]['nnodes' ] += int(nnodes)
    sru_time[partition][user]['ncpus'  ] += int(ncpus)
    sru_time[partition][user]['ngpus'  ] += int(ngpus)
    sru_time[partition][user]['cputime'] += int(cputime)
    sru_time[partition][user]['gputime'] += int(ngpus)*int(cputime)/int(ncpus)
    sru_time[partition][user]['elapsed'] += int(cputime)/int(ncpus)

# print result
print(f'\nUsage statistics from {opts.start} to {opts.end}')
for partition in sru_time: 
    table = [] 
    for user in sorted(sru_time[partition].keys()): 
        # conver sec to d-hh-mm-ss format 
        sru_time[partition][user]['cputime'] = timedelta(seconds=sru_time[partition][user]['cputime'])
        sru_time[partition][user]['gputime'] = timedelta(seconds=sru_time[partition][user]['gputime'])
        sru_time[partition][user]['elapsed'] = timedelta(seconds=sru_time[partition][user]['elapsed'])

        table.append([user] + list(sru_time[partition][user].values()))

    if table: 
        print(f'\n<{partition}>')
        print(tabulate(
            table, 
            headers=['USER', 'JOBS', 'NODES', 'CPUS','GPUS', 'CPU_TIME', 'GPU_TIME', 'ELAPSED_TIME'], 
            tablefmt='psql', 
            stralign='right')
        )
