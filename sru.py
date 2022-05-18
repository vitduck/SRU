#!/usr/bin/env python3 

import re
import os
import subprocess
import argparse
import logging

from tabulate import tabulate
from datetime import date, timedelta
from collections import defaultdict

# logging module
logging.basicConfig( 
    level    = logging.INFO, 
    format   = '%(message)s', 
    handlers = [logging.StreamHandler()])

logger = logging.getLogger()

# default: start/end date of last month 
current    = date.today()
end_date   = current.replace(day=1) - timedelta(days=1)
start_date = end_date.replace(day=1)

# cmdline options 
parser = argparse.ArgumentParser(
    description     = 'SLURM Accounting Utility', 
    formatter_class = lambda prog: argparse.HelpFormatter(prog, max_help_position=40, width=100))

parser.add_argument('-s', '--start', type=str, default=str(start_date), metavar='DATE',     help='starting date')
parser.add_argument('-e', '--end'  , type=str, default=str(end_date)  , metavar='DATE',     help='ending date'  )
parser.add_argument('-u', '--user' , type=str, default=None           , metavar='USERNAME', help='statistics for specific user')

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
        '--format=nodelist,partition%20,user%20,alloctres%60,cputimeraw']

# filter for specific users (only for root)
if os.geteuid() == 0 and opts.user: 
    cmd += ['--user', opts.user]

    # save usage to file
    logger.addHandler(logging.FileHandler(f'{opts.user}_{opts.start}_{opts.end}.txt'))

# capture output of sacct 
pout = subprocess.check_output(cmd, stderr=subprocess.PIPE).decode('utf-8').strip()

# parse output of sacct
for line in pout.splitlines():
    if re.search('None assigned', line): 
        continue 
   
   # extract walltime
    nodelist, partition, user, resource, cputime = line.split()

    # extract billing information
    ncpus, ngpus, nnodes = re.search('cpu=(\d+)(?:.+gpu=(\d+))?.+node=(\d+)', resource).groups()

    # no gpu allocated (free) 
    if not ngpus: 
        ngpus = 0

    # initialization dict
    if not sru_time[partition][user]:
        sru_time[partition][user] = {'count':0, 'nnodes':0, 'ncpus':0, 'ngpus':0, 'usage':0 }
    
    # accumulation
    sru_time[partition][user]['count' ] += 1
    sru_time[partition][user]['nnodes'] += int(nnodes)
    sru_time[partition][user]['ncpus' ] += int(ncpus)
    sru_time[partition][user]['ngpus' ] += int(ngpus)
    sru_time[partition][user]['usage' ] += int(nnodes)*int(ngpus)*int(cputime)/int(ncpus)

# print result
logger.info(f'\nSLURM Usage Statistics from {opts.start} to {opts.end}')

for partition in sru_time: 
    table = [] 

    for user in sorted(sru_time[partition].keys()): 
        # conver sec to d-hh-mm-ss format 
        sru_time[partition][user]['usage'] = timedelta(seconds=sru_time[partition][user]['usage'])
        
        table.append([user] + list(sru_time[partition][user].values()))

    if table: 
        logger.info(f'\n<{partition}>')

        logger.info(tabulate(
            table, 
            headers=['USER', 'JOBS', 'NODES', 'CPUS','GPUS', 'USAGE'], 
            tablefmt='pretty', 
            stralign='right'))
