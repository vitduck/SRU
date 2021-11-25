#!/usr/bin/env python3 

import re 
import argparse
import subprocess

from pprint      import pprint
from tabulate    import tabulate
from datetime    import date, datetime, timedelta
from collections import defaultdict

class SlurmStat:
    version = '0.1'

    def __init__(self): 
        self.cur   = date.today()
        self.end   = self.cur.replace(day=1) - timedelta(days=1)
        self.start = self.end.replace(day=1) 

        self.time  = self._autovivication() 
        self.usage = self._autovivication() 

    def debug(self): 
        pprint(vars(self))

    def getopt(self): 
        parser = argparse.ArgumentParser(description='SLURM Accounting Utility')
        
        parser.add_argument('--start', type=str, default=str(self.start), help='Starting date')
        parser.add_argument('--end'  , type=str, default=str(self.end)  , help='Ending date'  )
        
        opts = parser.parse_args() 

        self.start = opts.start 
        self.end   = opts.end 

    def sacct(self): 
        cmd = [
            'sacct', 
                '-n','-X', '-T',
                '-S', self.start,
                '-E', self.end,
                '--format=partition%20,user%20,nodelist,alloctres%60,start,end' ]

        pout = subprocess.check_output(cmd, stderr=subprocess.PIPE).decode('utf-8').strip()

        # parse sacct output 
        for line in pout.splitlines(): 
            if re.search('None assigned', line): 
                continue
            
            partition, user, nodelist, resource, start, end = line.split()
            ncpus, ngpus, nnodes = self._alloctres(resource)
            nodes = self._nodelist(nodelist)  

            # build hash of resource 
            if not self.usage[partition][user]: 
                self.usage[partition][user] = { 'jobs':0, 'nnodes':0, 'ncpus':0, 'ngpus':0, 'sru':0 }
                
            self.usage[partition][user]['jobs'  ] += 1
            self.usage[partition][user]['nnodes'] += int(nnodes)
            self.usage[partition][user]['ncpus' ] += int(ncpus)
            self.usage[partition][user]['ngpus' ] += int(ngpus)

            #build hash of start/end time
            for node in nodes: 
                if not self.time[partition][user][node]: 
                    self.time[partition][user][node] = [] 
                    
                self.time[partition][user][node].append([self._fromisoformat(start), self._fromisoformat(end)])

        # process timestamps
        for partition in self.time:
            for user in self.time[partition]: 
                for node in self.time[partition][user]: 
                    timestamp = self.time[partition][user][node] 

                    # initial timestamp
                    start0, end0 = timestamp.pop(0) 
                    self.usage[partition][user]['sru'] += (end0 - start0).total_seconds()

                    while (len(timestamp)):
                        start1, end1 = timestamp.pop(0)

                        # timestamp overlap
                        if start1 < end0 and end1 > end0:
                            self.usage[partition][user]['sru'] += (end1 - end0).total_seconds()  

                        # reset timestamp 
                        if start1 > end0: 
                            self.usage[partition][user]['sru'] += (end1 - start1).total_seconds()  
                            
                            start0 = start1 
                            end0   = end1 

    def summary(self):
        print(f'\nUsage statistics from {self.start} to {self.end}')

        for partition in self.usage:
            table = [] 

            for user in sorted(self.usage[partition].keys()): 
                # convert seconds to DD-HH:MM:SS
                self.usage[partition][user]['sru'] = timedelta(seconds=self.usage[partition][user]['sru'])

                table.append([user] + list(self.usage[partition][user].values()))

            if table: 
                print(f'\n<{partition}>')

                print(tabulate(
                    table, 
                    headers=['USER', 'JOBS', 'NODES', 'CPUS','GPUS', 'USAGE'], 
                    tablefmt='pretty', 
                    stralign='right' ))

    def _autovivication(self): 
        nested_dict = lambda: defaultdict(nested_dict)

        return nested_dict() 

    def _nodelist(self, string):  
        slurm_nodes = [] 
        name, index = re.search('([a-zA-Z]+)\[?([0-9\-,]+)\]?', string).group(1,2)

        for node in index.split(','):
            if re.search('-', node):
                istart, iend = node.split('-')
                
                # return the list of node with leading zeroes
                slurm_nodes += [ name + str(node).zfill(len(istart)) for node in range(int(istart), int(iend)+1) ]
            else:
                slurm_nodes += [ name + str(node) ]

        return slurm_nodes

    def _alloctres(self, string): 
        ncpus, ngpus, nnodes = re.search('cpu=(\d+)(?:.+gpu=(\d+))?.+node=(\d+)', string).groups()

        # without --gres
        if not ngpus: 
            ngpus = 0

        return ncpus, ngpus, nnodes

    def _fromisoformat(self, date_string): 
        year, month, day, hour, minute, second = re.search('(\d+)-(\d+)-(\d+)T(\d+):(\d+):(\d+)', date_string).groups()

        return datetime(int(year), int(month), int(day), int(hour), int(minute), int(second))
