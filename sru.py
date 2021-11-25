#!/usr/bin/env python3

from slurmstat import SlurmStat

sru = SlurmStat()

sru.getopt() 
sru.sacct()
sru.summary()
