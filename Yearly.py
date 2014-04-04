#!/usr/bin/env python

import stats
import sys

year=sys.argv[1]

theJobs = stats.getJobs(['stats/*'+year])
print "%%%%%%%% Report for " + year + " %%%%%%%%"
print


print "--- Summary"
groups = stats.Collect(theJobs,'group','JOBEND')
jobs=[(group,len(groups[group]),sum(groups[group])) for group in groups.keys()]
jobs.sort(key=lambda tup: tup[2])
for job in jobs:
  print "%18s: %7d jobs; %12.2f cpu-hours" % job
print "--------------------------------------------------------"
#print "%10s  %5d jobs; %10.2f cpu-hours" % ("",len(groups.values()),sum(groups.values()))
print



queues = stats.Collect(theJobs,'queue','JOBEND')
for queue in sorted(queues.keys()):
  groups = stats.Collect(queues[queue],'group')
  print "--- " + queue.upper()

  jobs=[(group,len(groups[group]),sum(groups[group])) for group in groups.keys()]
  jobs.sort(key=lambda tup: tup[2])
  for job in jobs:
    print "%18s: %7d jobs; %12.2f cpu-hours" % job
  print "--------------------------------------------------------"
  print

