#!/usr/bin/env python

import stats
import sys

month=sys.argv[1]
year=sys.argv[2]

theJobs = stats.getJobs(['stats/*'+month+'*'+year])
print "%%%%%%%% Report for " + month + " " + year + " %%%%%%%%"
print


print "--- Summary"
groups = stats.Collect(theJobs,'group','JOBEND')
jobs=[(group,len(groups[group]),sum(groups[group])) for group in groups.keys()]
jobs.sort(key=lambda tup: tup[2])
for job in jobs:
  print "%13s: %5d jobs; %10.2f cpu-hours" % job
print "-----------------------------------------------"
print



queues = stats.Collect(theJobs,'queue','JOBEND')
for queue in sorted(queues.keys()):
  groups = stats.Collect(queues[queue],'group')
  print "--- " + queue.upper()

  jobs=[(group,len(groups[group]),sum(groups[group])) for group in groups.keys()]
  jobs.sort(key=lambda tup: tup[2])
  for job in jobs:
    print "%13s: %5d jobs; %10.2f cpu-hours" % job
  print "-----------------------------------------------"
  print

