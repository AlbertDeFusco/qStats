#!/usr/bin/env python

import stats
import sys

#Month must be given as three letters with the first capatilized
month=sys.argv[1]
year=sys.argv[2]
myGroup=sys.argv[3]


print "%%%%%%%% Report for " + myGroup + " group " + month + " " + year + " %%%%%%%%"
print

theJobs = stats.getJobs(['stats/*'+month+'*'+year],True)
thisGroup = stats.Collect(theJobs,'account','JOBEND')[myGroup]
users = stats.Collect(thisGroup,'user')

print "--- Summary"
jobs=[(user,len(users[user]),sum(users[user]),stats.sumSU(users[user])) for user in users.keys()]
jobs.sort(key=lambda tup: tup[2])
for job in jobs:
  print "%10s: %5d jobs; %10.2f cpu-hours; %10.2f SU" % job
print "-----------------------------------------------------------"
print "%10s  %5d jobs; %10.2f cpu-hours; %10.2f SU" \
  % ("Total",len(thisGroup),sum(thisGroup),stats.sumSU(thisGroup))
print


queues = stats.Collect(thisGroup,'queue')
for queue in sorted(queues.keys()):
  print "--- " + queue.upper()
  users = stats.Collect(queues[queue],'user')

  jobs=[(user,len(users[user]),sum(users[user]),stats.sumSU(users[user])) for user in users.keys()]
  jobs.sort(key=lambda tup: tup[2])
  for job in jobs:
    print "%10s: %5d jobs; %10.2f cpu-hours; %10.2f SU" % job
  print "-----------------------------------------------------------"
  print


