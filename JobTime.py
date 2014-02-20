#!/usr/bin/env python

import stats
import sys
import pylab

events=sys.argv[1]

jobs=stats.getJobs(['stats/events.'+events])

binwidth=1

Runtimes = list()
for job in jobs:
  try:
    if (job.status == "JOBEND"):
      if(job.runtime>0.5 and job.runtime<168.0):
        Runtimes.append(job.runtime)
  except:
    pass

Runtimes.sort()
pylab.title("All jobs")
pylab.xlabel("Run time (hours)")
pylab.ylabel("Jobs")
pylab.hist(Runtimes,bins=range(int(min(Runtimes)),int(max(Runtimes))+binwidth,binwidth))
pylab.savefig("JobTime/Runtime.alljobs.png")
pylab.clf()

queues = stats.Collect(jobs,'queue','JOBEND')
for queue in sorted(queues.keys()):
  print "# " + queue.upper()
  Runtimes = list()
  for job in queues[queue]:
    try:
      if(job.runtime>0.5 and job.runtime<168.0):
        Runtimes.append(job.runtime)
    except:
      pass
  Runtimes.sort()
  pylab.title(queue.upper() + " jobs")
  pylab.xlabel("Run time (hours)")
  pylab.ylabel("Jobs")
  bins=range(int(min(Runtimes)),int(max(Runtimes))+2*binwidth,binwidth)
  pylab.hist(Runtimes,bins)
  pylab.xlim((pylab.xlim()[0],pylab.xlim()[1]+2*binwidth))
  pylab.savefig("JobTime/Runtime."+queue+".png")
  pylab.clf()
