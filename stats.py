#!/usr/bin/env python
import sys
import getopt
import glob
import time

class Job(object):
  def __init__(self,stats):
    try:
      # The status of this event entry
      self.status=stats[4]

      # user information
      self.user=stats[7]
      self.group=stats[8]
      self.account=stats[28]

      # job information
      self.jobID=stats[3]

      #requested resources
      self.queue=stats[11].replace("[","").replace("]","").replace(":1","")
      self.memory=stats[37]
      self.partition=stats[33]
      self.features=stats[22].replace("[","").replace("]","")
      self.qos=stats[26].split(":")[0]
      self.qosDelivered=stats[26].split(":")[1]

      self.nodes=int(stats[5])
      if self.nodes==0:
        self.nodes=1

      self.cpus=int(stats[6])
      self.ppn=self.cpus/self.nodes
      self.reqwall=int(stats[9])/60./60.

      # epoch times; be careful! I have found cases where
      # they come out as 0
      self.start=int(stats[14])
      self.end=int(stats[15])
      self.submit=int(stats[12])
      self.eligible=int(stats[55])

      #This is a problem. There is another field I can use
      if (self.start == 0):
        self.cputime=float(stats[32])
      else:
        self.cputime=(self.end - self.start) * self.cpus/60./60.

      self.runtime=self.cputime/self.cpus
      self.queued=self.eligible/60./60.

      if (self.start == 0 or self.submit == 0):
        self.wait=0
        self.blocked=0
      else:
        self.wait=(self.start - self.submit) /60./60.
        self.blocked=(self.start - self.submit - self.eligible) / 60./60.

      if(self.blocked<0.0):
        self.blocked = 0

      self.submitDate = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(self.submit))
      self.startDate  = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(self.start))
      self.endDate  = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(self.end))
    except:
      #there are cases where a line in the event log has no
      # reasonable information or when a value (like an epoch time)
      # is incorrect. I'm just ignoring those lines
      pass


  # When adding two job objects a new job object is returned
  # where newJob.cputime is the sum of the two jobs.
  # All other attributes come from one of the two objects.
  def __add__(self,other):
    try:
      return self.cputime+other.cputime
    except:
      return 0.0
  def __radd__(self,other):
    try:
      return other + self.cputime
    except:
      return self


# Make dictionaries (hastables) based on the 'key'.
# key can be any of the above attributes and must be provided
# as a string, like 'queue', 'user', 'reqwall'
# Stat can be 'JOBEND','JOBSTART','JOBCANCEL' or just ignored.
def Collect(myStats,key,stat=None):
  Collected = {}
  for job in myStats:
    if stat != None:
      try:
        if job.status == stat:
          value = job.__dict__[key]
          if value in Collected:
            Collected[value].append(job)
          else:
            Collected[value] = [job]
      except:
        pass
    else:
      try:
        value = job.__dict__[key]
        if value in Collected:
          Collected[value].append(job)
        else:
          Collected[value] = [job]
      except:
        pass
  return Collected


# Read the event logs
# The input is a list of globular-aware file names with paths, like
# ['stats/events.*Feb*2014','stats/events.*Mar*2014']
def getJobs(files):
  theFiles = [glob.glob(thisFile) for thisFile in files]
  #make one list
  theStats = [y for x in theFiles for y in x]
  Stats = list()
  for f in theStats:
    with open(f) as myFile:
      [Stats.append(Job(l.split())) for l in (line.strip() for line in myFile) if l]
  return Stats

# Main is not programmed. See other scripts for example usage
def main(argv):
  try:
    opts, args = getopt.getopt(argv,"h")
  except getopt.GetoptError:
    print 'stats.py -s start-date -e end-date'
    sys.exit(2)
  for opt, arg in opts:
    if opt == '-h':
      print 'stats.py -s start-date -e end-date'
      sys.exit()

if __name__ == "__main__":
  main(sys.argv[1:])
