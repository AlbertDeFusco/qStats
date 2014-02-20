#!/usr/bin/env python
import sys
import getopt
import glob
import time

class Job(object):
  def __init__(self,stats):
    try:
      self.user=stats[7]
      self.group=stats[8]
      self.jobID=stats[3]
      self.status=stats[4]
      self.queue=stats[11].replace("[","").replace("]","").replace(":1","")
      self.memory=stats[37]
      self.partition=stats[33]
      self.features=stats[22].replace("[","").replace("]","")
      self.account=stats[28]
      self.qos=stats[26].split(":")[0]
      self.qosDelivered=stats[26].split(":")[1]
      self.nodes=int(stats[5])
      self.cpus=int(stats[6])
      self.ppn=self.cpus/self.nodes
      #epoch times; be careful!
      self.start=int(stats[14])
      self.end=int(stats[15])
      self.submit=int(stats[12])
      self.eligible=int(stats[55])
      #derived
      self.reqwall=int(stats[9])/60./60.

      #This is a problem. There is another field I can use
      if (self.start == 0):
        #self.cputime=0.0
        self.cputime=float(stats[32])
      else:
        self.cputime=(self.end - self.start) * self.cpus/60./60.

      self.runtime=self.cputime/self.cpus
      self.blocked=(self.start - self.submit - self.eligible) / 60./60.
      self.queued=self.eligible/60./60.
      self.wait=(self.start - self.submit) /60./60.

      self.submitDate = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(self.submit))
      self.startDate  = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(self.start))
      self.endDate  = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(self.end))
    except:
      pass
  #these work for only one attribute?
  def __add__(self,other):
    return self.cputime+other.cputime
  def __radd__(self,other):
    return other + self.cputime


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


def getJobs(files):
  theFiles = [glob.glob(thisFile) for thisFile in files]
  #make one list
  theStats = [y for x in theFiles for y in x]
  Stats = list()
  for f in theStats:
    with open(f) as myFile:
      [Stats.append(Job(l.split())) for l in (line.strip() for line in myFile) if l]
  #print "%d jobs" % len(Stats)
  return Stats

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
