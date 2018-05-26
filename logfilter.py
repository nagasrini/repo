from datetime import datetime
import os, sys
import mimetypes
import gzip
import re

home = "/home/nutanix/"
for path in os.listdir("/usr/local/nutanix/lib/py"):
        sys.path.insert(0, os.path.join("/usr/local/nutanix/lib/py", path))
import util.base.log as log
from cerebro.interface.cerebro_interface_pb2 import *
#logentry_re = r'^(\w)(\d+\ \d+\:\d+\:\d+\.\d+) (\d+) (\S+):(\d+). (\w+.*)'
logentry_re = r'^(\w)(\d+\ \d+\:\d+\:\d+\.\d+) (\d+) (\S+):(\d+). (.*)'
logentry_begin_re=r'^(\w)(\d+\ \d+\:\d+\:\d+\.\d+) (\d+) (\S+):(\d+)'
logentry_re_m = r'^(\w)(\d+\ \d+\:\d+\:\d+\.\d+) (\d+) (\S+):(\d+). (\w+.*) [{].*[}]'

logcollector_firstline_re = r'^\*.* = (\d+/\d+/\d+)-(\d+:\d+:\d+).* = (\d+/\d+/\d+)-(\d+:\d+:\d+)'
clusterlog_firstline_re=r'^.*: (\d+/\d+/\d+) (\d+:\d+:\d+)'

class logentry:
  def __init__(self, ip, service, year, entry):
    self.ip = ip;
    self.service = service;
    self.type = entry[0];
    self.date = datetime.strptime(str(year) + entry[1], "%Y%m%d %H:%M:%S.%f");
    self.thrid = entry[2];
    self.file = entry[3];
    self.line = entry[4];
    self.msg = entry[5];

  def get_str(self):
    print ("|%s| %s %s|%s|%s:%s %s") % (self.ip, self.type, self.date.strftime("%m%d %H:%M:%S.%f"), self.thrid, self.file, self.line, self.msg)

  def prn(self):
    print self.get_str()

  def __repr__(self):
    return ("|%s| %s %s|%s|%s:%s %s") % (self.ip, self.type, self.date.strftime("%m%d %H:%M:%S.%f"), self.thrid, self.file, self.line, self.msg)

  def appendmsg(self, msg):
    self.msg = self.msg + msg

  def __eq__(self, other):
    return self.date == other.date

  def __ne__(self, other):
    return self.date != other.date

  def __lt__(self, other):
    return self.date < other.date

  def __le__(self, other):
    return self.date <= other.date

  def __gt__(self, other):
    return self.date > other.date

  def __ge__(self, other):
    return self.date >= other.date


class filter:
  def __init__(self):
    self.ip = "0"
    self.service = "cerebro"
    self.type = "INFO"
    self.datemax = datetime.today()
    self.datemin = datetime.min
    self.rep = []

  def set_type(self, type):
    self.type = type.upper()

  def set_re(self, restr):
    self.rep.append(re.compile(restr))

  def set_datemin(self, dstr):
    '''
    Specify date in "%Y%m%d %H:%M:%S.%f" format, ex:20180526 02:31:01.586469"
    '''
    self.datemin = datetime.strptime(dstr, "%Y%m%d %H:%M:%S.%f")

  def set_datemax(self, dstr):
    '''
    Specify date in "%Y%m%d %H:%M:%S.%f" format, ex:20180526 02:31:01.586469"
    '''
    self.datemax = datetime.strptime(dstr, "%Y%m%d %H:%M:%S.%f")

#pathre=r"(?P<service>^.*)\.[nN][tT][nN][xX].*\.log\..*\.(?P<date>\d+)-(?P<time>\d+).(?P<ddd>\d+)"
pathre=r"^.*\.log.*\.(?P<date>\d+)-(?P<time>\d+)\.(?P<ddd>.*)"
def readlog(logdir=None, filter=None):
  if logdir == None:
    logdir = home + "data/logs/"

  loglist=[]
  for f in os.listdir(logdir):
    le = None
    d1 = None
    d2 = None

    # try to avoid logs not in filter criteria
    if filter:
      if filter.service and filter.service not in f:
        #print "skipping %s" % f
        continue
      if filter.type not in f:
        continue

    match = re.compile(pathre).match(f)
    if match:
      md=match.groupdict()
      start_date = datetime.strptime(md["date"] + md["time"], "%Y%m%d%H%M%S")
      if filter and filter.datemin and filter.datemax < start_date:
        print "skipping %s" % (filter.datemin,f)
        continue

    logfile=os.path.join(logdir, f);
    if os.path.isfile(logfile):
      log.INFO("Processing %s" % logfile)
      print "Processing %s" % logfile
      if mimetypes.guess_type(logfile)[1] == 'gzip':
        file=gzip.open(logfile)
      else:
        file=open(logfile)

      line = file.readline()
      # beginning line from log collector
      lst=re.findall(logcollector_firstline_re, line)
      if lst:
        d1 = datetime.strptime(lst[0][0] + lst[0][1], "%Y/%m/%d%H:%M:%S")
        d2 = datetime.strptime(lst[0][2] + lst[0][3], "%Y/%m/%d%H:%M:%S")
      else:
        lst = re.findall(clusterlog_firstline_re, line)
        if lst:
          d1 = datetime.strptime(lst[0][0] + lst[0][1], "%Y/%m/%d%H:%M:%S")
      if d1:
        year = d1.year

      year_change_hint = False
      for line in file:
        #print "DEBUG: line %s" % (line)
        found = re.findall(logentry_re, line);
        # if line doesn't start with pattern,
        # add it to the previous logentry
        if not found:
          if le:
            le.appendmsg(line)
          continue
        entry = found[0]

        # a little bit of heuristics
        le_year = year
        if "1231" in entry[1]:
          year_change_hint = True
        if "0101" in entry[1] and year_change_hint:
          le_year = year + 1
          year_change_hint = False

        #apply filter if any
        le = logentry("0", "cerebro", le_year, entry)
        if filter and applyfilter(le, filter):
          loglist.append(le)
  loglist.sort()
  return loglist

def applyfilter(record, filter):
  ret = False
  if filter.ip != "0" and record.ip != filter.ip:
    return False
  if filter.datemax < record.date:
    return False
  if filter.datemin > record.date:
    return False
  if filter.service != record.service:
    return False

  if filter.rep:
    for reitem in filter.rep:
      match = reitem.match(record.msg)
      if match:
        ret = True
        break;

  #record.prn()
  return ret
