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

  def __init__(self, fstr):
    self.service = None
    self.ip = None
    self.rep = []
    self.parse_filters(fstr)

  # ',cerebro,I,0527 04:04:32.465232,0527 04:15:59,"^.*Top level.*$"'
  ip_re=re.compile(r"(\d+\.\d+\.\d+\.\d+)")
  ftype = {'I': "INFO", 'W': "WARNING", 'E': "ERROR", 'F': "FATAL"}
  def parse_filters(self, fstr):
    l=fstr.split(',')
    m = self.ip_re.match(l[0])
    if m:
      self.ip = m.group()
    if len(l[1]):
      self.service = l[1]
    if len(l[2]):
      self.set_type(l[2])
    if len(l[3]):
      self.set_datemin(l[3])
    if len(l[4]):
      self.set_datemax(l[4])
    if len(l[5]):
      self.set_re(l[5])

  def set_type(self, t):
    self.type = self.ftype[t]

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
 
def read_one_logfile(logfile, filter):
  '''
  reads one log file and returns logentry object
  '''
  le = None
  d1 = None
  d2 = None

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
      lelist.append(le)
  lelist.sort()
  return lelist

#pathre=r"(?P<service>^.*)\.[nN][tT][nN][xX].*\.log\..*\.(?P<date>\d+)-(?P<time>\d+).(?P<ddd>\d+)"
pathre=r"^.*\.log.*\.(?P<date>\d+)-(?P<time>\d+)\.(?P<ddd>.*)"
def readlog(logdir=None, filters=None):
  if logdir == None:
    logdir = home + "data/logs/"

  loglist=[]
  for f in os.listdir(logdir):
    le = None
    d1 = None
    d2 = None

    logfile=os.path.join(logdir, f);
    if not os.path.isfile(logfile):
      continue

    # try to avoid logs not in filter criteria
    for afilter in filters:
      if afilter:
        if afilter.service and afilter.service not in f:
          #print "skipping %s" % f
          continue
        if afilter.type not in f:
          continue

      match = re.compile(pathre).match(f)
      if match:
        md=match.groupdict()
        start_date = datetime.strptime(md["date"] + md["time"], "%Y%m%d%H%M%S")
        if afilter and afilter.datemin and afilter.datemax < start_date:
          print "skipping %s" % (afilter.datemin,f)
          continue

      log.INFO("Processing %s" % logfile)
      print "Processing %s with filter" % logfile
      loglist = read_one_logfile(logfile, afilter)
      break

  loglist.sort()
  return loglist

def applyfilters(record, filters):
  ret = False
  for filter in filters:
    if filter.ip != "0" and record.ip != filter.ip:
      continue
    if filter.datemax < record.date:
      continue
    if filter.datemin > record.date:
      continue
    if filter.service != record.service:
      continue

    if filter.rep:
      for reitem in filter.rep:
        match = reitem.match(record.msg)
        if match:
          ret = True
          break;

  #record.prn()
  return ret

