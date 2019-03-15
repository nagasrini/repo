from datetime import datetime
import os, sys
import mimetypes
import json
import gzip
import re

home = "/home/nutanix/"
for path in os.listdir("/usr/local/nutanix/lib/py"):
        sys.path.insert(0, os.path.join("/usr/local/nutanix/lib/py", path))
import util.base.log as log
from cerebro.interface.cerebro_interface_pb2 import *
#logentry_re = r'^(\w)(\d+\ \d+\:\d+\:\d+\.\d+) (\d+) (\S+):(\d+). (\w+.*)'
logentry_re = r'^(\w)(\d+\ \d+\:\d+\:\d+\.\d+) *(\d+) (\S+):(\d+). (.*)'
logentry_begin_re=r'^(\w)(\d+\ \d+\:\d+\:\d+\.\d+) (\d+) (\S+):(\d+)'
logentry_re_m = r'^(\w)(\d+\ \d+\:\d+\:\d+\.\d+) (\d+) (\S+):(\d+). (\w+.*) [{].*[}]'

logcollector_firstline_re = r'^\*.* = (\d+/\d+/\d+)-(\d+:\d+:\d+).* = (\d+/\d+/\d+)-(\d+:\d+:\d+)'
clusterlog_firstline_re=r'^.*: (\d+/\d+/\d+) (\d+:\d+:\d+)'

log.initialize("/home/nutanix/logfilter.INFO")
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
    return ("|%s|%10.10s| %s %s|%s|%s:%s %s") % (self.ip, self.service, self.type, self.date.strftime("%m%d %H:%M:%S.%f"), self.thrid, self.file, self.line, self.msg)

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
  def __init__(self, fstr=None, jsfile=None):
    self.service = None
    self.ip = None
    self.rep = []
    if fstr:
      self.parse_filters(fstr)
    if jsfile:
      fp=open(jsfile)
      fj = json.load(fp)
      self.load_filters_from_dict(fj)

  # ["1.2.3.4","cerebro","I","cerebro_master.cc","0527 04:04:32.465232","0527 04:15:59","^.*Top level.*$"']
  # j=json.loads('{"component": "cerebro", "level":"I", "start":"20190201 00:00:00.000000","end": "20190310 04:04:32.465232", "rep":["^.*Top level.*", ".*Replicate.*"]}')
  ip_re=re.compile(r"(\d+\.\d+\.\d+\.\d+)")
  ftype = {'I': "INFO", 'W': "WARNING", 'E': "ERROR", 'F': "FATAL"}
  def parse_filters(self, flist=[]):
    m = self.ip_re.match(flist[0])
    if m:
      self.ip = m.group()
    if len(flist[1]):
      self.service = flist[1]
    self.set_type(flist[2] if flist[2] else 'I')
    if len(flist[3]):
      self.file = flist[3]
    self.set_datemin(flist[4])
    self.set_datemax(flist[5])
    for reitem in flist[6:]:
      if len(reitem):
        self.set_re(reitem.strip("\"\'"))
      else:
        self.set_re("^.*$")

  def load_filters_from_dict(self, fdict):
    m = self.ip_re.match(fdict.get('ip_addr', ''))
    if m:
      self.ip = m.group()
    service = fdict.get('component')
    if service:
      self.service = service
    self.set_type(fdict.get('level', 'I'))
    self.set_datemin(fdict.get('start'))
    self.set_datemax(fdict.get('end'))
    for reitem in fdict.get('rep'):
      self.set_re(reitem)

  def set_type(self, t):
    self.type = self.ftype[t]

  def set_re(self, restr):
    print "re <%s>" % restr
    self.rep.append(re.compile(restr))

  def set_datemin(self, dstr):
    '''
    Sets the min date to start the analysis.
    Specify date in "%Y%m%d %H:%M:%S.%f" format, ex:20180526 02:31:01.586469"
    '''
    try:
      self.datemin = datetime.strptime(dstr, "%Y%m%d %H:%M:%S.%f")
    except:
      self.datemin = datetime.min

  def set_datemax(self, dstr):
    '''
    Sets the max date to end the analysis.
    Specify date in "%Y%m%d %H:%M:%S.%f" format, ex:20180526 02:31:01.586469"
    '''
    try:
      self.datemax = datetime.strptime(dstr, "%Y%m%d %H:%M:%S.%f")
    except:
      self.datmax = datetime.max

import multiprocessing as mp

def read_one_logfile(q, logfile, afilter):
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

  year = None
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

  if not year:
    print "error for %s" % logfile
    return

  service = os.path.basename(logfile).split(".")[0]
  lelist = []
  year_change_hint = False
  for line in file:
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
    le = logentry("0", service, le_year, entry)
    if filter and applyfilters(le, afilter):
      lelist.append(le)
  print "%s: going to write %d" % (logfile, len(lelist))
  q.put(lelist)
  print "%s: wrote" % logfile

#pathre=r"(?P<service>^.*)\.[nN][tT][nN][xX].*\.log\..*\.(?P<date>\d+)-(?P<time>\d+).(?P<ddd>\d+)"
pathre=r"^.*\.log.*\.(?P<date>\d+)-(?P<time>\d+)\.(?P<ddd>.*)"
def readlog(logdir=None, filters=None):
  if logdir == None:
    logdir = home + "data/logs/"

  loglist=[]
  plist=[]
  for f in os.listdir(logdir):
    logfile=os.path.join(logdir, f);
    if not os.path.isfile(logfile):
      continue

    # try to avoid logs not in any filter criteria
    found = False
    match = re.compile(pathre).match(f)
    for afilter in filters:
      if afilter:
        if afilter.service and afilter.service not in f:
          continue
        if afilter.type and afilter.type not in f:
          print "%s not in %s" % (afilter.type, f)
          continue
        if match:
          md = match.groupdict()
          start_date = datetime.strptime(md["date"] + md["time"], "%Y%m%d%H%M%S")
          if afilter and afilter.datemin and afilter.datemax < start_date:
            print "skipping %s < %s %s" % (afilter.datemax, start_date, f)
            continue
        found = True
    if not found:
      continue
    q = mp.Queue()
    log.INFO("Processing %s" % logfile)
    name = "Processing %s with filter" % logfile
    print name
    p = mp.Process(target=read_one_logfile, name=name, args=(q,logfile, filters))
    p.start()
    #loglist.extend(read_one_logfile(logfile, afilter))
    plist.append((p,q))

  while len(plist):
    for p, q in plist:
      if not q.empty():
        loglist.extend(q.get())
        print "read from %s len now %d" % (p.name, len(loglist))
      if p.is_alive():
        continue
      print "Task \"%s\" exited with %s" % (p.name, p.exitcode)
      p.join()
      plist.remove((p, q))
      del q
  loglist.sort()
  return loglist

def applyfilters(record, filters):
  '''
  Matches if one filter matches
  '''
  ret = False
  for filter in filters:
    if filter.ip != None and record.ip != filter.ip:
      continue
    if filter.datemax < record.date:
      continue
    if filter.datemin > record.date:
      continue
    if filter.service != record.service:
      continue

    if filter.rep:
      # matches if all re in this filter match
      log.INFO("b4 %s" % record)
      all_match = True
      for reitem in filter.rep:
        match = reitem.match(record.msg)
        if not match:
          all_match = False
          break
      ret = all_match
    else:
      ret = True
    if ret:
      log.INFO("%s" % record)
      break
  return ret
