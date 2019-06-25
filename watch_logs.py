import re
import sys
import subprocess
import datetime
sys.path.append('/home/nutanix/cluster/bin')
import env
import gflags
cmd_dropped = ["allssh", "'zgrep soft_max_cost ~/data/logs/stargate.*'"]
cmd_expired = ["allssh", "'zgrep \"inorder stretch oplog waiters\" ~/data/logs/stargate.*'"]
cmd = cmd_expired

gflags.DEFINE_integer("OCCR_CNT", 20, "total number of occurances of matching messages")
gflags.DEFINE_integer("DUR", 5, "duration in seconds in which we check for occurances")
FLAGS = gflags.FLAGS

outfile = "/home/nutanix/tmp/stargate_admctl_drop_watch_"

# Simple way to get the log enty year, though not the best
year=datetime.datetime.now().year

#/home/nutanix/data/logs/stargate.ntnx-18fm57500153-a-cvm.nutanix.log.INFO.20190618-053640.3103.gz:I0618 06:02:41.482791  4397 qos_weighted_fair_queue.h:465] Admctl wfq (total=260): Dropped 76 elements - queue: 1 current_cost: -86 (Write Ops Queue) initial_cost_quota: 86 soft_max_cost: 172 hard_max_cost: 194
r_dropped=re.compile(r'^(?P<lfp>\S+):(\w)(?P<date>\d+\ \d+\:\d+\:\d+\.\d+) *(?P<what>\d+) (?P<code_file>\S+):(?P<line>\d+). (?P<msg>.*)')

#Expired 3 inorder stretch oplog waiters
#W0618 04:28:05.875941  4401 vdisk_distributed_oplog.cc:4017] vdisk_id=51666832 inherited_episode_sequence=-1 ep_seq_base=33759 Expired 2 inorder stretch oplog waiters - next expected timestamp 980709
r_expired = re.compile(r'^(?P<lfp>\S+):(\w)(?P<date>\d+\ \d+\:\d+\:\d+\.\d+) *(?P<what>\d+) (?P<code_file>\S+):(?P<line>\d+). (?P<msg>.*)')

r = r_expired

s_proc = subprocess.Popen(["svmips"], stdout=subprocess.PIPE)
s_proc.wait()
svmips = s_proc.stdout.read().split()

class logentry:
  def __init__(self, ip, mdict):
    self.ip = ip
    self.lfp = mdict['lfp']
    self.codefile = mdict['code_file']
    self.msg = mdict['msg']
    self.date = datetime.datetime.strptime(str(year) + mdict['date'], "%Y%m%d %H:%M:%S.%f")
    self.line = mdict['line']

  def get_str(self):
    print ("|%s|%50s| %s|%s:%s %s") % (self.ip, self.date.strftime("%m%d %H:%M:%S.%f"), self.file, self.line, self.msg)

  def prn(self):
    print self.get_str()

  def __repr__(self):
    return ("|%s|%50s| %s|%s:%s %s") % (self.ip, self.lfp, self.date.strftime("%m%d %H:%M:%S.%f"), self.codefile, self.line, self.msg)

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

lentries = []
g_proc = []
outf = []
print "## Starting the check at %s" % datetime.datetime.now().strftime("%Y%m%d %H:%M:%S.%f")[4:]
for ip in svmips:
  # mimicking allssh
  file = outfile + ip.replace('.','_')
  f=open(file, "w+r")
  f.truncate()
  outf.append(f)
  print "## Doing ssh to %s" % ip
  cmd = ['ssh', '-q', '-o LogLevel=ERROR', '-o StrictHostKeyChecking=no', ip, "source /etc/profile; zgrep 'inorder stretch oplog waiters' ~/data/logs/stargate.*.INFO.*"]
  g_proc.append(subprocess.Popen(cmd, stdout=f))

for p in g_proc:
  p.wait()

for f in outf:
  f.flush()
  f.seek(0)
  print "## Processing the output"
  for line in f:
    line_match = r.match(line)
    if not line_match:
      continue
    match_dict = line_match.groupdict()
    ip = svmips[outf.index(f)]
    lentries.append(logentry(ip, match_dict))

lentries.sort(reverse=True)
found = False
if len(lentries) > FLAGS.OCCR_CNT:
  for i, le in enumerate(lentries):
    if i + FLAGS.OCCR_CNT - 1 >= len(lentries):
      continue
    diff = lentries[i].date - lentries[i+FLAGS.OCCR_CNT-1].date
    #print diff.seconds
    if diff.seconds < FLAGS.DUR:
      found = True
      print "%s:%s At %s, %d matching messages in %d seconds" % (le.ip, le.lfp, le.date.strftime("%Y%m%d %H:%M:%S.%f")[4:], FLAGS.OCCR_CNT, FLAGS.DUR)
      #print "Starting: %s" % lentries[i+19]
      #print "Ending  : %s" % lentries[i]
      #print ""

if not found:
  print "No matching message if found"
