import sys
sys.path.append('/home/nutanix/cluster/bin')
import env
import re
from zeus.zookeeper_session import ZookeeperSession
from datetime import datetime
from time import sleep

date_fmt = "%d%m %Y %H:%M:%S.%f"

zks = ZookeeperSession()

base_path = "/appliance/logical/leaders/cerebro_master"

_exit = False

def z_cb(zk, path, event):82C                                                                                                                                                                                                                 23,3          All
  #print "callback called for %s %s %s" % (zk, path, event)
  path = base_path + "/" + zks.list("/appliance/logical/leaders/cerebro_master")[0]
  date = datetime.now()
  sleep(1)
  print zks.get(path,z_cb)
  m = re.match("Handle: (?P<ip>.*):.*\+(?P<inc>\w+) ", zks.get(path,z_cb))
  if m:
    matchdict = re.match("Handle: (?P<ip>.*):.*\+(?P<inc>\w+) ", s).groupdict()
  print "%s: Cerebro Master changed to %s" % (date.strftime(date_fmt), matchdict.get('ip', '??'))

if __name__ == "__main__":
  path = base_path + "/" + zks.list("/appliance/logical/leaders/cerebro_master")[0]
  s=zks.get(path,z_cb)
  while _exit == False:
    continue
