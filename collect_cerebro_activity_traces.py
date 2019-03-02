#
# collect cerebro activity traces around a replication
#
# Input: PD name, Remote Name
# Get the progressing replication metaop for the PD to the remote
# Get CerebroMasterReplicateMetaOp traces.
# Get the slaves handling work_ids.
# Get 'http:0:2020/?pd=27Feb_test_pd&op=74556'
import os
import sys
import time
import requests
for path in os.listdir("/usr/local/nutanix/lib/py"):
        sys.path.insert(0, os.path.join("/usr/local/nutanix/lib/py", path))
import gflags
from cerebro.interface.cerebro_interface_pb2 import *
from cerebro.master.cerebro_schedule_pb2 import CerebroOutOfBandScheduleProto
from cerebro.client.cerebro_interface_client import CerebroInterfaceTool, CerebroInterfaceError
from util.base.types import NutanixUuid
from util.misc.protobuf import pb2json
from util.nfs.nfs_client import NfsClient, NfsError
from cerebro.master.persistent_op_state_pb2 import PersistentOpBaseStateProto as op_state
import util.base.command as command
import util.base.log as log

gflags.DEFINE_string("pd", "", "pd name")
gflags.DEFINE_string("remote", "", "remote name")
gflags.DEFINE_string("dump_dir", "/home/nutanix/tmp/", "directory to dump the pages in")

FLAGS = gflags.FLAGS

RpcClient = CerebroInterfaceTool()

def get_cerebro_master():
  '''
  gets cerebro master handle
  '''
  arg = GetMasterLocationArg()
  ret = GetMasterLocationRet()
  try:
    ret = RpcClient.GetMasterLocation(arg)
  except CerebroInterfaceError as e:
    print "Error querying cerebro master %s" % e
  return ret.master_handle

class Replication():
  def __init__(pd, remote, dump_dir):
    self.pd = pd
    self.remote = remote
    self.dump_dir = dump_dir
    self.metaop_id = 0
    self.metaop_page = ""
    self.replicate_op_page_vec = []
    self.get_repl_metaop_id(pd, remote)

  def get_repl_metaop_id(name, rem):
    '''
    returns ongoing replication metaop_id for the PD to the remote
    '''
    print "pd=%s remtoe=%s" % (name, rem)
    arg = QueryProtectionDomainArg()
    ret = QueryProtectionDomainRet()
    arg.protection_domain_name = name
    arg.fetch_executing_meta_ops = True
    try:
      ret = RpcClient.query_protection_domain(arg)
    except CerebroInterfaceError as e:
      print "Error querying pds %s" % e
    #print ret
    for exec_op in ret.executing_meta_op_vec:
      if exec_op.base_persistent_state.opcode == op_state.Opcode.Value("kReplicateMetaOp"):
        for repl in exec_op.base_persistent_state.reference_actions.replication:
          if repl.remote_name == rem:
            self.meta_opid = exec_op.meta_opid
            break

  def dump_to_file(self, path, data_bytes, convert=True):
    '''
    Truncate a file and dump the data
    '''
    l = len(data_bytes)
    flags = os.O_CREAT|os.O_RDWR|os.O_TRUNC
    fd = os.open(path, flags)
    res_l = os.write(fd, data_bytes)
    if not res_l == len(data_bytes):
      log.WARNING("couldn't dump to %s path completely" % path)
    os.close(fd)
    txt_path = path.split('.')[0]+".txt"
    cmd = "/usr/bin/links -dump %s" % path
    rv, txt_data_bytes, _ = command.timed_command(cmd, 60)
    if rv:
      return
    fd = os.open(txt_path, flags)
    res_l = os.write(fd, txt_data_bytes)
    if not res_l = len(txt_data_bytes):
      log.WARNING("couldn't dump to %s path completely" % txt_path)
    os.close(fd)

  def get_metaop_data(self):
    url="http://%s/?pd=%s&op=%d" % (ip,pd,metaop_id)
    ip = get_cerebro_master()
    params={'timeout': self.timeout}
    ret = requests.get(url, params=params)
    if not ret.ok:
      return None, None
    self.metaop_page = dump_dir + "/%d_replicate_metaop" % metaop_id
    file_path = self.metaop_page + ".html"
    dump_to_file(file_path, ret.text, convert=True)
    return ret

  def get_slave_ops(self):
    if not os.lexists(self.metaop_page + ".txt"):
      print "ERROR: text metaop page doesn't exist"
      return
    filter = " work_id|slave_incarnation_id| file_path"
    cmd "/usr/bin/egrep -e '%s' %s.txt" % (filter,self.metaop_page)
    rv, data, err = command.timed_command(cmd, 60)
    if rv:
      print err
      return

if __name__ == "__main__":
  argv = FLAGS(sys.argv)
  pd = FLAGS.pd
  remote = FLAGS.remote
  log.initialize()
  if FLAGS.dump_dir:
    if not os.path.lexists(FLAGS.dump_dir):
      os.mkdir(FLAGS.dump_dir)
    else:
      if not os.path.isdir(FLAGS.dump_dir):
        print "ERROR: %s is not a directory, exiting"
        sys.exit(1)
  if not ip:
    print "ERROR: ip"
    sys.exit(3)
  rep = Replication(pd, remote, dump_dir)
  repl = rep.get_metaop_data()
