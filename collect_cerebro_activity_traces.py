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

gflags.DEFINE_string("pd", "", "pd name")
gflags.DEFINE_string("remote", "", "remote name")
gflags.DEFINE_string("dump_dir", "/home/nutanix/tmp/", "directory to dump the pages in")

FLAGS = gflags.FLAGS

RpcClient = CerebroInterfaceTool()

# returns ongoing replication metaop_id for the PD to the remote
def get_repl_metaop_id(name, rem):
  arg = QueryProtectionDomainArg()
  ret = QueryProtectionDomainRet()
  arg.protection_domain_name = name
  arg.fetch_executing_meta_ops = True
  try:
    ret = RpcClient.query_protection_domain(arg)
  except CerebroInterfaceError as e:
    print "Error querying pds %s" % e
  #print ret

  repl_meta_opid = 0
  for exec_op in ret.executing_meta_op_vec:
    if exec_op.base_persistent_state.opcode == op_state.Opcode.Value("kReplicateMetaOp"):
      for repl in exec_op.base_persistent_state.reference_actions.replication:
        if repl.remote_name == rem:
          repl_meta_opid = exec_op.meta_opid
          break
  return repl_meta_opid

def get_cerebro_master():
  arg = GetMasterLocationArg()
  ret = GetMasterLocationRet()
  try:
    ret = RpcClient.GetMasterLocation(arg)
  except CerebroInterfaceError as e:
    print "Error querying cerebro master %s" % e
  return ret.handle

def dump_to_file(path, data_bytes):
  '''
  Truncate a file and dump the data
  '''
  l = len(data_bytes):
  flags = os.O_CREAT|os.O_RDWR
  fd = os.open(path, os.O_CREAT|os.O_RDWR|os.O_APPEND)
  res_l = os.write(fd, data_bytes)
  if not l == res_l:
    log.WARNING("couldn't dump to %s path completely" % path)
  os.close(fd)

def get_metaop_data(metaop_id, ip, pd, dump_dir, timeout=0):
  url="http://%s:2020/?pd=%s&op=%d" % (ip,pd,metaop_id)
  params={'timeout': timeout}
  ret = requests.get(url, params=params)
  if not r.ok:
    return None, None
  if dump_dir:
    file_path = dump_dir + "/%d_repl_metaop.html" % metaop_id
    dump_to_file(file_path, r.text, new=True)
    return ret, file_path
  else:
    return ret, None

if __name__ == "__main__":
  pd = FLAGS.pd
  remote = FLAGS.remote
  argv = FLAGS(sys.argv)
  if FLAGS.dump_dir:
    if not os.path.lexists(dump_dir):
      os.mkdir(dump_dir)
    else:
      if not os.path.isdir(dump_dir):
        print "ERROR: %s is not a directory, exiting"
        sys.exit(1)
  ip = get_cerebro_master()
  mid = get_repl_metaop_id(pd, remote)
  if not ip or not mid:
    print "ERROR: ip or metaopid"
    sys.exit(3)
  repl = get_metaop_data(mid, ip, pd, FLAGS.dump_dir)
