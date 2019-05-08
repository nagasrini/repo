# Specify the pd and remote to report the replication progress
# Date 4 Mar 2019
#
import os
import sys
import time
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
import util.base.log as log


gflags.DEFINE_string("pd", "", "pd name")
gflags.DEFINE_string("remote", "", "remote name")
gflags.DEFINE_integer("interval", 5, "interval at which pool the progress")
FLAGS = gflags.FLAGS

RpcClient = CerebroInterfaceTool()

# list of all the pd names
def list_pds():
  try:
    arg = ListProtectionDomainsArg()
    ret=RpcClient.list_protection_domains(arg)
  except CerebroInterfaceError as e:
    print "Error getting list of pds" % e
  return ret.protection_domain_name

# returns PD object by the name specified
def get_pd_obj(name):
  if not name:
    print "Name of PD not speified"
    return None

  arg = QueryProtectionDomainArg()
  ret = QueryProtectionDomainRet()
  arg.protection_domain_name = name
  arg.fetch_executing_meta_ops = True
  arg.list_snapshot_handles.CopyFrom(QueryProtectionDomainArg.ListSnapshotHandles())
  arg.list_consistency_groups.CopyFrom(QueryProtectionDomainArg.ListConsistencyGroups())
  try:
    ret = RpcClient.query_protection_domain(arg)
  except CerebroInterfaceError as e:
    print "Error querying pds %s" % e
  return ret

def get_pd_metaops(name):
  if not name:
    print "Name of PD not speified"
    return None

  arg = QueryProtectionDomainArg()
  ret = QueryProtectionDomainRet()
  arg.protection_domain_name = name
  arg.fetch_executing_meta_ops = True
  try:
    ret = RpcClient.query_protection_domain(arg)
  except CerebroInterfaceError as e:
    print "Error querying pds %s" % e
  #print ret
  return ret

def get_repl_progress(name, rem):
  time0 = time.time()
  r0 = 0
  while True:
    pd=get_pd_metaops(name)
    for exec_op in pd.executing_meta_op_vec:
      if exec_op.base_persistent_state.opcode == op_state.Opcode.Value("kReplicateMetaOp"):
        for repl in exec_op.base_persistent_state.reference_actions.replication:
          if repl.remote_name == FLAGS.remote:
            #print exec_op.progress
            idx = None
            for i, n in enumerate(exec_op.progress.work_unit_description):
              if "tx" in n and "bytes" in n:
                idx = i
            tx_bytes = exec_op.progress.completed_work_units[idx]
            spd = exec_op.progress.work_unit_completion_per_sec[idx]
            time1 = time.time()
            diff = int(time1 - time0)
            diff_r = tx_bytes - r0
            r0 = tx_bytes
            cur_spd = 0 if diff == 0 else diff_r/diff
            r_info = "%15s %4d %18s (diff: %12s) bytes; " % (FLAGS.remote, diff, tx_bytes, diff_r)
            r_info += "overall: %8s (momentary: %8.4f) bytes/s" % (spd, cur_spd)
            log.INFO(r_info)
            print "%s: " % time.ctime(time1) + r_info
            time0 = time1
      time.sleep(FLAGS.interval)
    else:
      # No more metaops for this PD.
      break

if __name__ == "__main__":
    argv = FLAGS(sys.argv)
    log.initialize()
    get_repl_progress(FLAGS.pd, FLAGS.remote)
