# Specify the pd and remote to report the replication progress
#
import os
import sys
import time
for path in os.listdir("/usr/local/nutanix/lib/py"):
        sys.path.insert(0, os.path.join("/usr/local/nutanix/lib/py", path))
from cerebro.interface.cerebro_interface_pb2 import *
from cerebro.master.cerebro_schedule_pb2 import CerebroOutOfBandScheduleProto
from cerebro.client.cerebro_interface_client import CerebroInterfaceTool, CerebroInterfaceError
from util.base.types import NutanixUuid
from util.misc.protobuf import pb2json
from util.nfs.nfs_client import NfsClient, NfsError
from cerebro.master.persistent_op_state_pb2 import PersistentOpBaseStateProto as op_state

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
  while True:
    pd=get_pd_metaops(name)
    for exec_op in pd.executing_meta_op_vec:
      if exec_op.base_persistent_state.opcode == op_state.Opcode.Value("kReplicateMetaOp"):
        for repl in exec_op.base_persistent_state.reference_actions.replication:
          if repl.remote_name == rem:
            tx_bytes = exec_op.progress.completed_work_units[1]
            speed = exec_op.progress.work_unit_completion_per_sec[1]
            time1 = time.time()
            diff = int(time1 - time0)
            print "%s\t%s\t%s\t%s\t%s" % (time.ctime(time1), repl.remote_name, diff, tx_bytes, speed)
            time0 = time1
      time.sleep(5)

if __name__ == "__main__":
    pdname = sys.argv[-2]
    rem = sys.argv[-1]
    #pd = get_pd_obj(pdname)
    get_repl_progress(pdname, rem)

