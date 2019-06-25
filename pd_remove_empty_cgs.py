import os
import sys
sys.path.append("/home/nutanix/serviceability/bin")
import env
from cerebro.interface.cerebro_interface_pb2 import *
from cerebro.master.cerebro_schedule_pb2 import CerebroOutOfBandScheduleProto
from cerebro.client.cerebro_interface_client import CerebroInterfaceTool, CerebroInterfaceError
import gflags

gflags.DEFINE_boolean("dryrun", True, "Is CG Remove a Dryrun?")
gflags.DEFINE_string("pd_name", "", "PD name")
FLAGS = gflags.FLAGS

RpcClient = CerebroInterfaceTool()

def remove_cg(pd_name, cg_name):
  arg = RemoveConsistencyGroupArg()
  arg.protection_domain_name = pd_name
  arg.consistency_group_name = cg_name
  try:
    ret = RpcClient.remove_consistency_group(arg)
  except CerebroInterfaceError as e:
    print "Error removing CG %s from PD %s: %s" % (cg_name, pd_name, e)
    return None
  return ret

def remove_empty_cgs(pd_name):
  arg = QueryProtectionDomainArg()
  ret = QueryProtectionDomainRet()
  arg.protection_domain_name = pd_name
  arg.list_consistency_groups.CopyFrom(QueryProtectionDomainArg.ListConsistencyGroups())
  while True:
    try:
      pdo = RpcClient.query_protection_domain(arg)
    except CerebroInterfaceError as e:
      print "Error querying PD: %s" % e
      return None
    if not pdo:
      print "Getting PD object failed"
      return

    for cg in pdo.consistency_group_vec:
      if len(cg.entity):
        continue
      if FLAGS.dryrun:
        print "Would remove cg %s" % cg.name
        continue
      ret = remove_cg(pd_name, cg.name)
      if not ret:
        print "Couldn't remove CG"
      else:
        print "Removed %s" % cg.name
    if len(pdo.consistency_group_vec)
      arg.list_consistency_groups.last_consistency_group_name = pdo.consistency_group_vec[-1].name

if __name__ == "__main__":
  gflags.FLAGS(sys.argv)
  remove_empty_cgs(FLAGS.pd_name)
