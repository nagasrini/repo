# version.0.1
import os, sys
import env
import gflags
import util.base.log as log
import util.base.command as command

from cerebro.interface.cerebro_interface_pb2 import *
from cerebro.client.cerebro_interface_client import CerebroInterfaceTool, CerebroInterfaceError
from pithos.pithos_pb2 import VDiskConfig
from pithos.client.pithos_client import PithosClient
from zeus.configuration import Configuration
from tools.vdisk_usage_printer.vdisk_usage_pb2 import VdiskUsageProto
from cassandra.cassandra_client.client import CassandraClient

gflags.DEFINE_boolean("remote_check", True, "Whether to do remote vdisk check?")
gflags.DEFINE_boolean("skip_usage", True, "Whether to skip vdisk usage?")

FLAGS = gflags.FLAGS

RpcClient = CerebroInterfaceTool()

remote_site_map = {}

def get_remote_site_map():
  zcp = Configuration().initialize().config_proto()
  for rs in zcp.remote_site_list:
    remote_site_map[rs.cluster_id] = rs.remote_cerebro_ip_list


def pclient(host=None):
  cassandra_client = CassandraClient(host, "9161")
  pithos_client = PithosClient(cassandra_client=cassandra_client)
  pithos_client.initialize()
  return pithos_client

def get_vdisk_config(pithos_client, vdisk_id):
  '''
  Get the vdisk_config for the specified vdisk_id from the specified pithos client
  :param pithos_client:
  :param vdisk_id:
  :return:
  '''
  entries = pithos_client.create_iterator('vdisk_id', skip_values=False, consistent=False)
  for entry in entries:
    vdisk_config = pithos_client.entry_to_vdisk_config(entry)
    if vdisk_config and vdisk_config.vdisk_id == vdisk_id:
      return vdisk_config

output_file = "/tmp/vup.proto"

def vdisk_usage_printer(vdisk_id):
  cmd = "/usr/local/nutanix/bin/vdisk_usage_printer --vdisk_id %d" % vdisk_id
  cmd += " -output_type proto -output_file_name %s" % output_file
  rv, out, err = command.timed_command(cmd, 60)
  if rv:
    print out
    log.ERROR("Error running the command %s\n%s" %(cmd, out))
    return None
  vup_proto = VdiskUsageProto()
  vup_proto.ParseFromString(open(output_file).read())
  os.unlink(output_file)
  return vup_proto

def get_pd_objs():
  """
  Gets a dictionary of all pd_names to their pd object
  """
  ret = None
  try:
    arg = ListProtectionDomainsArg()
    ret = RpcClient.list_protection_domains(arg)
  except CerebroInterfaceError as e:
    print "Error getting list of pds" % e
  if not ret:
    log.ERRO("Couldn't get the PD list")
    return

  pd_name_list = ret.protection_domain_name
  pd_obj_dic = {}
  for pd_name in pd_name_list:
    arg = QueryProtectionDomainArg()
    ret = QueryProtectionDomainRet()
    arg.protection_domain_name = pd_name
    arg.list_snapshot_handles.CopyFrom(QueryProtectionDomainArg.ListSnapshotHandles())
    try:
      ret = RpcClient.query_protection_domain(arg)
    except CerebroInterfaceError as e:
      print "Error querying pd <%s> %s" % (pd_name, e)
      continue
    if ret.stretch_params.stretch_params_id.entity_id:
      print "skipping stretch PD %s" % pd_name
      continue
    pd_obj_dic[pd_name] = ret
  return pd_obj_dic

def snapshot_vdisks():
  """
  Gets dictionary of all vdisks to list of entity vdisk from their snapshots
  """
  pd_snap_vdisk_dic = {}
  pd_obj_dics = get_pd_objs()
  for name, obj in pd_obj_dics.iteritems():
    snap_vdisk_list = []
    for snap_cblock in obj.snapshot_control_block_vec:
      snap_vdisk_list.extend(snap_cblock.entity_vdisk_id_vec)
    pd_snap_vdisk_dic[name] = snap_vdisk_list
  return pd_snap_vdisk_dic

from tools.vdisk_usage_printer.vdisk_usage_pb2 import VdiskUsageProto

def check_vdisk_usage(vdisk_list):
  """
  If the originating vdisk not present on source side, gets the vdisk usage info
  :param vdisk_list:
  :return VdiskUsageProto:
  """
  local_pithos_client = pclient()

  filtered_vdisk_list = []
  if not FLAGS.remote_check:
    filtered_vdisk_list = vdisk_list
  else:
    for vdisk_id in vdisk_list:
      remote_pclient = None
      vdisk_config = get_vdisk_config(local_pithos_client, vdisk_id)
      remote_ip_list = []

      if vdisk_config and vdisk_config.originating_cluster_id:
        remote_ip_list = remote_site_map[vdisk_config.originating_cluster_id]

      for remote_ip in remote_ip_list:
        remote_pclient = pclient(remote_ip)
        if remote_pclient:
          break
      else:
        log.ERROR("Remote site not found for cluster: %d" % vdisk_config.originating_cluster_id)
        return None

      if vdisk_config.originating_vdisk_id:
        is_present = " "
        orig_vdisk_config = get_vdisk_config(remote_pclient, vdisk_config.originating_vdisk_id)
        if not orig_vdisk_config:
          is_present = "not "
          filtered_vdisk_list.append(vdisk_id)
        log.INFO("local vdisk %d; originating vdisk %d %spresent" % (vdisk_id, vdisk_config.originating_vdisk_id, is_present))
        print "local %d originating %d %spresent" % (vdisk_id, vdisk_config.originating_vdisk_id, is_present)

  log.DEBUG("filtered vdisk list %s" % filtered_vdisk_list)
  print "filtered vdisk list %s" % filtered_vdisk_list
  if FLAGS.skip_usage:
    return None, 0, 0
  vup_proto_list = []
  # get vdisk usage for the filtered vdisk_ids
  print "%10s%22s%22s" % ("Vdisk ID", "Transformed Usage", "Internal Garbage")
  total_used = 0
  total_garbage = 0
  for vdisk_id in filtered_vdisk_list:
    vup_proto = vdisk_usage_printer(vdisk_id)
    if vup_proto:
      print "%10d%22d%22d" % (vdisk_id, vup_proto.total_transformed_size_bytes, vup_proto.total_internal_garbage)
      total_used += vup_proto.total_transformed_size_bytes
      total_garbage += vup_proto.total_internal_garbage
      vup_proto_list.append(vup_proto)
  return vup_proto_list, total_used, total_garbage

if __name__ == "__main__":
  log.initialize()
  get_remote_site_map()
  pd_snap_vdisk_list = snapshot_vdisks()
  for pd, vdisk_list in pd_snap_vdisk_list.iteritems():
    print "##  checking for PD %s" % pd
    vup_proto_list, usage, garbage = check_vdisk_usage(vdisk_list)
    if vup_proto_list:
      print "    Total Usage:   %d" % usage
      print "    Total Garbage: %d" % garbage
