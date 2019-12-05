# v.0.3
# Use arithmos where possible
# Use curator when exclusive usage is available
# find usage on active side as well.
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

from stats.arithmos.interface.arithmos_interface import *
from stats.arithmos.interface.arithmos_interface_pb2 import *

gflags.DEFINE_boolean("remote_check", True, "Whether to do remote vdisk check?")
gflags.DEFINE_boolean("skip_usage", True, "Whether to skip vdisk usage?")

FLAGS = gflags.FLAGS

CerebroRpcClient = CerebroInterfaceTool()
ArithmosRpcClient = ArithmosInterface()
gml_arg = GetMasterLocationArg()
try:
  error,_,ret = ArithmosRpcClient.get_master_location(gml_arg)
except Exception as e:
  log.ERROR("Arithmos get master location %s, %s" % (error, e))
if ret:
  ArithmosRpcClient = ArithmosInterface(ret.master_handle.split(":")[0])

remote_site_map = {}

zcp = Configuration().initialize().config_proto()
local_cluster_id = zcp.cluster_id

def get_remote_site_map():
  global zcp
  if not zcp:
    zcp = Configuration().initialize().config_proto()
  for rs in zcp.remote_site_list:
    remote_site_map[rs.cluster_id] = rs.remote_cerebro_ip_list

def pclient(host=None):
  cassandra_client = CassandraClient(host, "9161")
  pithos_client = PithosClient(cassandra_client=cassandra_client)
  pithos_client.initialize()
  return pithos_client

LocalPithosClient = pclient()
output_file = "/tmp/vup.proto"
remote_pclient = {} # remote pithos client
local_pithos_cache ={}

try:
  import curator.client.curator_interface_client as curator_client
  from curator.client.interface.curator_client_interface_pb2 import *
except:
  import cdp.client.curator.client.curator_interface_client as curator_client
  from cdp.client.curator.client.interface.curator_client_interface_pb2 import *

CuratorRpcClient = curator_client.CuratorInterfaceClient()

try:
  get_master_location_arg = curator_client.GetMasterLocationArg()
  get_master_location_ret = CuratorRpcClient.GetMasterLocation(get_master_location_arg)
  (curator_ip, curator_port) = str(get_master_location_ret.master_handle).split(":")
except curator_client.CuratorInterfaceError as error:
  log.ERROR("RPC failed. Couldn't acquire the curator master information. Error: %s" % error)

CuratorRpcClient = curator_client.CuratorInterfaceClient(curator_ip, curator_port)

def get_vdisk_config(pithos_client, vdisk_id):
  '''
  Get the cached vdisk_config for the specified vdisk_id from the specified pithos client
  :param pithos_client:
  :param vdisk_id:
  :return vdisk_config:
  '''
  global local_pithos_cache
  config_dict = local_pithos_cache.get(pithos_client, {})
  if not len(config_dict):
    entries = pithos_client.create_iterator('vdisk_id', skip_values=False, consistent=False)
    for entry in entries:
      vdisk_config = pithos_client.entry_to_vdisk_config(entry)
      if vdisk_config:
        config_dict[vdisk_config.vdisk_id] = vdisk_config
    local_pithos_cache[pithos_client] = config_dict

  if vdisk_id in config_dict.keys():
    return config_dict[vdisk_id]

# get list of vdisks on passive side for which corresponding active side vdisk not present.
def get_local_only_vdisks(vdisk_list):
  """
  Gets the vdisk id list for all vdisks which is missing the originating vdisk not present on source side
  :param vdisk_list:
  :return VdiskUsageProto:
  """
  local_pithos_client = pclient()

  filtered_vdisk_list = []
  if not FLAGS.remote_check:
    return vdisk_list

  remote_pclient = {}

  for vdisk_id in vdisk_list:
    r_pclient = None
    vdisk_config = get_vdisk_config(local_pithos_client, vdisk_id)
    if not vdisk_config:
      log.ERROR("couldn't fetch local vdisk config for %d" % vdisk_id)
      continue
    if vdisk_config and vdisk_config.originating_cluster_id:
      remote_ip_list = remote_site_map[vdisk_config.originating_cluster_id]
      for remote_ip in remote_ip_list:
        r_pclient = remote_pclient.get(vdisk_config.originating_cluster_id)
        if not r_pclient:
          r_pclient = pclient(remote_ip)
          remote_pclient[remote_ip] = r_pclient
          if r_pclient:
            break
      else:
        log.ERROR("Remote site not found for cluster: %d" % vdisk_config.originating_cluster_id)
        return None

    if vdisk_config.originating_vdisk_id:
      is_present = " "
      orig_vdisk_config = get_vdisk_config(r_pclient, vdisk_config.originating_vdisk_id)
      if not orig_vdisk_config:
        is_present = "not "
        filtered_vdisk_list.append(vdisk_id)
      log.INFO("local vdisk %d; originating vdisk %d %spresent" % (vdisk_id, vdisk_config.originating_vdisk_id, is_present))
      print "local %d originating %d %spresent" % (vdisk_id, vdisk_config.originating_vdisk_id, is_present)
  return filtered_vdisk_list

class Vdisk(object):
  def __init__(self, name, id, excl_bytes=-1):
    self.name = name
    self.id = id
    self.vdisk_size = 0
    self.vdisk_exclusive_bytes = excl_bytes # unless curator calculates excl. size
    '''
    qvu_arg = curator_client.QueryVDiskUsageArg()
    qvu_arg.vdisk_id_list.append(id)
    qvu_ret = CuratorRpcClient.QueryVDiskUsage(qvu_arg)
    if not qvu_ret:
      log.ERROR("Couldn't get exclusive usage for the vdisk %d" % id)
      return
    if qvu_ret.exclusive_usage_bytes[0] == -1:
      log.WARNING("Exclusive usage for vdisk %d not available" % id)
      return
    self.vdisk_exclusive_bytes = qvu_ret.exclusive_usage_bytes[0]

    The arithmos stat for vdisk is only for leaf vdisks.
    # get vdisk stats from Arithmos
    if not ArithmosRpcClient:
      ArithmosRpcClient = ArithmosInterface()
    self.arithmos = ArithmosRpcClient
    query_proto = MasterGetStatsArg()
    query_request = query_proto.request_list.add()
    query_request.entity_type = ArithmosEntityProto.kVdisk
    query_request.entity_id = name
    err, _, ret = self.arithmos.master_get_stats(query_proto)
    response = ret.response_list[0]
    '''

class Snapshot(object):
  def __init__(self, scb):
    global ArithmosRpcClient, LocalPithosClient
    self.handle = "%s:%s:%s" % (scb.handle.cluster_id, scb.handle.cluster_incarnation_id, scb.handle.entity_id)
    self.vdisk_list = []
    self.scb = scb
    self.usage = 0
    self.exclusive_usage = 0
    log.INFO("Processing snapshot %s" % self.handle)

    if not ArithmosRpcClient:
      ArithmosRpcClient = ArithmosInterface()
    self.arithmos = ArithmosRpcClient
    query_proto = MasterGetStatsArg()
    query_request = query_proto.request_list.add()
    query_request.entity_type = ArithmosEntityProto.kSnapshot
    query_request.entity_id = self.handle.replace(":", "-")
    err,_,ret = self.arithmos.master_get_stats(query_proto)
    if not ret:
      log.ERROR("Arithmos error for snapshot %s" % self.handle)
      return
    response = ret.response_list[0]
    if response and not response.error:
      for stat in response.entity.snapshot[0].stats.generic_stat_list:
        self.exclusive_usage = None
        if stat.stat_name == "space_used_bytes":
          self.exclusive_usage = stat.stat_value
      self.usage = response.entity.snapshot[0].user_written_bytes

    if not LocalPithosClient:
      LocalPithosClient = pclient()

    self.local_only_vdisk_list = get_local_only_vdisks(scb.entity_vdisk_id_vec)

    if not len(self.local_only_vdisk_list):
      return

    qvu_arg = curator_client.QueryVDiskUsageArg()
    for vdisk_id in self.local_only_vdisk_list:
      qvu_arg.vdisk_id_list.append(vdisk_id)

      '''
      vdisk_config = get_vdisk_config(LocalPithosClient, vdisk_id)
      if not vdisk_config:
        log.ERROR("Snapshot: couldn't fetch vdisk_id config for %d" % vdisk_id)
        continue
      self.vdisk_list.append(Vdisk(vdisk_config.vdisk_name, vdisk_config.vdisk_id))
      '''
    log.INFO("Querying vdisks' usage for %s" %  self.local_only_vdisk_list)
    try:
      qvu_ret = CuratorRpcClient.QueryVDiskUsage(qvu_arg)
    except Exception as e:
      log.ERROR("QueryVDiskUsage failed for %s failed with: %s" % (qvu_arg.vdisk_id_list, e))
    for idx, id in enumerate(qvu_arg.vdisk_id_list):
      if qvu_ret:
        self.vdisk_list.append(Vdisk("", id, qvu_ret.exclusive_usage_bytes[idx]))

class PD(object):
  def __init__(self, name=None):
    global CerebroRpcClient
    self.name = name
    self.snap_list = []

    if not CerebroRpcClient:
      CerebroRpcClient = CerebroInterfaceTool()
    self.cerebro = CerebroRpcClient
    arg = QueryProtectionDomainArg()
    arg.protection_domain_name = name
    arg.list_snapshot_handles.CopyFrom(QueryProtectionDomainArg.ListSnapshotHandles())
    try:
      ret = self.cerebro.query_protection_domain(arg)
    except CerebroInterfaceError as e:
      print "Error querying pd <%s> %s" % (pd_name, e)
      return
    if ret.stretch_params.stretch_params_id.entity_id:
      print "skipping stretch PD %s" % name
      return
    self.obj = ret
    for scb in self.obj.snapshot_control_block_vec:
      snap = Snapshot(scb)
      self.snap_list.append(snap)

def visit_all_pds():
  """
  Gets list of all PD objects
  """
  pd_snap_vdisk_dic = {}
  ret = None
  try:
    arg = ListProtectionDomainsArg()
    ret = CerebroRpcClient.list_protection_domains(arg)
  except CerebroInterfaceError as e:
    print "Error getting list of pds" % e
  if not ret:
    log.ERROR("Couldn't get the PD list")
    return
  pd_name_list = ret.protection_domain_name
  pd_list = []
  for pd_name in pd_name_list:
    log.INFO("Reading PD %s" % pd_name)
    pd_list.append(PD(pd_name))
  return pd_list

from tools.vdisk_usage_printer.vdisk_usage_pb2 import VdiskUsageProto

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

# get vdisk usage for all the vdisks in the list
def get_vdisk_usage(vdisk_list):
  log.DEBUG("vdisk list %s" % vdisk_list)
  print "vdisk list %s" % vdisk_list
  if FLAGS.skip_usage:
    return None, 0, 0
  vup_proto_list = []
  # get vdisk usage for the filtered vdisk_ids
  print "%10s%22s%22s" % ("Vdisk ID", "Transformed Usage", "Internal Garbage")
  total_used = 0
  total_garbage = 0
  for vdisk_id in vdisk_list:
    vup_proto = vdisk_usage_printer(vdisk_id)
    if vup_proto:
      print "%10d%22d%22d" % (vdisk_id, vup_proto.total_transformed_size_bytes, vup_proto.total_internal_garbage)
      total_used += vup_proto.total_transformed_size_bytes
      total_garbage += vup_proto.total_internal_garbage
      vup_proto_list.append(vup_proto)
  return vup_proto_list, total_used, total_garbage

if __name__ == "__main__":
  log.initialize()
  argv = FLAGS(sys.argv)
  get_remote_site_map()
  pd_obj_list = visit_all_pds()
  for pd in pd_obj_list:
    if not len(pd.snap_list):
      continue
    print "\n%20s%40s%15s%15s%15s%15s" % ("PD", "Snap Handle", "Snap Usage", "Snap E. Usage", "Vdisk id ", "Vdisk E. Usage")
    for snap in pd.snap_list:
      print "%20s%40s%15s%15s" % ( pd.name, snap.handle, snap.usage, snap.exclusive_usage)
      for vdisk in snap.vdisk_list:
        print "%20s%40s%15s%15s%15s%15s" % ( 20*" ", 40*" ", 15*" ", 15*" ", str(vdisk.id), vdisk.vdisk_exclusive_bytes)
'''
    vup_proto_list, usage, garbage = check_vdisk_usage(vdisk_list)
    if vup_proto_list:
      print "    Total Usage:   %d" % usage
      print "    Total Garbage: %d" % garbage
'''