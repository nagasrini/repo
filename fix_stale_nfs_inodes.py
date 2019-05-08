#
# Copyright (c) 2014 Nutanix Inc. All rights reserved.
#
# Author: rnallan@nutanix.com
#
# Simple helper to access the metadata from medusa
# It can lookup:
#  - VBlock map
#  - ExtentGroupId Map
#  - NFS Map
# Can be extended to do any map
# Do a scan
#

__all__ = [ "MedusaErrorStatus", "MedusaHelper" ]

import cStringIO
import time
import re

import sys
sys.path.append('/home/nutanix/cluster/bin')
import env
import gflags
import subprocess
import medusa.medusa_printer_pb2 as medusa_printer_pb2
import util.base.command as command

from medusa import medusa_pb2
from medusa.medusa_pb2 import MedusaNFSMapEntryProto
from medusa.medusa_pb2 import MedusaExtentGroupIdMapEntryProto
try:
  # 5.9 code has kVDiskBlockMap in different location
  from medusa.medusa_pb2 import MedusaVDiskBlockMapEntryProto
except:
  from medusa.vdisk_block_map_pb2 import MedusaVDiskBlockMapEntryProto
try:
  ## 5.5 code has client here.
  from cassandra.cassandra_client import cassandra_pb2
  from cassandra.cassandra_client.client import CassandraClient, CassandraClientError
except:
  from util.cassandra import cassandra_pb2
  from util.cassandra.client import CassandraClient, CassandraClientError
from alerts.manager.alert_pb2 import AlertProto

import util.base.log as log

gflags.DEFINE_integer("nfs_map_lookup_batch_size",
                      64,
                      "Maximum number of Cassandra rows to read in one RPC "
                      "when doing lookups on the NFS map in Cassandra.")

FLAGS = gflags.FLAGS

#==============================================================================
# Medusa ErrorStatus constants.
class MedusaErrorStatus(object):
  kNoError = 0
  kCASFailure = 1
  kTimeoutError = 2
  kBackendUnavailable = 3
  kVDiskNonExistent = 4
  kRetry = 5

  medusa_error_to_string = {
                            kNoError: '',
                            kCASFailure: 'CAS Failure',
                            kTimeoutError: 'Timeout',
                            kBackendUnavailable: 'Backend Unavailable',
                            kVDiskNonExistent: 'VDisk does not exist',
                            kRetry: 'Retry'}

#==============================================================================
class MedusaHelper(object):
  ''' Class that provides a subset of the interface found in medusa/medusa.h. '''

  # Mapping from a Cassandra error code to the corresponding Medusa error code
  # for Cassandra error codes we expect to potentially see.
  CASSANDRA_MEDUSA_ERROR_MAP = {
    cassandra_pb2.CassandraError.kNoError :
      MedusaErrorStatus.kNoError,
    cassandra_pb2.CassandraError.kCasFailure :
      MedusaErrorStatus.kCASFailure,
    cassandra_pb2.CassandraError.kTimeout :
      MedusaErrorStatus.kTimeoutError,
    cassandra_pb2.CassandraError.kEpochMismatch :
      MedusaErrorStatus.kCASFailure,
    cassandra_pb2.CassandraError.kUnavailable :
      MedusaErrorStatus.kBackendUnavailable,
    cassandra_pb2.CassandraError.kInvalidRequest :
      MedusaErrorStatus.kBackendUnavailable,
    cassandra_pb2.CassandraError.kRetry :
      MedusaErrorStatus.kRetry }

  # Medusa Cassandra constants.
  #TODO: Move these as class variables
  # nfsmap
  kNFSMap = "medusa_nfsmap"
  kNFSMapCF = "nfsmap"
  kNFSMapColumn = "0"

  #vdiskblock map
  kVDiskBlockMap = "medusa_vdiskblockmap"
  kVDiskBlockMapCF = "vdiskblockmap"
  ## The column is based on vblock_num
  #kVDiskBlockMapColumn = "1"
  kVDiskColumnNameWidth = 16

  # egidmap
  kEGIDMap = "medusa_extentgroupidmap"
  kEGIDMapCF = "extentgroupidmap"
  kEGIDMapColumn = "1"

  # alerts
  kAlertsMap = "alerts_keyspace"
  kAlertsCF = "alerts_cf"

  keyspace_to_proto_map = {
                        kNFSMap:MedusaNFSMapEntryProto,
                        kVDiskBlockMap:MedusaVDiskBlockMapEntryProto,
                        kEGIDMap:MedusaExtentGroupIdMapEntryProto,
                        kAlertsMap:AlertProto}


  # Cassandra token hash constants.
  kNumHashChars = 62
  kHashChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

  replica_status_to_string = {
    medusa_pb2.MedusaExtentGroupIdMapEntryProto.Replica.kHealthy:'Healthy',
    medusa_pb2.MedusaExtentGroupIdMapEntryProto.Replica.kCorrupt:'Corrupt',
    medusa_pb2.MedusaExtentGroupIdMapEntryProto.Replica.kMissing:'Missing'}

  def __init__(self, host=None, port=None):
    # Cassandra client
    self.__cassandra_client = CassandraClient(host, port)
    log.DEBUG('Initializing cassandra rpc %s:%s' %
                  (self.__cassandra_client._host,
                   self.__cassandra_client._port))

  def scan(self, keyspace, cf, column_name=None, start_token="", end_token=""):
    '''
     Scan 'cf' Column Family in 'keyspace' and return all objects.
     It parses the return objects for the corresponding map using the mapping
     in keyspace_to_proto_map. To add support for newer map, add the values to
     this map.
    '''
    start_token = ""
    end_token = ""
    prev_start_token = start_token
    error = MedusaErrorStatus.medusa_error_to_string[MedusaErrorStatus.kNoError]

    values = {}
    # Prepare args for MultigetSlice().
    arg = cassandra_pb2.CassandraGetRangeSlicesArg()
    arg.keyspace = keyspace
    arg.column_parent.column_family = cf
    arg.predicate.slice_range.count = 10 # medusa_printer sets this to kint32max, 2^32?
    arg.predicate.slice_range.start = ""
    arg.predicate.slice_range.finish = ""
    arg.consistency_level = cassandra_pb2.CassandraConsistencyLevel.kQuorum
    arg.range.end_token = end_token
    log.INFO("Scanning from %s - %s" % (start_token, end_token))

    # If start_token and end_token are passed, and start_token > end_token,
    # i.e the case of wrap around, we need to break the scan into 2, i.e,
    # scan 1 - from start_token to ''
    # scan 2 - from '' to end_token
    num_tokens = 0
    while True:
      arg.range.start_token = start_token
      # arg.range.count = 100 # Default

      #print "rpc", time.time()
      # Issue Cassandra RPC.
      try:
        rpc, getrange_slices_ret = self.__cassandra_client.GetRangeSlices(arg)
      except (RpcClientTransportError, CassandraClientError) as exc:
        print("Cassandra rpc failed with: %s", exc)
        return None

      if not getrange_slices_ret or len(getrange_slices_ret.range_slices) == 0:
        break

      #print "rpc end", time.time()
      # Got results...
      for aCassandraKeySlice in getrange_slices_ret.range_slices:
        start_token = aCassandraKeySlice.key
        # TODO: add comments on how to understand the results.
        for aCassandraColumnOrSuperColumn in aCassandraKeySlice.columns:
          aCassandraColumn = aCassandraColumnOrSuperColumn.column

          proto_obj = None
          if aCassandraColumn.value.value:
            # Create instances from the respective protoobj to parse the protos
            if keyspace in self.keyspace_to_proto_map:
              proto_obj = self.keyspace_to_proto_map[keyspace]()
              proto_obj.ParseFromString(aCassandraColumn.value.value)

          if proto_obj:
            values[aCassandraKeySlice.key] = proto_obj
            #if proto_obj.control_block.owner_vdisk_id == 728129526:
            # log.INFO("Found a match %s" % str(aCassandraKeySlice.key))
            # return proto_obj

      if start_token == prev_start_token:
        break
      prev_start_token = start_token

    print "call end", time.time()
    print("Scan found ", len(values)," Values")
    return values


# Author: nagaraj@nutanix.com
#
# This part uses the MedusaHelper module to walk nfs attr append
# check each inode for criteria that describes the stale inode
# and fixes the inode's stale locs before updating the medusa metadata back.
#
from zeus.zookeeper_session import ZookeeperSession
from zeus.configuration import Configuration

gflags.DEFINE_string("inode_id", "", "nfs inode_id")
gflags.DEFINE_string("inode_id_file", "", "File containing nfs inode_ids one per line")
gflags.DEFINE_boolean("dry_run", True, "If Update is a Dry Run?")
gflags.DEFINE_boolean("fix", False, "Try fixing the stale inodes, if false, just print")
gflags.DEFINE_integer("fs_id", 0, "fs_id part of inode to filter")
gflags.DEFINE_boolean("fix_single_loc", False, "fix single loc; Expert Mode!")

FLAGS = gflags.FLAGS

def process(inode_id, zk_dict):
  outputfile = "./inode_file_%s" % inode_id
  inputfile = "./inode_file_modified_%s" % inode_id

  medusa_printer_proto = medusa_printer_pb2.MedusaPrinterProto()
  cmd = "medusa_printer --lookup nfs --nfs_inode_id %s --save_to_file " \
        "--output_file %s --serialization_format=binary" % (inode_id, outputfile)
  #print cmd
  print "## Processing inode %s" % inode_id

  rv, out, err = command.timed_command(cmd, 60)
  if rv:
    print "error looking up inode %s" % inode_id
    print err
    sys.exit(0)
  medusa_printer_proto.ParseFromString(open(outputfile, "r").read())
  nfs_map = medusa_printer_proto.rows[0].columns[0].nfs_map_entry
  num_data_shards = nfs_map.nfs_attr.num_data_shards

  if num_data_shards:
    cmd += " --nfs_num_data_shards %d" % num_data_shards
    rv, _, _ = command.timed_command(cmd, 60)
    medusa_printer_proto.ParseFromString(open(outputfile, "r").read())

  ret = modify_proto(medusa_printer_proto, inputfile, zk_dict)
  if not ret:
    print "## couldn't modify proto for %s" % inode_id
    return
  cmd = "medusa_printer --lookup nfs --nfs_inode_id %s " \
        "--input_file %s --serialization_format=binary " \
        "--update " % (inode_id, inputfile)
  if num_data_shards:
    cmd += " --nfs_num_data_shards %d" % num_data_shards
  if FLAGS.dry_run:
    print "##Dry Run. Would otherwise run:"
    print cmd
    return
  print "##Executing..."
  rv, _, _ = command.timed_command(cmd, 30)
  print "##Done"

def modify_proto(medusa_proto, inputfile, zk_dict):
  '''
  modify the proto as per the condition and
  write it to a binary file for the consumption of medusa_printer --update
  '''
  if not medusa_proto:
    return False
  col = medusa_proto.rows[0].columns[0]
  entry = col.nfs_map_entry
  inode_id = entry.nfs_attr.inode_id
  inode = "%d:%d:%d" % (inode_id.fsid, inode_id.epoch, inode_id.fid)
  debug_in_file = "./dbg_source_str_%s" % inode
  debug_out_file = "./dbg_target_str_%s" % inode
  if debug_in_file:
    open(debug_in_file, "w").write(str(medusa_proto))

  ctr_dict = zk_dict["ctr_dict"]
  current_epoch =  ctr_dict[str(entry.nfs_attr.container_id)].epoch
  if current_epoch <= entry.nfs_attr.inode_id.epoch:
    print "Nothing to do: inode's epoch is not older than that of container"
    return False

  if len(entry.nfs_attr.locs) == 1 and not FLAGS.fix_single_loc:
    print "Has one loc, needs a manual review"
    return False

  # check if the first loc has invalid component id.
  rm_loc_list = []
  component_dict = zk_dict["component_dict"]
  for i, loc in enumerate(entry.nfs_attr.locs):
    if str(loc.component_id) not in component_dict.keys():
      print "  removing loc with component_id %d at %d" % (loc.component_id, i)
      rm_loc_list.append(i)
    else:
      # check for incarnation and operation ids
      print "%s is in valid_component_list" % loc.component_id
      inc_id, op_id = zk_dict["component_dict"][str(loc.component_id)]
      if inc_id < loc.incarnation_id:
        print "  removing loc with incarnation id %d higher than in zk %d" % (loc.incarnation_id, inc_id)
        rm_loc_list.append(i)
      else:
        print "  nothing to do with this loc"
        print "  inodes incarnation_id:operation_id : %16d:%16d" % (loc.incarnation_id, loc.operation_id)
        print "  zk's incarnation_id:operation_ida  : %16d:%16d" % (inc_id, op_id)

  if not len(rm_loc_list):
    print "removing none; ignoring this inode"
    return False

  if len(rm_loc_list) == len(entry.nfs_attr.locs):
    if not FLAGS.fix_single_loc:
      print "would remove all locs; ignoring this inode"
      return False

  print "removing or fixing %d locs" % len(rm_loc_list)
  for idx in rm_loc_list.__reversed__():
    loc = entry.nfs_attr.locs[i]
    if inc_id < loc.incarnation_id:
      # fix loc
      loc.incarnation_id = inc_id - 1
    else:
      # remove loc
      entry.nfs_attr.locs.remove(loc)

  print "resulting locs of size %d" % len(entry.nfs_attr.locs)

  col.timestamp += 1

  #debugging
  if debug_out_file:
    open(debug_out_file, "w").write(str(medusa_proto))

  if not inputfile:
    print "no input file specified"
    return False
  open(inputfile, "w").write(medusa_proto.SerializeToString())
  return True

def get_zk_data():
  component_zkpath = '/appliance/logical/clock'
  wal_zkpath = '/appliance/logical/stargate/nfs_namespace_last_flushed_walid'
  component_re = r"incarnation_id: (?P<inc_id>\d+)operation_id: (?P<op_id>\d+)"
  zk_dict={}
  zks = ZookeeperSession()
  valid_component_id_list = zks.list(component_zkpath)
  if not len(valid_component_id_list):
    print "Couldn't get component ids"
    sys.exit(0)
  zcp=Configuration().initialize().config_proto()
  ctr_list = zcp.container_list
  ctr_dict={}
  for ctr in zcp.container_list:
    ctr_dict[str(ctr.container_id)] = ctr
  zk_dict["ctr_dict"] = ctr_dict
  component_dict = {}
  for comp_id in valid_component_id_list:
    s = zks.get(component_zkpath + '/' + comp_id)
    re_pattern = re.compile(component_re)
    d = re_pattern.match(s).groupdict()
    component_dict[comp_id] = (int(d["inc_id"]), int(d["op_id"]))
  zk_dict["component_dict"] = component_dict
  last_flushed = zks.get(wal_zkpath)
  zk_dict["last_flushed"] = int(last_flushed)
  return zk_dict

if __name__ == "__main__":
  log.initialize()
  argv = FLAGS(sys.argv)
  mh = MedusaHelper()
  zk_dict=get_zk_data()
  nfsmaps = mh.scan(mh.kNFSMap, mh.kNFSMapCF)
  inode_list = []
  total_viewed = 0
  total_skipped = 0
  total_old_epoch = 0
  total_invalid = 0
  total_walid = 0
  total_snaps = 0
  total_invalid_comp = 0
  total_invalid_incarnation = 0
  total_invalid_operation = 0
  total_invalid_single_loc = 0
  for key, nfsmap in nfsmaps.iteritems():
    total_viewed += 1
    invalid = False
    ctr_dict = zk_dict["ctr_dict"]
    curr_epoch = ctr_dict[str(nfsmap.nfs_attr.container_id)].epoch
    inode_id = nfsmap.nfs_attr.inode_id
    if FLAGS.fs_id and FLAGS.fs_id != inode_id.fsid:
      total_skipped += 1
      continue

    component_dict = zk_dict["component_dict"]
    if curr_epoch > inode_id.epoch:
      reason = ""
      # we don't validate snapshots
      if inode_id.fsid == 0:
        total_snaps += 1
        continue
      total_old_epoch += 1
      last_flushed = zk_dict["last_flushed"]
      prev_wal_id = nfsmap.nfs_attr.prev_wal_id
      wal_id = nfsmap.nfs_attr.wal_id
      inode_str = "%d:%d:%d" % (inode_id.fsid, inode_id.epoch, inode_id.fid)
      if prev_wal_id != -1 and prev_wal_id > last_flushed or \
        wal_id != -1 and wal_id > last_flushed:
        # we allow 10,000 unflushed transactions in the WAL
        print "inode %s has wal-id greater than last flushed wal-id" % inode_str
        print "zk\'s last_flushed_wal_id %s inode\'s wal_id %d, prev_wal_id %d" % (last_flushed, wal_id, prev_wal_id)
        print "need manual review of this inode"
        total_walid += 1
        continue

      for loc in nfsmap.nfs_attr.locs:
        if str(loc.component_id) not in component_dict.keys():
          invalid = True
          reason = "Component id %d not present" % loc.component_id
          total_invalid += 1
          total_invalid_comp += 1
          break
        inc_id, op_id = zk_dict["component_dict"][str(loc.component_id)]
        if inc_id < loc.incarnation_id :
          invalid = True
          reason = "The loc's incarnation_id is greater: loc: %d vs. %d" % (loc.incarnation_id, inc_id)
          total_invalid += 1
          total_invalid_incarnation += 1
        elif inc_id == loc.incarnation_id and op_id < loc.operation_id:
          invalid = True
          reason = "The loc's operation_id is greater: %d vs. %d" % (loc.operation_id, op_id)
          total_invalid += 1
          total_invalid_operation += 1

      # this is to consider single stale loc that needs to be removed.
      # if we have multiple locs and all stale, we still need manual fixing.
      if invalid and len(nfsmap.nfs_attr.locs) == 1:
        total_invalid_single_loc += 1
        reason += " and there is only one loc in the inode."

      if not invalid:
        continue

      if not FLAGS.fix:
        print "========="
        print "current epoch %d : inode_epoch %d" % (curr_epoch, inode_id.epoch)
        print "Stale inode: %s" % (inode_str)
        print "========="
        print "DEBUG Reason: %s" % reason
        print nfsmap.nfs_attr
      inode_list.append(inode_str)

  print "total_viewed: %d" % total_viewed
  print "total_skipped: %d" % total_skipped
  print "total_old_epoch: %d" % total_old_epoch
  print "total_invalid: %d -- total_invalid_comp: %d; total_invalid_operation: %d;" \
    " total_invalid_incarnation: %d" % (total_invalid, total_invalid_comp, \
    total_invalid_operation, total_invalid_incarnation)
  print "total_walid: %d" % total_walid
  print "total_invalid_single_loc: %d" % total_invalid_single_loc
  print "total_snaps: %d" % total_snaps
  if not FLAGS.fix:
    sys.exit(0)

  for inode in inode_list:
    process(inode, zk_dict)
  sys.exit(0)
