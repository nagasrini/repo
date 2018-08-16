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

import sys
sys.path.append('/home/nutanix/cluster/bin')
import env
import gflags

from medusa import medusa_pb2
import util.base.log as log
try:
  ## 5.5 code has client here.
  from cassandra.cassandra_client import cassandra_pb2
  from cassandra.cassandra_client.client import CassandraClient, CassandraClientError
except:
  from util.cassandra import cassandra_pb2
  from util.cassandra.client import CassandraClient, CassandraClientError

from alerts.manager import alert_pb2

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
                        kNFSMap:medusa_pb2.MedusaNFSMapEntryProto,
                        kVDiskBlockMap:medusa_pb2.MedusaVDiskBlockMapEntryProto,
                        kEGIDMap:medusa_pb2.MedusaExtentGroupIdMapEntryProto,
                        kAlertsMap:alert_pb2.AlertProto}
    
  
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

      print "rpc", time.time()
      # Issue Cassandra RPC.
      try:
        rpc, getrange_slices_ret = self.__cassandra_client.GetRangeSlices(arg)
      except (RpcClientTransportError, CassandraClientError) as exc:
        print("Cassandra rpc failed with: %s", exc)
        return None
  
      if not getrange_slices_ret or len(getrange_slices_ret.range_slices) == 0:
        break

      print "rpc end", time.time()
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

if __name__ == "__main__":
  log.initialize()
  mh = MedusaHelper()

  from zeus.configuration import Configuration
  zcp=Configuration().initialize().config_proto()
  ctr_list = zcp.container_list
  ctr_dict={}
  for ctr in zcp.container_list:
    ctr_dict[str(ctr.container_id)] = ctr
  
  nfsmaps = mh.scan(mh.kNFSMap, mh.kNFSMapCF)
  # Add the check here.
  for key, nfsmap in nfsmaps.iteritems():
    current_epoch =  ctr_dict[str(nfsmap.nfs_attr.container_id)].epoch
    inode_epoch = nfsmap.nfs_attr.inode_id.epoch
    #print nfsmap.nfs_attr
    print "========="
    print "%d: %d" % (current_epoch, inode_epoch)
    print "========="
    if current_epoch > inode_epoch:
      inode_id = nfsmap.nfs_attr.inode_id
      print "DEBUG"
      print nfsmap.nfs_attr
