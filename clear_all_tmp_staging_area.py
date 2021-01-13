#
# Copyright (c) 2014 Nutanix Inc. All rights reserved.
#
# Author: rnallan@nutanix.com, nagaraj@nutanix.com

# Simple helper to access the metadata from medusa
# It can lookup/scan:
#  - NearSyncStagingAreaMap
#    The medusa_lookup currently cannot scan NearSyncStagingAreaMap. Hence the need for this script
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

gflags.DEFINE_string("cg_id", "", "cg_id corresponding to the path"                                                                           
                     " to be removed in a:b:c format")
gflags.DEFINE_string("prefix", "", "prefix of the path to be removed in "                                                                     
                     "/.snapshot/tmp_staging/[x%100]/x-y-z/ format")
FLAGS = gflags.FLAGS

from medusa import medusa_pb2
import util.base.log as log
try:
  ## 5.5 code has client here.
  from cassandra.cassandra_client import cassandra_pb2
  from cassandra.cassandra_client.client import CassandraClient, CassandraClientError
except:
  from util.cassandra import cassandra_pb2
  from util.cassandra.client import CassandraClient, CassandraClientError

from medusa.medusa_pb2 import MedusaNearSyncStagingAreaMapEntryProto

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

  kStagingAreaMap="medusa_nearsync_maps"
  kStagingAreaCF="nearsyncstagingareamap"
  kStaginAreaColumn = "0"

  keyspace_to_proto_map = {kStagingAreaMap:MedusaNearSyncStagingAreaMapEntryProto}

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
      except Exception as exc:
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

      if start_token == prev_start_token:
        break
      prev_start_token = start_token

    log.INFO("call end %s" % time.time())
    log.INFO("Scan found %s Values" % len(values))
    return values


def Do_RPC():
  log.INFO("Issing RPC with %s, %s" % (FLAGS.prefix, FLAGS.cg_id))
  stargate_client = StargateClient()

  arg = NfsLWSClearStagingAreaArg()
  arg.staging_area_path_prefix = FLAGS.prefix
  arg.cg_id.originating_cluster_id = int(FLAGS.cg_id.split(':')[0])
  arg.cg_id.originating_cluster_incarnation_id = int(FLAGS.cg_id.split(':')[1])
  arg.cg_id.id = int(FLAGS.cg_id.split(':')[2])

  try:
    ret = stargate_client.NfsLWSClearStagingArea(arg)
  except Exception as e:
    log.ERROR("RPC failed with %s " % e)
    return
  log.INFO("RPC succeeded ")


if __name__ == "__main__":
  log.initialize()
  if FLAGS.prefix != "" and FLAGS.cg_id != "":
    Do_RPC()
    sys.exit(0)

  m = MedusaHelper()

  maps = m.scan(m.kStagingAreaMap, m.kStagingAreaCF)
  log.DEBUG(maps)
  for k,v in maps.iteritems():
    log.INFO("--cg_id=%s:%s:%s --prefix=%s" % (v.cg_id.originating_cluster_id,
                                            v.cg_id.originating_cluster_incarnation_id,
                                            v.cg_id.id, v.staging_area_dir_path))
    if "tmp_staging" in v.staging_area_dir_path:
      FLAGS.prefix = v.staging_area_dir_path
      FLAGS.cg_id = "%d:%d:%d" % (v.cg_id.originating_cluster_id,
                                  v.cg_id.originating_cluster_incarnation_id,
                                  v.cg_id.id)
      Do_RPC()
  sys.exit(0)