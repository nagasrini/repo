#
# Copyright (c) 2014 Nutanix Inc. All rights reserved.
#
# Author: rnallan@nutanix.com
#
# Simple helper to access the metadata from medusa
# It can lookup:
#  - NearSyncStagingAreaMap MedusaNearSyncOplogMapEntryProto
# Can be extended to do any map
# Do a scan
#

__all__ = ["MedusaErrorStatus", "MedusaHelper"]

import cStringIO
import time
import re

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

from medusa.medusa_pb2 import MedusaNearSyncOplogMapEntryProto

gflags.DEFINE_integer("disk_id", 0, "disk_id to filter")
gflags.DEFINE_string("source_ip", "", "source ip address")
FLAGS = gflags.FLAGS


# ==============================================================================
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


# ==============================================================================
class MedusaHelper(object):
    ''' Class that provides a subset of the interface found in medusa/medusa.h. '''

    # Mapping from a Cassandra error code to the corresponding Medusa error code
    # for Cassandra error codes we expect to potentially see.
    CASSANDRA_MEDUSA_ERROR_MAP = {
        cassandra_pb2.CassandraError.kNoError:
            MedusaErrorStatus.kNoError,
        cassandra_pb2.CassandraError.kCasFailure:
            MedusaErrorStatus.kCASFailure,
        cassandra_pb2.CassandraError.kTimeout:
            MedusaErrorStatus.kTimeoutError,
        cassandra_pb2.CassandraError.kEpochMismatch:
            MedusaErrorStatus.kCASFailure,
        cassandra_pb2.CassandraError.kUnavailable:
            MedusaErrorStatus.kBackendUnavailable,
        cassandra_pb2.CassandraError.kInvalidRequest:
            MedusaErrorStatus.kBackendUnavailable,
        cassandra_pb2.CassandraError.kRetry:
            MedusaErrorStatus.kRetry}

    # Medusa Cassandra constants.
    # TODO: Move these as class variables

    kNearSyncMaps = "medusa_nearsync_maps"
    kNearSyncConsistencyGroupMapColumnFamily = "nearsyncoplogmap"
    kNearSyncConsistencyGroupMapColumn = "0"

    keyspace_to_proto_map = {kNearSyncMaps: MedusaNearSyncOplogMapEntryProto}

    # Cassandra token hash constants.
    kNumHashChars = 62
    kHashChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"

    replica_status_to_string = {
        medusa_pb2.MedusaExtentGroupIdMapEntryProto.Replica.kHealthy: 'Healthy',
        medusa_pb2.MedusaExtentGroupIdMapEntryProto.Replica.kCorrupt: 'Corrupt',
        medusa_pb2.MedusaExtentGroupIdMapEntryProto.Replica.kMissing: 'Missing'}

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
        arg.predicate.slice_range.count = 10  # medusa_printer sets this to kint32max, 2^32?
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

            # print "rpc", time.time()
            # Issue Cassandra RPC.
            try:
                rpc, getrange_slices_ret = self.__cassandra_client.GetRangeSlices(arg)
            except Exception as exc:
                print("Cassandra rpc failed with: %s", exc)
                return None

            if not getrange_slices_ret or len(getrange_slices_ret.range_slices) == 0:
                break

            # print "rpc end", time.time()
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
                        # if proto_obj.control_block.owner_vdisk_id == 728129526:
                        # log.INFO("Found a match %s" % str(aCassandraKeySlice.key))
                        # return proto_obj

            if start_token == prev_start_token:
                break
            prev_start_token = start_token

        print "call end", time.time()
        print("Scan found ", len(values), " Values")
        return values


def get_vdisk_names_for_disk():
    m = MedusaHelper()
    maps = m.scan(m.kNearSyncMaps, m.kNearSyncConsistencyGroupMapColumnFamily)
    # print maps
    vdisk_name_list = []
    for k, val in maps.iteritems():
        found = False
        for vdisk_oplog in val.vdisk_oplogs:
            for episode in vdisk_oplog.episodes:
                for stripe in episode.secondary_stripes:
                    if FLAGS.disk_id in stripe.replica_disks:
                        found = True
                if episode.primary_disk == FLAGS.disk_id:
                    found = True
            if found:
                vdisk_name_list.append(vdisk_oplog.vdisk_name)

    return vdisk_name_list


from pithos.pithos_pb2 import VDiskConfig
from pithos.client.pithos_client import PithosClient
from cassandra.cassandra_client.client import CassandraClient


def pclient(host=None):
    cassandra_client = CassandraClient(host, "9161")
    pithos_client = PithosClient(cassandra_client=cassandra_client)
    pithos_client.initialize()
    return pithos_client


def get_source_vdisk_config_for_names(vdisk_names):
    pithos_client = pclient(host=FLAGS.source_ip)
    entries = pithos_client.create_iterator('vdisk_id', skip_values=False, consistent=False)
    config_dict = {}
    for entry in entries:
        vdisk_config = pithos_client.entry_to_vdisk_config(entry, include_deleted=True)
        if vdisk_config and vdisk_config.vdisk_name in vdisk_names:
            config_dict[vdisk_config.vdisk_name] = vdisk_config.iscsi_target_name
    return config_dict


# Get the remote VM config
from acropolis.acropolis_interface_pb2 import VmGetArg
from acropolis.client import AcropolisClient
from util.base.types import NutanixUuid


def get_all_vms():
    client = AcropolisClient(FLAGS.source_ip)
    vm_to_vdisk_map = {}
    for vm_info in client.vm_get_iter():
        vdisk_list = []
        print vm_info
        for disk in vm_info.config.disk_list:
            if disk.HasField("vmdisk_uuid") and disk.vmdisk_uuid:
                vdisk_list.append(disk)
        vm_to_vdisk_map[vm_info.config.name] = vdisk_list
    return vm_to_vdisk_map


if __name__ == "__main__":
    log.initialize()
    argv = FLAGS(sys.argv)
    if not FLAGS.source_ip:
        log.ERROR("No remote ip specified")
        print("No remote ip specified")
        sys.exit(1)

    vdisk_names = get_vdisk_names_for_disk()

    vdisk_iscsi_names = get_source_vdisk_config_for_names(vdisk_names)
    #print "v %s" % vdisk_iscsi_names

    # VMs using thoe vdisks
    vm_map = get_all_vms()
    for name, dl in vm_map.iteritems():
        header = False
        for disk in dl:
            for vdisk_name, iscsi_name in vdisk_iscsi_names.iteritems():
                uuid = NutanixUuid(disk.vmdisk_uuid)
                if uuid.hex in iscsi_name:
                    if not header:
                        print("%-20s%-10s%-20s%-20s%40s" % (
                        "VM Name", "SCSI addr", "Vdisk Name", "Vdisk Size", "ISCSI Name"))
                        header = True
                    addr = "%s:%s" % (disk.addr.bus, disk.addr.index)
                    print("%-20s%-10s%-20s%-20d%40s" % (name, addr, vdisk_name, disk.vmdisk_size, iscsi_name))
