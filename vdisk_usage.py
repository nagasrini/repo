import sys
import time
sys.path.append("/home/nutanix/serviceability/bin")
import env
import cStringIO
import util.base.log as log
from medusa import medusa_pb2
from cassandra.cassandra_client import cassandra_pb2
from util.net.http_protobuf_rpc import HttpProtobufRpcClient
from util.net.sync_rpc_client import RpcClientTransportError
from util.net.sync_rpc_client import SyncRpcClientMixin
from util.net.sync_rpc_client import sync_rpc_client_metaclass
from cerebro.master import cerebro_master_WAL_pb2
from pithos.pithos_pb2 import VDiskConfig
from hyperint.hyperint_cloud.pithos_helper import PithosHelper
import util.base.command as command
from medusa.medusa_pb2 import MedusaNFSMapEntryProto
import medusa.medusa_printer_pb2 as medusa_printer_pb2

from tools.vdisk_usage_printer.vdisk_usage_pb2 import VdiskUsageProto


try:
  from alerts.interface.alert_pb2 import AlertProto
except:
  from alerts.manager.alert_pb2 import AlertProto

import gflags

gflags.DEFINE_integer("vdisk_id", 0, "vdisk id to scan")
FLAGS = gflags.FLAGS



#from util.sl_bufs.base.data_transformation_type_pb2 import DataTransformationInfoProto

try:
  ## 5.5 code has client here.
  from cassandra.cassandra_client import cassandra_pb2
  from cassandra.cassandra_client.client import CassandraClient, CassandraClientError
except:
  from util.cassandra import cassandra_pb2
  from util.cassandra.client import CassandraClient, CassandraClientError

try:
  # 5.9 code has kVDiskBlockMap in different location
  from medusa.medusa_pb2 import MedusaVDiskBlockMapEntryProto
except:
  from medusa.vdisk_block_map_pb2 import MedusaVDiskBlockMapEntryProto

# cerebro
from cerebro.interface.cerebro_interface_pb2 import *
from cerebro.client.cerebro_interface_client import CerebroInterfaceTool, CerebroInterfaceError
RpcClient = CerebroInterfaceTool()

class CassandraException(Exception):
  def __init__(self, message, code=None):
    Exception.__init__(self, message)
    self.code = code
class CassandraClient(SyncRpcClientMixin):
  __metaclass__ = sync_rpc_client_metaclass(cassandra_pb2.CassandraRpcSvc_Stub)

  def __init__(self, host, port):
    SyncRpcClientMixin.__init__(self, cassandra_pb2.CassandraError)
    self.__host = host
    self.__port = port

  def _create_stub(self):
    rpc_client = HttpProtobufRpcClient(self.__host, self.__port)
    return cassandra_pb2.CassandraRpcSvc_Stub(rpc_client)

  def _filter_rpc_result(self, rpc, err, ret):
    if err != cassandra_pb2.CassandraError.kNoError:
      raise CassandraException(self.strerror(), code=err)
    return ret

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
kNumHashChars = 62
kHashChars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
kHexChars = '0123456789ABCDEF'
kvDiskBlockMapMaxCellsPerRow = 128

class MedusaHelper(object):


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
  kNFSMap = "medusa_nfsmap"
  kNFSMapCF = "nfsmap"
  kNFSMapColumn = "0"
  kVDiskBlockMap = "medusa_vdiskblockmap"
  kVDiskBlockMapCF = "vdiskblockmap"
  kVblockSize = 1024*1024

  # alerts
  kAlertsMap = "alerts_keyspace"
  kAlertsCF = "alerts_cf"

  keyspace_to_proto_map = {
                        kNFSMap:medusa_pb2.MedusaNFSMapEntryProto,
                        kVDiskBlockMap:MedusaVDiskBlockMapEntryProto,
                        kAlertsMap:AlertProto}

  def __init__(self):
    self.__host = "127.0.0.1"
    self.__port = "9161"
    self.__cassandra_client = CassandraClient(self.__host, self.__port)

    rpc_client = HttpProtobufRpcClient(self.__host, self.__port)
    self.cassandra_svc = cassandra_pb2.CassandraRpcSvc_Stub(rpc_client)

  def lookup_medusa(self, keyspace, keys, column_family, column_names, proto_type):
    ''' Common code to lookup cassandra by making a RPC call
    Args:
      keyspace: the keyspace for lookup (map name)
      keys: List of keys to lookup in the keyspace
      column_family
      column_names: List of columns to lookup
      medusa_entry_proto_cls: Class of the proto obj which the result
                                  will be parsed from
    Return: Returns a tuple of error and list of values
    '''
    # Prepare args for MultigetSlice().
    arg = cassandra_pb2.CassandraMultigetSliceArg()
    arg.keyspace = keyspace
    arg.priority = False
    arg.keys.extend(keys)
    arg.column_parent.column_family = column_family
    arg.predicate.column_names.extend(column_names)
    arg.consistency_level = cassandra_pb2.CassandraConsistencyLevel.kQuorum

    # Issue Cassandra RPC.
    try:
      multiget_slice_ret = self.__cassandra_client.MultigetSlice(arg)
    except CassandraException, ex:
      print "Error: Cassandra error %d" % ex.code
    except RpcClientTransportError, ex:
      print "Error: RPC error %d" % ex.code
    values = []
    for xx in range(len(multiget_slice_ret.column_list)):
      column_list_elem = multiget_slice_ret.column_list[xx]
      if len(column_list_elem.columns) == 0:
        values.append(None)
        continue
      #CHECK_EQ(len(column_list_elem.columns), 1)
      column = column_list_elem.columns[0]
      CHECK(column.column)
      medusa_entry_proto = proto_type()
      if proto_type == MedusaVDiskBlockMapEntryProto:
        # skipping a lot of things in misc/base_message.h
        # and assuming this is kProto message.
        # needs work if this is kFlatbuffer
        actual_val = column.column.value.value[8:]
      else:
        actual_val = column.column.value.value
      medusa_entry_proto.ParseFromString(actual_val)
      values.append(medusa_entry_proto)
    return values

  def scan(self, keyspace, cf, column_name=None, start_token="", end_token=""):
    '''
     Scan 'cf' Column Family in 'keyspace' and return all objects.
     It parses the return objects for the corresponding map using the mapping
     in keyspace_to_proto_map. To add support for newer map, add the values to
     this map.
    '''
    prev_start_token = start_token
    error = MedusaErrorStatus.medusa_error_to_string[MedusaErrorStatus.kNoError]

    values = {}
    # Prepare args for MultigetSlice().
    arg = cassandra_pb2.CassandraGetRangeSlicesArg()
    arg.keyspace = keyspace
    arg.column_parent.column_family = cf
    arg.predicate.slice_range.count = 100 # medusa_printer sets this to kint32max, 2^32?
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
        getrange_slices_ret = self.__cassandra_client.GetRangeSlices(arg)
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
              vheader = hex(struct.unpack("<I", x[:4])[0])
              if vheader == ''0xacedfeed':
                 print "One of the values is in kFlatbuffer format: can't parse"
                 break
              if vheader == "0xfeeefeee" and keyspace == mh.kVDiskBlockMap:
                # skipping a lot of things in misc/base_message.h
                # and assuming this is kProto message.
                # needs work if this is kFlatbuffer
                actual_val = aCassandraColumn.value.value[8:]
              else:
                actual_val = aCassandraColumn.value.value
              proto_obj.ParseFromString(actual_val)

          if proto_obj:
            values[aCassandraKeySlice.key] = proto_obj

      if start_token == prev_start_token:
        break
      prev_start_token = start_token

    print "call end", time.time()
    print("Scan found ", len(values)," Values")
    return values

def get_hash(val, len=8):
  buf = cStringIO.StringIO()
  for xx in range(8):
    rem = val % kNumHashChars
    val /= kNumHashChars
    buf.write(kHashChars[rem])
  return(buf.getvalue())

def get_next_hash(hash):
  nh = ""
  carry = 1
  for n in range(len(hash)-1,-1,-1):
    val = ord(hash[n]) + carry
    if chr(val) in kHashChars:
      carry = 0
    else:
      val = 48
      carry = 1
    nh = chr(val) + nh
  return nh

def nfs_key(nfs_inode_id):
  fields = nfs_inode_id.split(":")
  fsid = int(fields[0])
  epoch = int(fields[1])
  fid = int(fields[2])
  hash = get_hash(fid)
  nfs_inode_id_int128 = (fsid << 96) | (epoch << 32) | fid
  #print hex(nfs_inode_id_int128)
  return "%s-nfs:%d" % (hash,nfs_inode_id_int128)

def ToHex(value):
  buf=''
  for xx in range(15, -1, -1):
    buf = kHexChars[value & 0xF] + buf
    value >>= 4
  return buf

def vblock_key(vdisk_id, vblk):
  hash=get_hash(vdisk_id)
  row_blk = (vblk/kvDiskBlockMapMaxCellsPerRow) * kvDiskBlockMapMaxCellsPerRow
  return "%s:%d:%s" % (hash,vdisk_id,ToHex(row_blk))

def NfsMap(inode_id):
  """
  Do medusa lookup for one inode
  """
  mh = MedusaHelper()
  keys = [nfs_key(inode_id)]
  nfs_map = mh.lookup_medusa(kNFSMap, keys, kNFSMapCF, kNFSMapColumn, medusa_pb2.MedusaNFSMapEntryProto)
  return nfs_map

def VBlockMapMedusa(vdisk_id, vblock_num=None):
  """
  Read vblockmap of a single/multiple blocks from medusa
  """
  mp_vblock_proto = medusa_printer_pb2.MedusaPrinterProto()
  outputfile = "/tmp/vblock.bin"
  cmd = "medusa_printer -lookup vblock "
  if not vblock_num:
    hash=get_hash(vdisk_id)
    nhash = get_next_hash(hash)
    cmd += "--scan --start_token %s --end_token %s " % (hash, nhash)
  else:
    cmd += "--vdisk_id %d --vblock_num %d " % (vdisk_id, vblock_num)
  cmd += "--print_values_for_scan --save_to_file --serialization_format " \
        "binary --output_file %s" % (outputfile)
  #print cmd
  rv, out, err = command.timed_command(cmd, 60)
  if rv:
    print "error scanning nfs inodes"
    print err
    sys.exit(0)
  if "negative cache value" in out:
    print "negative cache value"
    return None
  mp_vblock_proto.ParseFromString(open(outputfile, "r").read())
  vblk_map = []
  for row in mp_vblock_proto.rows:
    for column in row.columns:
      vblk_map.append(column.block_map_entry)
  return vblk_map

def VBlockMap(vdisk_id, vblock_num):
  """
  Do medusa lookup for vblock map
  The value from cassandra is not being parsed into the proto
  """
  mh = MedusaHelper()
  keys = [vblock_key(vdisk_id, vblock_num)]
  print keys
  col_name = "%.16d" % vblock_num
  vblk_map = mh.lookup_medusa(kVDiskBlockMap, keys, kVDiskBlockMapCF, [col_name], MedusaVDiskBlockMapEntryProto)
  return vblk_map

def get_vdisk_config(vdisk_id):
  pithos_helper = PithosHelper()
  vdisk_config = pithos_helper.lookup_vdisk_config_by_id(vdisk_id)
  return vdisk_config

# returns dic of PD snapshot_id as key and vdisk list as value
def get_pd_snapshot_vdisks(name):
  snap_vdisk = {}
  qpd_arg = QueryProtectionDomainArg()
  #ret = QueryProtectionDomainRet()
  qpd_arg.protection_domain_name = name
  qpd_arg.list_snapshot_handles.CopyFrom(QueryProtectionDomainArg.ListSnapshotHandles())
  qpd_arg.list_consistency_groups.CopyFrom(QueryProtectionDomainArg.ListConsistencyGroups())
  qpd_arg.list_files = True
  try:
    qpd_ret = RpcClient.query_protection_domain(qpd_arg)
  except CerebroInterfaceError as e:
    print "Error querying pds %s" % e
  for scb in qpd_ret.snapshot_control_block_vec:
    sh = scb.handle
    sh_str = "%d:%d:%d" % (sh.cluster_id, sh.cluster_incarnation_id, sh.entity_id)
    snap_vdisk[sh.entity_id] = [v for v in scb.entity_vdisk_id_vec]
  for snap in snap_vdisk:
    print "snapshot id %d" % (snap)
    for vdisk in snap_vdisk[snap]:
      print "%12s | %12s " % (vdisk, vdisk_excl_bytes(vdisk)[0])
  return snap_vdisk


if __name__ == "__main__":
  gflags.FLAGS(sys.argv)
  log.initialize()
  mh = MedusaHelper()
  key = vblock_key(FLAGS.vdisk_id, 0)
  hash = key.split(":")[0]
  nhash = get_next_hash(hash)
  print "hash %s; nhash %s" % (hash, nhash)
  maps = mh.scan(mh.kVDiskBlockMap, mh.kVDiskBlockMapCF, start_token=hash, end_token=nhash)
  #print maps
  for key,map in maps.iteritems:
    region_cnt = len(map.regions)
