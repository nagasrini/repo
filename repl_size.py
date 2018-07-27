import os
import sys
for path in os.listdir("/usr/local/nutanix/lib/py"):
        sys.path.insert(0, os.path.join("/usr/local/nutanix/lib/py", path))

from cerebro.interface.cerebro_interface_pb2 import *
from cerebro.master.cerebro_schedule_pb2 import CerebroOutOfBandScheduleProto
from cerebro.client.cerebro_interface_client import CerebroInterfaceTool, CerebroInterfaceError
from cerebro.master.persistent_op_state_pb2 import PersistentOpBaseStateProto as ost
from util.nfs.nfs_client import NfsClient, NfsError
import util.base.log as log
import gflags

gflags.DEFINE_string("pd_name", "", "PD name")
gflags.DEFINE_integer("snap_id", 0, "Snapshot ID")
gflags.DEFINE_integer("ref_snap_id", 0, "Ref Snapshot ID")

FLAGS = gflags.FLAGS

RpcClient = CerebroInterfaceTool()

class Range():
  def __init__(self, start=0, end=0):
    if start > end:
      start = end = 0
    if start == 0 and end == 0:
      self.chunks = []
    else:
      self.chunks = [[start, end]]

  def overlap(self, chunk):
    for n in self.chunks:
      if (chunk[0] >= n[0] and chunk[0] <= n[1]) or\
        (chunk[1] >= n[0] and chunk[1] <= n[1]):
        return n
    return 0

  def borders(self, chunk):
    for n in self.chunks:
      if chunk[0] == n[1] + 1:
        return n, 1
      if chunk[1] == n[0] - 1:
        return n, 0
    return None, 0

  def merge(self):
    for i, n in enumerate(self.chunks):
      if i + 1 >= len(self.chunks):
        return
      m = self.chunks[i+1]
      if n[1]+1 == m[0]:
        print "merging %s and %s" % (n, m)
        n[1] = m[1]
        self.chunks.remove(m)
  
  def add(self, chunk):
    if len(chunk) != 2:
      print "oversized"
      return
    if chunk[0] > chunk[1]:
      print "wrong chunk"
      return
    if not self.overlap(chunk):
      x, i = self.borders(chunk)
      if x:
        print "borders"
        x[i] = chunk[i]
      else:
        self.chunks.append(chunk)
        self.chunks.sort()
      self.merge()
    else:
      print "%s Overlaps!!" % chunk
    self.chunks.sort()

  def __repr__(self):
    return "Range chunks: %s" % self.chunks

  def sum(self):
    s = 0
    for n in self.chunks:
      s += n[1] - n[0] + 1
    return s


# returns PD object by the name specified
def get_pd_obj(name):
  if not name:
    print "Name of PD not speified"

  arg = QueryProtectionDomainArg()
  ret = QueryProtectionDomainRet()
  arg.protection_domain_name = name
  arg.list_snapshot_handles.CopyFrom(QueryProtectionDomainArg.ListSnapshotHandles())
  arg.list_consistency_groups.CopyFrom(QueryProtectionDomainArg.ListConsistencyGroups())
  arg.list_files = True
  try:
    ret = RpcClient.query_protection_domain(arg)
  except CerebroInterfaceError as e:
    print "Error querying pds %s" % e
  return ret

def get_nfs_paths(pd):
  path_list=[]
  for path in pd.vec:
    path_list.append(path)
  return path_list

# returns repl_metaop_vec(executing_meta_op_vec)
def get_repl_metaops(name):
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
  repl_metaop_vec = []
  for eop in ret.executing_meta_op_vec:
    if eop.base_persistent_state.opcode == ost.Opcode.Value("kReplicateMetaOp"):
      repl_metaop_vec.append(eop)
  return repl_metaop_vec

def process(pdname,shlist):
  cb_dict = {}
  pd = get_pd_obj(pdname)
  blklen = 0
  for path in pd.file_vec:
    ctr = path.split('/',2)[1]
    rpath = path.split('/',2)[2]
    snap_path_list=[]
    client = NfsClient("127.0.0.1", "/" + ctr)
    for pdscb in pd.snapshot_control_block_vec:
      sh = pdscb.handle
      if sh not in shlist:
        continue
      hash = sh.entity_id % 100
      snpath="/.snapshot/%02d/%d-%d-%d/%s" % \
        (hash,sh.cluster_id,sh.cluster_incarnation_id,sh.entity_id,rpath)
      if not client.lstat(snpath):
        log.ERROR("Snapshot Path %s does not exist", snpath)
        return None
      snap_path_list.append("/" + ctr + snpath)
    cb_dict[path] = get_changed_blocks(snap_path_list)
  return cb_dict


def get_changed_blocks(snap_path_list):
  arg=ComputeChangedRegionsArg()
  ret=ComputeChangedRegionsRet()

  arg.snapshot_file_path = snap_path_list[0]
  arg.reference_snapshot_file_path = snap_path_list[1]
  try:
    ret = RpcClient.compute_changed_regions(arg)
  except CerebroInterfaceError as e:
    print "Error from ComputeChangedRegions %s" % e
    print "for %s " % snap_path_list
    return Range()
  changed_blk_range = Range()
  blksize = 1024*1024
  for reg in ret.changed_region_list:
    start_blknum = reg.offset / blksize
    end_blknum = (reg.offset + reg.length - 1) / blksize
    log.INFO("%s %s %d-%d" % (snap_path_list[0], snap_path_list[1], reg.offset, reg.offset + reg.length))
    changed_blk_range.add([start_blknum, end_blknum])
  return changed_blk_range

# Helpers
def change_blocks_summary(cbr_dict):
  total = 0
  for k,v in cbr_dict.iteritems():
    print "path %s" % k
    print "  block counts %d" % v.sum()
    total += v.sum()
  print "total block counts %d" % total

def snap_str(snap):
  return "(%s,%s,%s)" % (snap.cluster_id,snap.cluster_incarnation_id,snap.entity_id)

def main(pdname, snap, ref_snap):
  dlist=[]
  if not snap:
    repl_metaop_vec = get_repl_metaops(pdname)
    if not len(repl_metaop_vec):
      print "No replications running for the pd %s" % pdname
      return
    for repl in repl_metaop_vec:
      metaop = repl.meta_opid
      actions=repl.base_persistent_state.reference_actions.replication
      for action in actions:
        sn=[]
        sn.append(action.snapshot_handle_vec[0])
        sn.append(action.reference_snapshot_handle)
        print "Replication (%d) Snapshots for remote: %s" % (metaop, action.remote_name)
        print "  Snapshot:     %s" % snap_str(sn[0])
        print "  Ref Snapshot: %s" % snap_str(sn[1])
        d=process(pdname, sn)
        print d
        change_blocks_summary(d)
        dlist.append(d)
  else:
    sn = [None, None]
    pd = get_pd_obj(pdname)
    for sh in pd.snapshot_handle_vec:
      if sh.entity_id == snap:
        sn[0] = sh
      if sh.entity_id == ref_snap:
        sn[1] = sh
    if None in sn:
      if snap and ref_snap:
        log.ERROR("One of the snapshots doesn't exist")
      return dlist
    d=process(pdname, sn)
    print d
    change_blocks_summary(d)
    dlist.append(d)
  return dlist

if __name__ == "__main__":
  argv = FLAGS(sys.argv)
  log.initialize("/home/nutanix/repl_size.INFO")
  if not FLAGS.pd_name:
    log.ERROR("No PD name specified")
    sys.exit(1)
  if not FLAGS.snap_id:
    main(FLAGS.pd_name, None, None)
  else:
    main(FLAGS.pd_name, FLAGS.snap_id, FLAGS.ref_snap_id)
