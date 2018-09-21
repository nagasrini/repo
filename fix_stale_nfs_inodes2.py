import re
import sys
sys.path.append('/home/nutanix/cluster/bin')
import env
import gflags
import subprocess
import medusa.medusa_printer_pb2 as medusa_printer_pb2
import util.base.command as command

from zeus.zookeeper_session import ZookeeperSession
from zeus.configuration import Configuration

gflags.DEFINE_string("inode_id", "", "nfs inode_id")
gflags.DEFINE_string("inode_id_file", "", "File containing nfs inode_ids one per line")
gflags.DEFINE_boolean("dry_run", True, "If Update is a Dry Run?")
gflags.DEFINE_boolean("fix", False, "Try fixing the stale inodes, if false, just print")
gflags.DEFINE_integer("fs_id", 0, "fs_id part of inode to filter")

FLAGS = gflags.FLAGS

def read_all_nfs_inodes(zk_dict):
'''
Scans the nfsmap through medusa and returns proto of all the nfsmaps.
Checks if it can modify each row to fix the stale inodes.
If modified, updates medusa nfsmap.
'''
  nfs_proto = medusa_printer_pb2.MedusaPrinterProto()
  outputfile = "/tmp/inodes.bin"
  cmd = "medusa_printer --scan -lookup nfs --print_values_for_scan --save_to_file --serialization_format binary --output_file %s" % outputfile
  rv, out, err = command.timed_command(cmd, 60)
  if rv:
    print "error scanning nfs inodes"
    print err
    sys.exit(0)

  nfs_proto.ParseFromString(open(outputfile, "r").read())
  for row in nfs_proto.rows:
    col = row.columns[0]
    entry = col.nfs_map_entry
    ctr_dict = zk_dict["ctr_dict"]
    if FLAGS.fs_id and FLAGS.fs_id != entry.nfs_attr.inode_id.fsid:
      continue
    current_epoch = ctr_dict[str(entry.nfs_attr.container_id)].epoch
    inode_epoch = entry.nfs_attr.inode_id.epoch
    if current_epoch <= inode_id.epoch:
      # this inode is not stale
      continue
    one_inode = medusa_printer_pb2.MedusaPrinterProto()
    one_inode.rows.MergeFrom([row])
    inputfile = "./inode_file_modified_%s" % entry.nfs_attr.inode_id
    ret = modify_proto(one_inode, inputfile, zk_dict)
    if not ret:
      print "##couldn't modify the inode "
    num_data_shards = entry.nfs_attr.num_data_shards
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

def process(inode_id, zk_dict):
  outputfile = "./inode_file_%s" % inode_id
  inputfile = "./inode_file_modified_%s" % inode_id

  medusa_printer_proto = medusa_printer_pb2.MedusaPrinterProto()
  cmd = "medusa_printer --lookup nfs --nfs_inode_id %s --save_to_file " \
        "--output_file %s --serialization_format=binary" % (inode_id, outputfile)
  #print cmd
  print "##Processing inode %s" % inode_id

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
    print "##couldn't modify proto"
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

  if len(entry.nfs_attr.locs) == 1:
    print "Has one loc, needs a manual review"
    return False

  # check if the first loc has invalid component id.
  rm_loc_list = []
  component_dict = zk_dict["component_dict"]
  for i, loc in enumerate(entry.nfs_attr.locs):
    if str(loc.component_id) not in component_dict.keys():
      print "  removing loc with component_id %d at %d" % (loc.component_id, i)
      rm_loc_list.append(entry.nfs_attr.locs[i])
    else:
      # check for incarnation and operation ids
      print "%s is in valid_component_list" % loc.component_id
      inc_id, op_id = zk_dict["component_dict"][str(loc.component_id)]
      if inc_id < loc.incarnation_id:
        print "  removing loc with incarnation id %d higher than in zk %d" % (loc.incarnation_id, inc_id)
        rm_loc_list.append(entry.nfs_attr.locs[i])
      else:
        print "  nothing to do with this loc"
        print "  inodes incarnation_id:operation_id : %16d:%16d" % (loc.incarnation_id, loc.operation_id)
        print "  zk's incarnation_id:operation_ida  : %16d:%16d" % (inc_id, op_id)

  if not len(rm_loc_list):
    print "removing none; ignoring this inode"
    return False
  if len(rm_loc_list) == len(entry.nfs_attr.locs):
    print "would remove all locs; ignoring this inode"
    return False
  print "removing %d locs" % len(rm_loc_list)
  for loc in rm_loc_list:
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
  zk_dict={}
  zks = ZookeeperSession()
  valid_component_id_list = zks.list('/appliance/logical/clock')
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
    s = zks.get('/appliance/logical/clock/' + comp_id)
    re_pattern = re.compile(r"incarnation_id: (?P<inc_id>\d+)operation_id: (?P<op_id>\d+)")
    d = re_pattern.match(s).groupdict()
    component_dict[comp_id] = (int(d["inc_id"]), int(d["op_id"]))
  zk_dict["component_dict"] = component_dict
  last_flushed = zks.get('/appliance/logical/stargate/nfs_namespace_last_flushed_wali')
  zk_dict["last_flushed"] = int(last_flushed)
  return zk_dict

if __name__ == "__main__":
  log.initialize()
  argv = FLAGS(sys.argv)
  mh = MedusaHelper()
  zk_dict=get_zk_data()
  nfsmaps = mh.scan(mh.kNFSMap, mh.kNFSMapCF)
  inode_list = []
  for key, nfsmap in nfsmaps.iteritems():
    invalid = False
    ctr_dict = zk_dict["ctr_dict"]
    current_epoch = ctr_dict[str(nfsmap.nfs_attr.container_id)].epoch
    inode_id = nfsmap.nfs_attr.inode_id
    if FLAGS.fs_id and FLAGS.fs_id != inode_id.fsid:
      continue
    last_flushed = zk_dict["last_flushed"]
    if nfsmap.nfs_attr.prev_wal_id != -1 and nfsmap.nfs_attr.prev_wal_id > last_flushed or \
       nfsmap.nfs_attr.wal_id != -1 and nfsmap.nfs_attr.wal_id > last_flushed:
      print "inode %d:%d:%d has wal-id greater than last flushed wal-id" % (inode_id.fsid, inode_id.epoch, inode_id.fid)

    component_dict = zk_dict["component_dict"]
    if current_epoch > inode_id.epoch:
      reason = ""
      # we don't validate snapshots
      if inode_id.fsid == 0:
        continue
      for loc in nfsmap.nfs_attr.locs:
        if str(loc.component_id) not in component_dict.keys():
          invalid = True
          reason = "Component id %d not present" % loc.component_id
          break
        inc_id, op_id = zk_dict["component_dict"][str(loc.component_id)]
        if inc_id < loc.incarnation_id :
          invalid = True
          reason = "The loc's incarnation_id is greater: loc: %d vs. %d" % (loc.incarnation_id, inc_id)
        elif inc_id == loc.incarnation_id and op_id < loc.operation_id:
          invalid = True
          reason = "The loc's operation_id is greater: %d vs. %d" % (loc.operation_id, op_id)
      if not invalid:
        #print "%d:%d:%d\t%d-%d\t\t%d\t\t%s|%s
        continue

      if not FLAGS.fix:
        print "========="
        print "current epoch %d : inode_epoch %d" % (current_epoch, inode_id.epoch)
        print "Stale inode: %d:%d:%d" % (inode_id.fsid, inode_id.epoch, inode_id.fid)
        print "========="
        print "DEBUG Reason: %s" % reason
        print nfsmap.nfs_attr
      inode_list.append("%d:%d:%d" % (inode_id.fsid, inode_id.epoch, inode_id.fid))

  if not FLAGS.fix:
    sys.exit(0)

  for inode in inode_list:
    process(inode, zk_dict)
  sys.exit(0)
