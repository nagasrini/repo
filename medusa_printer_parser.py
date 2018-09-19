import sys
sys.path.append('/home/nutanix/cluster/bin')
import env
import gflags
import subprocess
import medusa.medusa_printer_pb2 as medusa_printer_pb2
from zeus.zookeeper_session import ZookeeperSession
import util.base.command as command
import re

num_data_shards = 0

gflags.DEFINE_string("inode_id", "", "nfs inode_id")
gflags.DEFINE_string("inode_id_file", "", "File containing nfs inode_ids one per line")
gflags.DEFINE_boolean("dry_run", True, "If Update is a Dry Run?")

FLAGS = gflags.FLAGS

valid_component_id_list=[]

def read_nfs_map_to_file(inode_id, outputfile):
  global num_data_shards
  medusa_printer_proto = medusa_printer_pb2.MedusaPrinterProto()
  cmd = "medusa_printer --lookup nfs --nfs_inode_id %s --save_to_file " \
        "--output_file %s --serialization_format=binary" % (inode_id, outputfile)
  print cmd

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

  return medusa_printer_proto

from zeus.configuration import Configuration
def modify(medusa_printer_proto, debug_in_file, debug_out_file, inputfile, ctr_dict):
  '''
  modify the proto as per the condition and
  write it to a binary file for the consumption of medusa_printer --update
  '''
  if not medusa_printer_proto:
    return False
  if debug_in_file:
    open(debug_in_file, "w").write(str(medusa_printer_proto))
  col = medusa_printer_proto.rows[0].columns[0]
  entry = col.nfs_map_entry

  current_epoch =  ctr_dict[str(entry.nfs_attr.container_id)].epoch
  if current_epoch <= entry.nfs_attr.inode_id.epoch:
    print "Not doing anything as inode's epoch is not older than that of container"
    return

  if len(entry.nfs_attr.locs) == 1:
    print "Has one loc, needs a manual review"
  else:
    # check if the first loc has invalid component id.
    #print valid_component_id_list
    zks = ZookeeperSession()
    value_list = []
    for i, loc in enumerate(entry.nfs_attr.locs):
      if str(loc.component_id) not in valid_component_id_list:
        print "  removing loc with component_id %d at %d" % (loc.component_id, i)
        #del entry.nfs_attr.locs[i]
        value_list.append(entry.nfs_attr.locs[i])
      else:
        print "%s is in valid_component_list" % loc.component_id
        # check for incarnation and operation ids
        s = zks.get('/appliance/logical/clock/' + str(loc.component_id))
        d = re.compile(r"incarnation_id: (?P<inc_id>\d+)operation_id: (?P<op_id>\d+)").match(s).groupdict()
        if int(d["inc_id"]) < loc.incarnation_id:
          print "  removing loc with incarnation id %d higher than in zk %s" % (loc.incarnation_id, d["inc_id"])
          #del entry.nfs_attr.locs[i]
          value_list.append(entry.nfs_attr.locs[i])
        else:
          print "  nothing to do with this loc"

    if not len(value_list):
      print "removing none"
      return False
    if len(value_list) == len(entry.nfs_attr.locs):
      print "would remove all locs"
      print "ignoring the op"
      return False
    print "removing %d locs" % len(value_list)
    for val in value_list:
      entry.nfs_attr.locs.remove(val)

    print "resulting locs of size %d" % len(entry.nfs_attr.locs)

  col.timestamp += 1

  #debugging
  if debug_out_file:
    open(debug_out_file, "w").write(str(medusa_printer_proto))

  if not inputfile:
    return False
  open(inputfile, "w").write(medusa_printer_proto.SerializeToString())

  return True

def execute(inputfile, inode_id, dry=True):
  cmd = "medusa_printer --lookup nfs --nfs_inode_id %s " \
        "--input_file %s --serialization_format=binary " \
        "--update " % (inode_id, inputfile)
  if num_data_shards:
    cmd += " --nfs_num_data_shards %d" % num_data_shards
  if dry:
    print "Dry Run. Would otherwise:"
    print cmd
    return
  print "Executing..."
  rv, _, _ = command.timed_command(cmd, 30)
  print "Done"

if __name__ == "__main__":
  argv = FLAGS(sys.argv)
  if not FLAGS.inode_id and not FLAGS.inode_id_file:
    print "inode_id is needed"
    sys.exit(0)
  zks = ZookeeperSession()
  valid_component_id_list = zks.list('/appliance/logical/clock')
  if not len(valid_component_id_list):
    print "Couldn't get component ids"
    sys.exit(0)

  if FLAGS.inode_id:
    list = [FLAGS.inode_id]
  else:
    try:
      f = open(FLAGS.inode_id_file, "r")
    except IOError as e:
      print "Error opening file %s: %s" % (FLAGS.inode_id_file, e)
      sys.exit(1)
    list = [n.replace("\n", "") for n in f]

  zcp=Configuration().initialize().config_proto()
  ctr_list = zcp.container_list
  ctr_dict={}
  for ctr in zcp.container_list:
    ctr_dict[str(ctr.container_id)] = ctr

  for inode_id in list:
    outputfile = "./inode_file_%s" % inode_id
    inputfile = "./inode_file_modified_%s" % inode_id
    debug_in_file = "./debug_proto_in_str_%s" % inode_id
    debug_out_file = "./debug_proto_out_str_%s" % inode_id
    proto = read_nfs_map_to_file(inode_id, outputfile)
    ret = modify(proto, debug_in_file, debug_out_file, inputfile, ctr_dict)
    if ret:
      execute(inputfile, inode_id, FLAGS.dry_run)
