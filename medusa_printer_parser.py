import sys
sys.path.append('/home/nutanix/cluster/bin')
import env
import gflags
import subprocess
import medusa.medusa_printer_pb2 as medusa_printer_pb2
from zeus.zookeeper_session import ZookeeperSession
import util.base.command as command

num_data_shards = 0

gflags.DEFINE_string("inode_id", "", "nfs inode_id")

FLAGS = gflags.FLAGS

valid_component_id_list=[]

def read_nfs_map_to_file(inode_id, inputfile):
  global num_data_shards
  medusa_printer_proto = medusa_printer_pb2.MedusaPrinterProto()
  cmd = "medusa_printer --lookup nfs --nfs_inode_id %s --save_to_file " \
        "--output_file %s --serialization_format=binary" % (inode_id, inputfile)

  print cmd

  rv, _, _ = command.timed_command(cmd, 60)
  if rv:
    print "error looking up inode %s" % inode_id
    sys.exit(0)
  medusa_printer_proto.ParseFromString(open(inputfile, "r").read())
  nfs_map = medusa_printer_proto.rows[0].columns[0].nfs_map_entry
  num_data_shards = nfs_map.nfs_attr.num_data_shards

  if num_data_shards:
    cmd += "--nfs_num_data_shards %d" % num_data_shards
    rv, _, _ = command.timed_command(cmd, 60)
    medusa_printer_proto.ParseFromString(open(inputfile, "r").read())

  return medusa_printer_proto

def modify(medusa_printer_proto, debugfile, output):
  '''
  modify the proto as per the condition and
  write it to a binary file for the consumption of medusa_printer --update
  '''
  col = medusa_printer_proto.rows[0].columns[0]
  entry = col.nfs_map_entry
  if len(entry.nfs_attr.locs) == 1:
    print "Has one loc"
  else:
    # check if the first loc has invalid component id.
    print valid_component_id_list
    for i, loc in enumerate(entry.nfs_attr.locs):
      if str(loc.component_id) not in valid_component_id_list:
        print "removing loc with component_id %d at %d" % (loc.component_id, i)
        del entry.nfs_attr.locs[i]
      else:
        break

  col.timestamp += 1

  #debugging
  if debugfile:
    open(debugfile, "w").write(str(medusa_printer_proto))

  if output:
    open(output, "w").write(medusa_printer_proto.SerializeToString())

def execute(inputfile, inode_id, Dry=True):
  cmd = "medusa_printer --lookup nfs --nfs_inode_id %s " \
        "--input_file %s --serialization_format=binary" \
        "--update " % (inode_id, inputfile)
  if num_data_shards:
    cmd += "--nfs_num_data_shards %d" % num_data_shards
  if dry:
    print cmd
    return
  rv, _, _ = command.timed_command(cmd, 30)

if __name__ == "__main__":
  argv = FLAGS(sys.argv)
  zks = ZookeeperSession()
  valid_component_id_list = zks.list('/appliance/logical/clock')
  if not len(valid_component_id_list):
    print "Couldn't get component ids"
    sys.exit(0)
  proto = read_nfs_map_to_file(FLAGS.inode_id, "./inode_file")
  modify(proto, "./debug_proto_str", "./inode_file_modified")
  #execute("./inode_file_modified", FLAGS.inode_id, True)
