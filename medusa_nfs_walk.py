import re
import sys, os
sys.path.append('/home/nutanix/cluster/bin')
import env
import gflags
import subprocess
import medusa.medusa_printer_pb2 as medusa_printer_pb2
import util.base.command as command

FLAGS = gflags.FLAGS

small_file_size = 524288

gflags.DEFINE_integer("fs_id", 0, "fs_id part of inode to filter")

fid_size = 64
epoch_size = 32
fsid_size = 32

fid_mask = (1 << fid_size) - 1
epoch_mask = (1 << epoch_size) - 1
fsid_mask = (1 << fsid_size) - 1

def hash(size):
  for n in range(19):
    if not (size-1)>>n:
      return n
  else:
    return 0

fsize_hist = {}
def read_nfs_inodes():
  '''
  Scans the nfsmap through medusa and returns proto of all the nfsmaps.
  Checks if it can modify each row to fix the stale inodes.
  If modified, updates medusa nfsmap.
  '''
  cmd = "medusa_printer --scan --lookup nfs"
  rv, out, err = command.timed_command(cmd, 60)

  inode_list=[]
  if not rv:
    for n in out.split("\n"):
      #print "-----", n
      if "ROW KEY" in n:
        key=n.split("=")[1]
        inode=int(key.split(":")[1])
        fs_id = (inode >> (epoch_size+fid_size)) & fsid_mask
        epoch = (inode >> (fid_size)) & epoch_mask
        fid = inode & fid_mask
        inode_id = "%d:%d:%d" % (fs_id,epoch,fid)
        #print inode_id
        if FLAGS.fs_id != 0 and FLAGS.fs_id != fs_id:
          continue
        inode_list.append(inode_id)
  for inode_id in inode_list:
    outputfile = "./inode_file_%s" % inode_id

    medusa_printer_proto = medusa_printer_pb2.MedusaPrinterProto()
    cmd = "medusa_printer --lookup nfs --nfs_inode_id %s --save_to_file " \
          "--output_file %s --serialization_format=binary" % (inode_id, outputfile)
    #print cmd

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
    if not medusa_printer_proto:
      return {None: False}
    col = medusa_printer_proto.rows[0].columns[0]
    entry = col.nfs_map_entry
    nfs_attr = entry.nfs_attr
    if nfs_attr.fsize > small_file_size:
      continue
    print "##Processing inode %s" % inode_id
    hsh = hash(nfs_attr.fsize)
    n = fsize_hist.get(str(hsh), 0)
    n += nfs_attr.fsize
    fsize_hist[str(hsh)] = n
    print "done"
  return fsize_hist
