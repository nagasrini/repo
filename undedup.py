import sys, os
sys.path.append('/home/nutanix/cluster/bin')
import env

from pithos.client.pithos_client import PithosClient
pithos_client = PithosClient()
pithos_client.initialize()
import medusa.medusa_printer_pb2 as medusa_printer_pb2
import util.base.command as command
import gflags

from uhura.client.client import *
from uhura.uhura_interface_pb2 import VmGetArg as UhuraVmGetArg
from util.base.types import NutanixUuid

gflags.DEFINE_boolean("dry_run", True, "If Update is a Dry Run?")
FLAGS = gflags.FLAGS

uhura_client = UhuraClient()

def uhura_list_vm_registered():
  vm_to_vdisk_map = {}
  vm_get_arg = UhuraVmGetArg()
  vm_get_arg.include_vdisk_config = True
  vm_get_ret = uhura_client.VmGet(vm_get_arg)
  for vm_info in vm_get_ret.vm_info_list:
    vdisk_list = []
    for disk in vm_info.config.disk_list:
      if disk.HasField("disk_addr") and disk.disk_addr:
        if disk.disk_addr.HasField("vmdisk_uuid") and disk.disk_addr.vmdisk_uuid:
          vdisk_list.append(str(NutanixUuid(disk.disk_addr.vmdisk_uuid)))
    vm_to_vdisk_map[vm_info.config.name] = vdisk_list
  return vm_to_vdisk_map

def get_all_leafvdisks():
  entries = pithos_client.create_iterator('vdisk_id', skip_values=False,
                                          consistent=False)
  vdisk_list = []
  for entry in entries:
    vdisk_config = pithos_client.entry_to_vdisk_config(entry)
    if not vdisk_config:
      continue
    if vdisk_config.HasField('to_remove') and vdisk_config.to_remove == True:
      continue
    if (not vdisk_config.HasField('mutability_state')
        or vdisk_config.mutability_state == 0):
      vdisk_list.append(vdisk_config)
  return vdisk_list

def dedup_vblocks(vdisk_id):
  mp_vblock_proto = medusa_printer_pb2.MedusaPrinterProto()
  outfile = "/tmp/vblock.txt"
  cmd = "medusa_printer -lookup vblock --scan --vdisk_id %d " % vdisk_id
  cmd += "--print_values_for_scan --save_to_file --output_file %s" % (outfile)
  #print cmd
  rv, out, err = command.timed_command(cmd)
  if rv:
    print "error scanning vblocks"
    print err
    sys.exit(0)
  if "negative cache value" in out:
    print "negative cache value"
    return False
  count = 0
  with open(outfile) as file:
    for line in file:
      if "sha1_hash" in line:
        count += 1
  if count:
    print "dedup regions: vdisk_id: %d; count: %d" % (vdisk_id, count)
  return count

def undedup(vdisk_id):
  outfile="/home/nutanix/analysis/vdisk_manipulator_undedup_%d" % vdisk_id
  cmd = "vdisk_manipulator -operation undedupe_vdisk  -nostats_only"
  cmd += " -vblock_fragmentation_threshold 0 -vblock_fragmentation_type regions"
  cmd += " -vdisk_id %d" % vdisk_id

  print cmd
  if FLAGS.dry_run:
    print "Dry_run: Not executing the command"
    return
  rv, out, err = command.timed_command(cmd)
  if rv:
    print "vdisk_manipulator failed %d" % rv
    print "Error: %s" % err
    print out
  return

def process(vdisk_list, vm_map):
  vdisk_visited = vdisk_deduped = 0
  #for each vdisk, run vblock scan and check if dedup
  for vdisk in vdisk_list:
    vm_name = "<Not Found>"
    for k,v in vm_map.iteritems():
      if vdisk.nfs_file_name in v:
        vm_name = k
        break
    vdisk_id = vdisk.vdisk_id
    vdisk_visited += 1
    print "Working to undedup: VM %s; vdisk name %s; vdisk id %d" % (vm_name,vdisk.nfs_file_name,vdisk_id)
    if dedup_vblocks(vdisk_id):
      vdisk_deduped += 1
      undedup(vdisk_id)
  dstr = datetime.datetime.now().strftime("%d-%m-%Y %H:%M:%S.%f")
  print "%s vdisks reviewed: %d; vdisks undeduped: %d" % (dstr, vdisk_visited,vdisk_deduped)

if __name__ == "__main__":
  argv = FLAGS(sys.argv)
  vdisk_list = get_all_leafvdisks()
  map = uhura_list_vm_registered()
  process(vdisk_list, map)