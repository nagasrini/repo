import os, sys
import env
from tools.vdisk_usage_printer.vdisk_usage_pb2 import VdiskUsageProto

import util.base.command as command

output_file = "/tmp/vup.proto"

def vdisk_usage_printer(vdisk_id):
  cmd = "/usr/local/nutanix/bin/vdisk_usage_printer --vdisk_id %d" % vdisk_id
  cmd += " -output_type proto -output_file_name %s" % output_file
  print cmd
  rv, out, err = command.timed_command(cmd, 60)
  if rv:
    print out
    sys.exit(1)
  vup_proto = VdiskUsageProto()
  vup_proto.ParseFromString(open(output_file).read())
  os.unlink(output_file)
  return vup_proto
