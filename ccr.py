import sys
sys.path.append("/home/nutanix/serviceability/bin")
import env

from acropolis.acropolis_interface_pb2 import VmGetArg
from acropolis.client import AcropolisClient
import gflags

import requests
import json
import multiprocessing as mp
import util.base.log as log

gflags.DEFINE_string("pd", "", "Name of PD")
gflags.DEFINE_string("ip", "", "IP")
gflags.DEFINE_string("snap", "", "ref_snap id")
gflags.DEFINE_string("ref_snap", "", "ref_snap id")
FLAGS = gflags.FLAGS

IP="10.48.64.130"
URL="https://%s:9440/api/nutanix/v3/data/changed_regions" % IP

SNAP_PATH="/tctr/.snapshot/38/5041124608852155971-1576046469291271-138/.acropolis/vmdisk/"
REFSNAP_PATH="/tctr/.snapshot/38/5041124608852155971-1576046469291271-138/.acropolis/vmdisk/"


from util.base.types import NutanixUuid

client = AcropolisClient()

def get_all_vms_id():
  vm_to_vm_uuid = {}
  for vm_info in client.vm_get_iter():
    vm_to_vm_uuid[vm_info.config.name] = NutanixUuid(vm_info.uuid)
  return vm_to_vm_uuid

VMDISK_UUIDS='141f424e-2f10-4915-aa2e-32ef4795a7c9'
REFVMDISK_UUIDS='240254b9-aef6-4c58-a75c-8cc1910af133'

def Issue_CCR(path, refpath, start, end):
    auth = ('admin', 'Nutanix.123')
    data_dict = {
             "end_offset" : end,
             "start_offset" : start,
             "reference_snapshot_file_path": refpath,
             "snapshot_file_path": path
    }
    data = json.dumps(data_dict)
    r = requests.post(url, data=data, auth=auth)
    log.INFO("sent request")

def main():
    log.initialize()
    argv = FLAGS(sys.argv)
    for vmdisk_uuid in VMDISK_UUIDS:
        rng=range(0,1099511627776L,1048576)
        for i in range(0,len(rng)):
            if i == len(rng):
                break
            start = rng[i]
            end = rng[i + 1]
            path =  SNAPSHOT_PATH + VMDISK_UUIDS
            refpath = REFSNAP_PATH + REFVMDISK_UUIDS
            name = str(vm_uuid) + "_" + str(start) + "_" + str(end)
            p = mp.Process(target=Issue_CCR, name=name, args=(path, refpath, start, end))
            #Issue_CCR(path, start, end)
