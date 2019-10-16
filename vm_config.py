import env
from acropolis.acropolis_interface_pb2 import VmGetArg
from acropolis.client import AcropolisClient
from util.base.types import NutanixUuid

client = AcropolisClient()


def get_all_vms():
  vm_to_vdisk_map = {}
  for vm_info in client.vm_get_iter():
    vdisk_list = []
    print vm_info.config.disk_list
    for disk in vm_info.config.disk_list:
      if disk.HasField("vmdisk_uuid") and disk.vmdisk_uuid:
        vdisk_list.append(NutanixUuid(disk.vmdisk_uuid))
    vm_to_vdisk_map[vm_info.config.name] = vdisk_list
  return vm_to_vdisk_map

from hyperint.client.hyperint_interface_client import HyperintInterfaceClient
from hyperint.hyperint_pb2 import GetVmInfoArg, GetVmInfoRet

HyperintRpcClient = HyperintInterfaceClient()

def list_vm_registered():
  map = {}
  get_vm_info_arg = GetVmInfoArg()
  get_vm_info_arg.include_vm_config = True
  try:
    get_vm_info_ret = HyperintRpcClient.GetVmInfo(get_vm_info_arg)
  except HyperintInterfaceError as e:
    log.ERROR("Gettig VM information %s failed: %s" % vm_name, e)
  for result in get_vm_info_ret.vm_info_results:
    if (not result.HasField('error') or result.error == GetVmInfoRet.kNoError):
      list = []
      for vm_disk in result.vm_info.vm_config.virtual_disks:
        list.append(vm_disk.uuid)
      map[result.vm_info.display_name] = list
  return map

if __name__ == "__main__":
    map = get_all_vms()
    print map
    print "======="
    map=list_vm_registered()
    print map