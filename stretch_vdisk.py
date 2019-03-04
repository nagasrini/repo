import env
import util.base.log as log
import gflags
from pithos.pithos_pb2 import VDiskConfig
from hyperint.hyperint_cloud.pithos_helper import PithosHelper
from pithos.client.pithos_client import PithosClient
from zeus.configuration import Configuration

try:
  from pithos.pithos_pb2 import StretchParams
except:
  from pithos.stretch_params_pb2 import StretchParams

gflags.DEFINE_string("vdisk_id", 0, "local vdisk_id the metro remote vdisk_id mapping is needed for")

def pclient(host=None):
  cassandra_client = CassandraClient(host, "9161")
  pithos_client = PithosClient(cassandra_client=cassandra_client)
  pithos_client.initialize()
  return pithos_client

def get_vdisks_pithos_helper(container_id, vdisk_id):
  vdisk_list = []
  pithos_helper = PithosHelper()
  container_vdisks_config = pithos_helper.lookup_vdisk_configs_by_container(container_id)
  for vdisk_config_entry in container_vdisks_config:
    if vdisk_id == vdisk_config_entry.data.vdisk_id:
      return vdisk_config_entry
  return None

def get_stretch_params(pclient):
  sps=[]
  for entry in pclient.create_iterator('stretch_params', skip_values=False, consistent=False):
    value=entry[1][1]
    if value is not None and len(value) >= 24:
      s = StretchParams()
      s.ParseFromString(value[24:])
      sps.append(s)
  return sps

# Called with pithos_client connected to arbitrary cassandra client
def get_vdisk_pithos_client(pithos_client, container_id, vdisk_id=0):
  vdisk_list = []
  category_container_id = "vdisk_container_" + str(container_id)
  pithos_entries = pithos_client.create_iterator(category_container_id, skip_values=False, consistent=False)
  if not pithos_entries:
    print "get_vdisk_pithos_client: error"
    return None
  for pe in pithos_entries:
    key, (logical_timestamp, value) = pe
    vdisk_config = None
    if len(value) > 8:
        vdisk_config = VDiskConfig()
        vdisk_config.ParseFromString(value[8:])
    if vdisk_config:
      if not vdisk_id or vdisk_config.vdisk_id == vdisk_id:
        return vdisk_config
  return None

def get_stretch_vdisk_config(pithos_client, vdisk_id):
  sps=get_stretch_params()
  if not len(sps):
    print "No stretch PDs found in Pithos"
    sys.exit(1)
  for sp in sps:
    vdc = get_vdisks_pithos(sp.vstore_id, vdisk_id)
    if not vdc:
      break
  else:
    print "vdisk %d not in any stretch container"
    sys.exit(2)
  return vdc

def main():
  argv = FLAGS(sys.argv)
  if not FLAGS.vdisk_id:
    print "vdisk id required"
    return
  stretch_params = get_stretch_params()
  ctr_sps = []
  for sp in stretch_params:
    ctr_sps.append(sp)
    if FLAGS.container_id:
      if FLAGS.container_id == sp.vstore_id:
        ctr_sps = [sp]
        break
  else:
    if FLAGS.container_id:
      print "ERROR: container %d is not stretched" % FLAGS.container_id
      return
  # search vdisk in specified container or all stretch containers
  local_pithos_client = pclient()
  for sp in ctr_sps:
    vdisk_config = get_vdisk_pithos_client(local_pithos_client, sp.vstore_id, FLAGS.vdisk_id)
      if vdisk_config:
        vdisk_stretched = sp
        break
  # get sp.remote
  if sp.HasField("last_remote_name"):
    print "ERROR: Stretch to %s disabled" % (sp.vstore_id)
    return
  # If local is primary, then take vdisk_name as key to search on remote vdisks
  # config, else take stretch_vdisk_name
  if sp.HasField("forward_remote_name"):
    remote_name = sp.forward_remote_name
    remote_vstore_id = sp.forward_vstore_id
    key_name = vdisk_config.stretch_vdisk_name
    remote_key_field = "vdisk_name"
  elif sp.HasField("replicate_remote_name"):
    remote_name = sp.replicate_remote_name
    remote_vstore = sp.replicate_vstore_id
    key_name = vdisk_config.vdisk_name
    remote_key_field = "stretch_vdisk_name"

  zcp=Configuration().initialize().config_proto()
  for rs in zcp.remote_site_list:
    if rs.remote_name == remote_name:
      remote_ip_list = rs.remote_cerebro_ip_list
      break
  for remote_ip in remote_ip_list:
    remote_pclient = pclient(remote_ip)
    if remote_pclient:
      break
  remote_vdisk_config = get_vdisk_pithos_client(remote_pithos_client, remote_vstore)
  if not remote_vdisk_config:
    print "ERROR: failed to get stretch vdisk config"
    return
  for v in remote_vdisk_config:


      
  if __name__ == "__main__":
    main()
