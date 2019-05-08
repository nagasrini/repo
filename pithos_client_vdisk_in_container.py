import env
import util.base.log as log
import gflags
from pithos.client.pithos_client import PithosClient

from pithos.pithos_pb2 import VDiskConfig
try:
  from pithos.pithos_pb2 import StretchParams
except:
  from pithos.stretch_params_pb2 import StretchParams
from cassandra.cassandra_client.client import CassandraClient

# Called with pithos_client connected to arbitrary cassandra client
def get_vdisk_pithos_client(pithos_client, container_id, vdisk_id):
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
    print "v %d" % vdisk_config.vdisk_id
    if vdisk_config and vdisk_config.vdisk_id == int(vdisk_id):
      return vdisk_config
  return None

def pclient(host):
  cassandra_client = CassandraClient(host, "9161")
  pithos_client = PithosClient(cassandra_client=cassandra_client)
  pithos_client.initialize()
  return pithos_client

if __name__ == "__main__":
  pc = pclient("10.48.64.100")
  vc=get_vdisk_pithos_client(pc, "111796", "114440")
  print vc
