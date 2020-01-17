import sys
sys.path.append('/home/nutanix/cluster/bin')
import env
import gflags

# Issues NfsLWSClearStagingArea RPC to stargate and cleans up tmp_staging area left behind by aborted/failed
# hydrate/restore ops.
# Requires a Stargate CG Id (Cerebro Session Id) and the prefix path

try:
  from stargate.stargate_interface.stargate_interface_pb2 import *
  from stargate.client import StargateClient
except:
  from cdp.client.stargate.stargate_interface.stargate_interface_pb2 import *
  from cdp.client.stargate.client import StargateClient

gflags.DEFINE_string("cg_id", "", "cg_id corresponding to the path removed in a:b:c format")
gflags.DEFINE_string("prefix", "", "prefix of the path removed in /.snapshot/tmp_staging/[x%100]/x-y-z/ format")
FLAGS = gflags.FLAGS


def Do_RPC():
  stargate_client = StargateClient()

  arg = NfsLWSClearStagingAreaArg()
  arg.staging_area_path_prefix = FLAGS.prefix
  arg.cg_id.originating_cluster_id = int(FLAGS.cg_id.split(':')[0])
  arg.cg_id.originating_cluster_incarnation_id = int(FLAGS.cg_id.split(':')[1])
  arg.cg_id.id = int(FLAGS.cg_id.split(':')[2])

  try:
    ret = stargate_client.NfsLWSClearStagingArea(arg)
  except Exception as e:
    print "RPC failed with %s " % e
    sys.exit(1)

  print "RPC succeeded "

if __name__ == "__main__":
  argv = FLAGS(sys.argv)
  Do_RPC()