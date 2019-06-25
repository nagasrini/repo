import sys
sys.path.append('/home/nutanix/cluster/bin')
import env

from stargate.stargate_interface.stargate_interface_pb2 import *
from stargate.client import StargateClient

stargate_client = StargateClient()

arg = StretchPingRemoteArg()
arg.forward_remote_name = ""
arg.remote_stargate_handle = "172.24.4.39:2009"

try:
  ret = stargate_client.StretchPingRemote(arg)
except Exception as e:
  print "failed with %s " % e
  sys.exit(1)

print "ping succeeded in %s usecs" % ret.forward_latency_usecs
