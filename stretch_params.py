import os
import sys
for path in os.listdir("/usr/local/nutanix/lib/py"):
        sys.path.insert(0, os.path.join("/usr/local/nutanix/lib/py", path))
from pithos.client.pithos_client import PithosClient
from pithos.pithos_pb2 import StretchParams

pithos_client = PithosClient()
pithos_client.initialize()

s=[]
for entry in pithos_client.create_iterator('stretch_params', skip_values=False, consistent=False):
  #s.append(pithos_client.entry_to_stretch_params(e))
  value=entry[1][1]
  #print entry
  if value is not None and len(value) >= 24:
    s = StretchParams()
    s.ParseFromString(value[24:])

print s
