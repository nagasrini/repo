import sys, os
from datetime import datetime
import struct
import socket
sys.path.append("/home/nutanix/serviceability/bin")
import env
import gflags
from util.sl_bufs.net.rpc_pb2 import RpcRequestHeader, RpcResponseHeader, \
     RpcBinaryLogRecordMetadata
from util.storage.binary_log import BinaryLogRecordMetadata

# rpc protos
from cerebro.interface.cerebro_interface_pb2 import *
from cassandra.cassandra_client.cassandra_pb2 import *

gflags.DEFINE_string("path", "", "binary log file path")

FLAGS = gflags.FLAGS

class ty():
    REQ = 1
    RES = 2

rpc_id_map = {}

def debug_print(msg):
  if FLAGS.debug:
    print msg

def dateobj(ts):
  return datetime.strptime(datetime.fromtimestamp(ts/1000000).ctime() + \
                           " %s" % (ts % 1000000), "%c %f")
def ip_str(ip):
  #this should work too: socket.inet_ntoa(struct.pack("<L",i))
  ip_h = socket.ntohl(ip)
  l = [str((ip_h >> (i*8)) & 0xff) for i in range(3,-1,-1)]
  return '.'.join(l)

def get_protobuf(data, proto_name):
  try:
    proto = getattr(sys.modules[__name__], proto_name.split('.')[-1])
    if not proto:
      print "ERROR: No proto obj imported or present"
      return None
    proto_obj = proto()
    proto_obj.ParseFromString(data)
  except Exception as e:
    print "ERROR: deserializing proto %s: %s" % (proto_name, e)
    return None
  return proto_obj

class UserRecord():
  def __init__(self, ur, fh):
    global proto_map
    self.ts = ur.time_usecs
    self.fh = fh
    self.pobjs = {}
    self.out = {}
    self.is_pending = False
    self.elapsed_time = None
    self.req_urec = None
    for comp in ur.component:
      proto_name = proto_map.get(comp.protobuf_id)
      comp_buf = self.fh.read(comp.len)
      debug_print("%d -> %s" % (comp.protobuf_id, proto_name))
      proto_obj = get_protobuf(comp_buf, proto_name)
      if not proto_obj:
        debug_print("### ERROR: reading proto %s" % proto_name)
        continue
      self.pobjs[proto_name] = proto_obj
      if proto_name in ["RpcRequestHeader", "RpcResponseHeader"]:
        self.out["rpc"] = handle_rpc_header(proto_obj)
      elif proto_name in "RpcBinaryLogRecordMetadata":
        self.out["rpc_binary"] = handle_rpc_binary_record(proto_obj)
      else:
        self.out["proto"] = self.handle_other_proto(proto_name, proto_obj)

  def handle_rpc_header(self, proto_obj):
    self.req_ur = None
    self.rpc_header = proto_obj
    #pending response
    self.is_pending = "Request" in rpc_header.__class__.__name__
    debug_print("<=========\nself.rpc_header\n=========>")
    if not self.is_pending:
      self.req_urec = pc_id_map.get(self.rpc_header.rpc_id, None)
      if not self.req_urec: # request for this response not found
        self.is_pending = True
      self.elapsed_time = self.ts - req_urec.time_usecs
      req_proto_name = req_rpc_header.method_name
      rh_out += " " + " %s { %s } ->" % (req_proto_name, req_proto_obj)
    rh_out += "%s" % req_rpc_header
    return rh_out.replace("\n", " ")

  def handle_other_proto(self, pname, pobj):
    proto_out = " %s { %s }" % (pname, pobj)
    return proto_out.replace("\n", " ")

  def handle_rpc_binary_record(self, pobj):
    rpc_out = "[ local %s:%s" % (ip_str(pobj.local_ip), pobj.local_port)
    rpc_out += " remote %s:%s ]" % (ip_str(pobj.remote_ip), pobj.remote_port)
    return rpc_out

  def myprint(self):
    ts = "%s" % dateobj(self.ts).strftime("%m%d%Y %H:%M:%S.%f")
    et = " + %d" % self.elapsed_time if self.elapsed_time else ""
    print "%s%s %s " % (ts, self.elapsed_time, self.out)

proto_map = {}
def read_one_record(f):
  global  proto_map
  szbuf = f.read(4)
  if not szbuf:
    return None
  len = struct.unpack("I", szbuf)[0]
  blr = BinaryLogRecordMetadata()
  blrbuf = f.read(len)
  blr.ParseFromString(blrbuf)

  proto_map = {id:name for id, name in zip(blr.protobuf_id,blr.protobuf_name)}
  debug_print(proto_map)
  for ur in blr.user_record:
    urec = UserRecord(ur, f)
    if urec.is_pending:
      rpc_id_map[urec.rpc_header.rpc_id] = urec
    else:
      urec.myprint()
      if urec.req_urec:
        del rpc_id_map[urec.req_urec.rpc_header.rpc_id]

def main():
  f  = open(FLAGS.path)
  while True:
    magic_x = read_one_record(f)
    if not magic_x:
      return None

"""
def read_one_record(f):
  szbuf = f.read(4)
  if not szbuf:
    return None
  len = struct.unpack("I", szbuf)[0]
  blr = BinaryLogRecordMetadata()
  blrbuf = f.read(len)
  blr.ParseFromString(blrbuf)

  proto_map = {id:name for id, name in zip(blr.protobuf_id,blr.protobuf_name)}
  for ur in blr.user_record:
    time_out = "%s" % dateobj(ur.time_usecs).strftime("%m%d %Y %H:%M:%S.%f")
    out = ""
    elapsed_time = ""
    pending = False
    for comp in ur.component:
      proto_name = proto_map.get(comp.protobuf_id)
      comp_buf = f.read(comp.len)
      #print "%d %s" % (comp.protobuf_id, proto_name)
      #print "len %d" % comp.len
      if "RpcRequestHeader" in proto_name or "RpcResponseHeader" in proto_name:
        req_ur = None
        rpc_header = get_protobuf(data, name)
        if not rpc_header:
          debug_print("### ERROR: reading rpc header")
          continue
        debug_print("<=========\nrpc_header\n=========>")
        # If this is a response, get the request information from the past.
        if rpc_header.__class__.__name__ == "RpcResponseHeader":
          req_rpc_header, req_ur, req_proto_obj = rpc_id_map.get(rpc_header.rpc_id, (None, None, None))
        if not req_ur:
          pending = True
        else:
          elapsed_time = ur.time_usecs - req_ur.time_usecs
          rh_out = "%s" % req_rpc_header
          out += " " + rh_out.replace("\n", " ")
          req_proto_name = req_rpc_header.method_name
          out += " " + " %s { %s } ->" % (req_proto_name, req_proto_obj)
      proto_obj = get_protobuf(comp_buf, proto_name)
      if "RpcBinaryLogRecordMetadata" in proto_name:
        rpc_out += " [ local %s:%s" % (ip_str(proto_obj.local_ip), proto_obj.local_port)
        rpc_out += " remote %s:%s ]" % (ip_str(proto_obj.remote_ip), proto_obj.remote_port)
        continue
      proto_out = " %s { %s }" % (proto_name, proto_obj)
      out += proto_out.replace("\n", " ")
      rpc_id_map[rpc_header.rpc_id] = (rpc_header, user_record)
    if not pending:
      print "%s + %s %s" % (time_out, elapsed_time, out.lstrip())
  magic = f.read(4)
  magic_x = hex(struct.unpack("I", magic)[0])
  if magic_x != 0xcabaddee:
    print "ERROR: Wrong magic %s:" % magic_x
    return None
  return magic_x
"""

if __name__ == "__main__":
  argv = FLAGS(sys.argv)
  main()
