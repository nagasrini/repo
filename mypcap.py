import sys
import socket
import struct

sys.path.append('/home/nutanix/cluster/bin')
import env
from util.sl_bufs.net.rpc_pb2 import RpcRequestHeader, RpcResponseHeader
from cerebro.interface.cerebro_interface_pb2 import *

import util.base.log as log

rpc_map = {
            'FetchReplicationTarget' : (FetchReplicationTargetArg(), FetchReplicationTargetRet),
            "SyncDataStats": (SyncDataStatsArg(), SyncDataStatsRet()),
            "QueryRemoteClusterStatus": (QueryRemoteClusterStatusArg(), QueryRemoteClusterStatusRet()),
          }

rpc_id_map = {}

filter_ip = "10.48.64.200"
filter_port = 2020

#s = socket.socket(socket.AF_INET,socket.SOCK_RAW,socket.IPPROTO_IP)
#s = socket.socket(socket.AF_INET,socket.SOCK_RAW,socket.IPPROTO_TCP)

def get_ip_header(data):
  ip_header = {}
  iph = struct.unpack("!BBHHHBBH4s4s", data)
  ip_header['version'] = iph[0] >> 4
  ip_header['hlen'] = iph[0] & 0xf
  ip_header['tos'] = iph[1]
  ip_header['total_len'] = iph[2]
  ip_header['id'] = iph[3]
  ip_header['frag_off'] = iph[4]
  ip_header['ttl'] = iph[5]
  ip_header['proto'] = iph[6]
  ip_header['hcksum'] = iph[7]
  ip_header['dip'] = socket.inet_ntoa(iph[8])
  ip_header['sip'] = socket.inet_ntoa(iph[9])
  return ip_header

def get_tcp_header(data):
  tcp_header = {}
  tcph = struct.unpack("!HHLLBBHHH", data)
  tcp_header['sport'] = tcph[0]
  tcp_header['dport'] = tcph[1]
  tcp_header['seqno'] = tcph[2]
  tcp_header['ackno'] = tcph[3]
  tcp_header['hlen'] = tcph[4] >> 2 #6bits of 8bits
  tcp_header['wsize'] = tcph[7]
  tcp_header['hcksum'] = tcph[7]
  return tcp_header

def get_ether_frame(data):
  ether_frame = {}
  ef = struct.unpack("!6s6sH", data)
  ether_frame['dmac'] = ef[0]
  ether_frame['smac'] = ef[1]
  ether_frame['etyp'] = ef[2]
  return ether_frame


def read_one(sock, ip=None, port=None):
  while True:
    pbuf=sock.recvfrom(65536)
    #print pbuf
    buf = pbuf[0][14:]
    ether_frame = get_ether_frame(pbuf[0][:14])
    if ether_frame['etyp'] != 0x800:
      continue
    ip_header = get_ip_header(buf[0:20])
    #print ip_header
    tcp_header = get_tcp_header(buf[20:40])
    #print tcp_header
    offset = (ip_header['hlen'] * 4) + tcp_header['hlen']
    if offset >= len(buf):
      #print "DEBUG: Skip: no tcp payload %d" % len(buf)
      continue
    if not port or tcp_header['sport'] == port or tcp_header['dport'] == port:
      print "--> %s:%s %s:%s" % (ip_header['sip'],tcp_header['sport'], ip_header['dip'],tcp_header['dport'])
      if not ip or ip_header['sip'] == ip or ip_header['dip'] == ip:
        break
    #print "DEBUG: Skip: filtered"
  return buf, ip_header, tcp_header

def parse_http(data):
  # HTTP
  lst = data.split('\n')
  content_len = 0
  content = None
  content_type = None
  for i, line in enumerate(lst):
    #print "DEBUG " + line
    if "Content-Length" in line:
      content_len = line.strip('\r').split()[1]
      continue
    if "Content-Type" in line:
      content_type = line.strip('\r').split()[1]
      continue
    if line == '\r':
      content = lst[i+1]
      break
  return content_len, content, content_type

# data is HTTP content
def parse_rpc(data, type):
  # looking for rpc header (req or res)
  # header size is in first 4 bytes
  rpc_len,  = struct.unpack("!I", data[0:4])
  #print "DEBUG: rpc_len %d" % rpc_len
  rpc_header = None
  arg_proto = None
  if rpc_len < 4:
    print "ERROR: rpc_len less than 4"
    return None
  rpc_header = RpcRequestHeader() if type == 1 else RpcResponseHeader()
  rpc_header.ParseFromString(data[4:rpc_len+4])
  if not rpc_header:
    print "ERROR: serializing RPC failed"
    return None, None
  if type == 1:
    rpc_method_name = rpc_header.method_name
    arg_proto, _ = rpc_map.get(rpc_method_name, (None, None))
    rpc_id_map[rpc_header.rpc_id] = rpc_method_name
  else:
    rpc_method_name = rpc_id_map.get(rpc_header.rpc_id, None)
    if rpc_method_name:
      _, arg_proto = rpc_map.get(rpc_method_name, (None,None))
  if arg_proto:
    arg_proto.ParseFromString(data[4+rpc_len:])
  return rpc_method_name, rpc_header, arg_proto

def scan(sock):
  while True:
    print "####"
    d, i ,t = read_one(sock, ip=filter_ip, port=filter_port)
    print "#####INFO: source: %s:%s, dest: %s:%s" % (i['sip'], t['sport'], i['dip'], t['dport'])
    type = 1 if t['dport'] == filter_port else 2 # 1 is Req and 2 is Res
    if type == 1:
      print "###########INFO Request"
    else:
      print "###########INFO Response"
    offset = (i['hlen'] * 4) + t['hlen']
    plen, payload, ptype = parse_http(d[offset:])
    if not plen or not payload:
      continue
    #print "DEBUG: %s" % ptype
    if ptype and not "x-rpc" in ptype:
      continue
    rpc_method_name, rpc_header, arg_proto = parse_rpc(payload, type)
    if rpc_header:
      print "##########INFO rpc id : %d" % rpc_header.rpc_id
      print "##########INFO rpc Method: %s" % (rpc_method_name)
      print "##########INFO rpc payload %s" % arg_proto


def do_it(sock):
  d, i ,t = read_one(sock, filter_ip, filter_port)

  offset = (ip_header['hlen'] * 4) + tcp_header['hlen']
  print "INFO: source: %s:%s, dest: %s:%s" % (i['sip'], t['sport'], i['dip'], t['dport'])
  #print "offset %s length %s" % (offset, len(d))
  type = 1 if t['dport'] == 2020 else 2 # 1 is Req and 2 is Res
  plen, payload = parse_http(d[offset:])
  if not plen or not payload:
    print "No HTTP Payload"
  rpc_header, arg_proto = parse_rpc(payload, type)
  if not rpc_header:
    return None, None
  if rpc_header.HasField("method_name"):
    print "RPC: Method: %s" % (rpc_header.method_name)
  return rpc_header, arg_proto

if __name__ == "__main__":
  #s = socket.socket(socket.AF_INET,socket.SOCK_RAW,socket.IPPROTO_TCP)
  s = socket.socket(socket.AF_PACKET,socket.SOCK_RAW,socket.htons(0x3))
  s.bind(('eth0',0))
  scan(s)
