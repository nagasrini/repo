#
# nagaraj.srinivasa@nutanix.com
#
import sys
import socket
import struct

sys.path.append('/home/nutanix/cluster/bin')
import env
from util.sl_bufs.net.rpc_pb2 import RpcRequestHeader, RpcResponseHeader
from cerebro.interface.cerebro_interface_pb2 import *

import util.base.log as log

rpc_map = {
            'FetchReplicationTarget' : (FetchReplicationTargetArg(), FetchReplicationTargetRet()),
            "SyncDataStats": (SyncDataStatsArg(), SyncDataStatsRet()),
            "QueryRemoteClusterStatus": (QueryRemoteClusterStatusArg(), QueryRemoteClusterStatusRet()),
            "ProtectSnapshotHandlesOnRemote": (ProtectSnapshotHandlesOnRemoteArg(), ProtectSnapshotHandlesOnRemoteRet()),
            "TransferConsistencyGroupMetadata": (TransferConsistencyGroupMetadataArg(), TransferConsistencyGroupMetadataRet()),
          }

def cerebro_rpc_obj(rpc_name, typ):
  import cerebro.interface.cerebro_interface_pb2
  suffix = "Arg" if typ == type.REQ else "Ret"
  obj_name = rpc_name + suffix
  return cerebro.interface.cerebro_interface_pb2.__dict__.get(obj_name)

filter_ips = ["10.48.64.202"]
filter_ports = [2020]
filter_rpc = "QueryRemoteClusterStatus"

class type():
    REQ = 1
    RES = 2


class Mypcap():
  def __init__(self, file=None):
    self.fh = None
    if not file:
      self.socket = socket.socket(socket.AF_PACKET,socket.SOCK_RAW,socket.htons(0x3))
      self.socket.bind(('eth0',0))
    else:
      try:
        self.fh = open(file, 'r')
      except Exception as e:
        print "ERROR: Opening file %s failed: %s" % (file, e)
        raise Exception("ERROR: Opening file %s failed: %s" % (file, e))
      self.pcap_gh = {}
      self.read_header()
      if self.pcap_gh['magicnum'] != 0xd4c3b2a1:
        print "ERROR: The pcap file %s is corrupt" % file
        raise Exception("ERROR: The pcap file %s is corrupt" % file)
      self.offset = 24
    self.rpc_id_map = {}
    self.iph = {}
    self.tcph = {}
    self.ethf = {}
    self.pkt_read = 0 # packets read successfully
    self.pkt_filtered = 0 # packets filtered out

  def read_header(self):
    self.fh.seek(0)
    hbuf = self.fh.read(24)
    gh = struct.unpack("!IHHiIII", hbuf)
    self.pcap_gh['magicnum'] = gh[0]
    self.pcap_gh['majorver'] = gh[1]
    self.pcap_gh['minorver'] = gh[2]
    self.pcap_gh['thiszone'] = gh[3]
    self.pcap_gh['sigfigs'] = gh[4]
    self.pcap_gh['snaplen'] = gh[5]
    self.pcap_gh['network'] = gh[5]

  def read_one_record(self):
    try:
      phbuf = self.fh.read(16)
    except Exception as e:
      print "ERROR: reading buf"
    if not len(phbuf):
      print "INFO: End of file"
      return False
    ph = struct.unpack("IIII", phbuf)
    self.cur_record = {}
    self.cur_record['tssec'] = ph[0]
    self.cur_record['tsusec'] = ph[1]
    self.cur_record['incllen'] = ph[2]
    self.cur_record['origlen'] = ph[3]
    self.pbuf = self.fh.read(self.cur_record['incllen'])
    #print "cur record len %d" % self.cur_record['incllen']
    return True

  def get_ip_header(self):
    iph = struct.unpack("!BBHHHBBH4s4s", self.buf[0:20])
    self.iph['version'] = iph[0] >> 4
    self.iph['hlen'] = (iph[0] & 0xf) * 4 # last 4bits - no of 32 bit words
    self.iph['tos'] = iph[1]
    self.iph['total_len'] = iph[2]
    self.iph['id'] = iph[3]
    self.iph['frag_off'] = iph[4]
    self.iph['ttl'] = iph[5]
    self.iph['proto'] = iph[6]
    self.iph['hcksum'] = iph[7]
    self.iph['sip'] = socket.inet_ntoa(iph[8])
    self.iph['dip'] = socket.inet_ntoa(iph[9])

  def get_tcp_header(self):
    tcph = struct.unpack("!HHLLBBHHH", self.buf[20:40])
    self.tcph['sport'] = tcph[0]
    self.tcph['dport'] = tcph[1]
    self.tcph['seqno'] = tcph[2]
    self.tcph['ackno'] = tcph[3]
    self.tcph['hlen'] = (tcph[4] >> 4)*4  #4bits of 8bits - no of 32bit words
    self.tcph['wsize'] = tcph[7]
    self.tcph['hcksum'] = tcph[8]

  def get_ether_frame(self):
    ethf = struct.unpack("!6s6sH", self.ebuf)
    self.ethf['dmac'] = ethf[0]
    self.ethf['smac'] = ethf[1]
    self.ethf['etyp'] = ethf[2]

  def read_one(self, ips=None, ports=None):
    while True:
      self.pkt_filtered += 1 # assume as filtered; reset if not
      if not self.fh:
        self.sockbuf = self.socket.recvfrom(65536)
        self.pbuf = self.sockbuf[0]
      else:
        if not self.read_one_record():
          self.pkt_filtered -= 1
          return False
      self.pkt_read += 1 # got one packet
      self.ebuf = self.pbuf[:14]
      self.buf = self.pbuf[14:]
      self.get_ether_frame()
      # skip non-tcpip
      if self.ethf['etyp'] != 0x800:
        continue
      if len(self.buf) < 40:
        print "WARN: shorter packet (len=%s)" % (len(self.buf))
      self.get_ip_header()
      #print self.iph
      self.get_tcp_header()
      #print self.tcph
      self.poffset = self.iph['hlen'] + self.tcph['hlen']
      if self.poffset >= self.iph['total_len']:
        #print "DEBUG: Skip: no tcp payload %d" % len(self.buf)
        continue
      self.type = type.REQ if self.tcph['dport'] <= 49151 else type.RES
      if not ports or len(ports) or self.tcph['sport'] in ports or self.tcph['dport'] in ports:
        #print "-> %s:%s %s:%s" % (self.iph['sip'], self.tcph['sport'], self.iph['dip'], self.tcph['dport'])
        if not ips or not len(ips) or self.iph['sip'] in ips or self.iph['dip'] in ips:
          break
    self.pkt_filtered -= 1
    return True

  def parse_http(self):
    # HTTP
    data=self.buf[self.poffset:]
    lines = data.split('\n')
    clen = 0
    content = None
    ctype = None
    for i, line in enumerate(lines):
      #print "DEBUG " + line
      if "Content-Length" in line:
        clen = line.strip('\r').split()[1]
        continue
      if "Content-Type" in line:
        ctype = line.strip('\r').split()[1]
        continue
      if line == '\r':
        # collate rest of lines
        content = '\n'.join(lines[i+1:])
        break
    if ctype and not 'x-rpc' in ctype:
      print "not rpc"
      return None
    if not clen or not content:
      print "len %d" % len(self.buf)
      print "type %s" % ctype
      return None
    return content

  # data is HTTP content
  def parse_rpc(self, data):
    # looking for rpc header (req or res); header size is in first 4 bytes
    rpc_len,  = struct.unpack("!I", data[0:4])
    #print "DEBUG: rpc_len %d" % rpc_len
    if rpc_len < 4:
      print "ERROR: rpc_len less than 4"
      return
    self.rpc_header = RpcRequestHeader() if self.type == type.REQ else RpcResponseHeader()
    self.rpc_header.ParseFromString(data[4:rpc_len+4])
    if not self.rpc_header:
      print "ERROR: serializing RPC failed"
      return
    if self.type == type.REQ:
      self.rpc_method_name = self.rpc_header.method_name
      self.rpc_id_map[self.rpc_header.rpc_id] = self.rpc_method_name
    else:
      self.rpc_method_name = self.rpc_id_map.get(self.rpc_header.rpc_id, None)
    if self.rpc_method_name:
      self.arg_proto = cerebro_rpc_obj(self.rpc_method_name, self.type)()
    if self.arg_proto:
      print self.arg_proto.__class__.__name__
      #print "##DEBUG: %s " % self.rpc_header
      proto_offset = 4+rpc_len
      proto_end_offset = proto_offset + self.rpc_header.protobuf_size
      self.arg_proto.ParseFromString(data[proto_offset:proto_end_offset])

def scan():
  pcap = Mypcap()
  while True:
    print "####"
    if not pcap.read_one(ips=filter_ips, ports=filter_ports):
      return
    print "#####INFO: %s:%s --> %s:%s ## %s" % (pcap.iph['sip'],
      pcap.tcph['sport'], pcap.iph['dip'], pcap.tcph['dport'],
      "REQUEST" if pcap.type == type.REQ else "Response")
    http_payload = pcap.parse_http()
    if not http_payload:
      continue
    pcap.parse_rpc(http_payload)
    if pcap.rpc_header:
      #print "##########INFO rpc id : %d" % pcap.rpc_header.rpc_id
      print "##########INFO rpc header:\n%s" % (pcap.rpc_header)
      print "##########INFO rpc Method: %s" % (pcap.rpc_method_name)
      print "##########INFO rpc payload:\n%s\n%s" % (pcap.arg_proto.__class__.__name__, pcap.arg_proto)

def read_pcap(f):
  pcap = Mypcap(f)
  while True:
    print "\n####"
    if not pcap.read_one(ips=filter_ips, ports=filter_ports):
      break
    print "#####INFO: %s:%s --> %s:%s ## %s" % (pcap.iph['sip'],
      pcap.tcph['sport'], pcap.iph['dip'], pcap.tcph['dport'],
      "REQUEST" if pcap.type == type.REQ else "Response")
    http_payload = pcap.parse_http()
    if not http_payload:
      continue
    pcap.parse_rpc(http_payload)
    if pcap.rpc_header:
      #print "##########INFO rpc id : %d" % pcap.rpc_header.rpc_id
      print "##########INFO rpc header:\n%s" % (pcap.rpc_header)
      print "##########INFO rpc Method: %s" % (pcap.rpc_method_name)
      print "##########INFO rpc payload:\n%s\n%s" % (pcap.arg_proto.__class__.__name__, pcap.arg_proto)
  print "Total packets %d" % pcap.pkt_read
  print "Packets filtered out %d" % pcap.pkt_filtered

if __name__ == "__main__":
  #scan()
  read_pcap("/tmp/repl2020.pcap")
