import socket
import struct

#s = socket.socket(socket.AF_INET,socket.SOCK_RAW,socket.IPPROTO_IP)
#s = socket.socket(socket.AF_INET,socket.SOCK_RAW,socket.IPPROTO_TCP)

def get_ip_header(data):
  ip_header = {}
  iph = struct.unpack("!BBHHHBBH4s4s", data)
  ip_header['version'] = iph[0] >> 4
  ip_header['len'] = iph[0] & 0xf
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
  tcp_header['hlen'] = tcph[4] & 0x3f
  tcp_header['wsize'] = tcph[7]
  tcp_header['hcksum'] = tcph[7]
  return tcp_header


def read_one(sock, port=None):
  while True:
    buf=sock.recvfrom(65536)[0]
    ip_header = get_ip_header(buf[0:20])
    tcp_header = get_tcp_header(buf[20:40])
    #print "%s %s" % (tcp_header['sport'], tcp_header['dport'])
    if not port or tcp_header['sport'] == port or tcp_header['dport'] == port:
      break
  return buf, ip_header, tcp_header

def parse_payload(data):
  # looking for rpc header (req or res)
  rpc_id = struct.unpack("!Q", data[0:64])
  print "%d" % rpc_id
