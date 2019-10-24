#!/usr/bin/python
#
# nagaraj.srinivasa@nutanix.com
#
import sys
import socket
import struct
import datetime

sys.path.append('/home/nutanix/cluster/bin')
import env
from util.sl_bufs.net.rpc_pb2 import RpcRequestHeader, RpcResponseHeader
from cerebro.interface.cerebro_interface_pb2 import *

import util.base.log as log

rpc_map = {
    'FetchReplicationTarget': (FetchReplicationTargetArg(), FetchReplicationTargetRet()),
    "SyncDataStats": (SyncDataStatsArg(), SyncDataStatsRet()),
    "QueryRemoteClusterStatus": (QueryRemoteClusterStatusArg(), QueryRemoteClusterStatusRet()),
    "ProtectSnapshotHandlesOnRemote": (ProtectSnapshotHandlesOnRemoteArg(), ProtectSnapshotHandlesOnRemoteRet()),
    "TransferConsistencyGroupMetadata": (TransferConsistencyGroupMetadataArg(), TransferConsistencyGroupMetadataRet()),
}


def otype(obj):
    return obj.__class__.__name__


def cerebro_rpc_obj(rpc_name, typ):
    import cerebro.interface.cerebro_interface_pb2
    suffix = "Arg" if typ == type.REQ else "Ret"
    obj_name = rpc_name + suffix
    return cerebro.interface.cerebro_interface_pb2.__dict__.get(obj_name)


def cassandra_rpc_obj(rpc_name, typ):
    import cassandra.cassandra_client.cassandra_pb2
    suffix = "Arg" if typ == type.REQ else "Ret"
    obj_name = "Cassandra" + rpc_name + suffix
    return cassandra.cassandra_client.cassandra_pb2.__dict__.get(obj_name)


rpc_port_map = {
    "2020": cerebro_rpc_obj,
    "9161": cassandra_rpc_obj
}


class type():
    REQ = 1
    RES = 2


class Packet():
    def __init__(self, pcap):
        if not pcap or not pcap.initialised:
            raise Exception("ERROR: Pcap object not initialised")
        self.pcap = pcap
        self.iph = {}
        self.tcph = {}
        self.ethf = {}
        self.delta = None
        self.pkt_read = 0  # packets read successfully
        self.pkt_filtered = 0  # packets filtered out

    def get_ip_header(self):
        """
    Parse the IP Header
    """
        iph = struct.unpack("!BBHHHBBH4s4s", self.buf[0:20])
        self.iph['version'] = iph[0] >> 4
        self.iph['hlen'] = (iph[0] & 0xf) * 4  # last 4bits - no of 32 bit words
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
        """
    Parse the TCP Header
    """
        tcph = struct.unpack("!HHLLBBHHH", self.buf[20:40])
        self.tcph['sport'] = tcph[0]
        self.tcph['dport'] = tcph[1]
        self.tcph['seqno'] = tcph[2]
        self.tcph['ackno'] = tcph[3]
        self.tcph['hlen'] = (tcph[4] >> 4) * 4  # 4bits of 8bits - no of 32bit words
        self.tcph['wsize'] = tcph[7]
        self.tcph['hcksum'] = tcph[8]

    def get_ether_frame(self):
        """
    Parse the Ethernet frame Header
    """
        ethf = struct.unpack("!6s6sH", self.ebuf)
        self.ethf['dmac'] = ethf[0]
        self.ethf['smac'] = ethf[1]
        self.ethf['etyp'] = ethf[2]

    def parse_http(self):
        """
    Parse HTTP from TCP Payload
    """
        self.http_payload = None
        data = self.tcp_payload
        lines = data.split('\n')
        clen = 0
        content = None
        ctype = None
        for i, line in enumerate(lines):
            # print "DEBUG " + line
            if "Content-Length" in line:
                clen = line.strip('\r').split()[1]
                continue
            if "Content-Type" in line:
                ctype = line.strip('\r').split()[1]
                continue
            if line == '\r':
                # collate rest of lines
                content = '\n'.join(lines[i + 1:])
                break
        if ctype and not 'x-rpc' in ctype:
            print "not rpc"
            return
        if not clen or not content:
            print "len %d" % len(self.buf)
            print "type %s" % ctype
            return
        self.http_payload = content

    def read_one_record(self):
        """
    Read one record from pcap file
    """
        handle = self.pcap.handle
        if otype(handle) != "file":
            raise Exception("ERROR: expected file object")
        try:
            phbuf = handle.read(16)
        except Exception as e:
            print "ERROR: reading buf"
        if not len(phbuf):
            print "DEBUG: End of file"
            return False
        ph = struct.unpack("IIII", phbuf)
        self.record = {}
        self.record['tssec'] = ph[0]
        self.record['tsusec'] = ph[1]
        self.record['incllen'] = ph[2]
        self.record['origlen'] = ph[3]
        self.pbuf = handle.read(self.record['incllen'])
        # print "cur record len %d" % self.cur_record['incllen']
        return True

    def read_one(self):
        """
    Read one packet from the handle
    """
        while True:
            self.pkt_filtered += 1  # assume as filtered; reset if not
            handle = self.pcap.handle
            if otype(handle) == "_socketobject":
                self.sockbuf = self.handle.recvfrom(65536)
                self.pbuf = self.sockbuf[0]
                # self.ts = int(float(datetime.datetime.now().strftime("%s.%f"))*1000)
                self.ts = datetime.datetime.now()
            elif otype(handle) == "file":
                if not self.read_one_record():
                    self.pkt_filtered -= 1
                    return False
                self.ts = datetime.datetime.fromtimestamp(int(self.record['tssec']))
                ts_delta = datetime.timedelta(microseconds=int(self.record['tsusec']))
                self.ts += ts_delta

            self.pkt_read += 1  # got one packet
            self.ebuf = self.pbuf[:14]  # ethernet header
            self.buf = self.pbuf[14:]  # rest of the packet
            self.get_ether_frame()

            if self.ethf['etyp'] != 0x800:
                continue  # skip non-tcpip

            if len(self.buf) < 40:
                print "WARN: shorter packet (len=%s)" % (len(self.buf))

            self.get_ip_header()
            # print self.iph

            self.get_tcp_header()
            # print self.tcph

            self.poffset = self.iph['hlen'] + self.tcph['hlen']
            if self.poffset >= self.iph['total_len']:
                # print "DEBUG: Skip: no tcp payload %d" % len(self.buf)
                continue

            # self.type = type.REQ if self.tcph['dport'] <= 49151 else type.RES
            known_ports = [2020, 2009, 9161, 2010]
            self.type = type.REQ if self.tcph['dport'] in known_ports else type.RES

            # apply filter if any
            if not self.pcap.filter:
                break
            if not len(self.pcap.filter.ports) or \
                    self.tcph['sport'] in self.pcap.filter.ports or \
                    self.tcph['dport'] in self.pcap.filter.ports:
                # print "-> %s:%s %s:%s" % (self.iph['sip'], self.tcph['sport'], self.iph['dip'], self.tcph['dport'])
                if not len(self.pcap.filter.ips) or \
                        self.iph['sip'] in self.pcap.filter.ips or \
                        self.iph['dip'] in self.pcap.filter.ips:
                    break
        self.pkt_filtered -= 1
        self.tcp_payload = self.buf[self.poffset:]
        self.debug_info = " %s:%s --> %s:%s ## %s" % (self.iph['sip'],
                                                      self.tcph['sport'], self.iph['dip'], self.tcph['dport'],
                                                      "REQUEST" if self.type == type.REQ else "Response")
        print "####### %s" % self.debug_info
        return True

    # data is HTTP content
    def parse_rpc(self):
        """
    Parse RPC in HTTP Payload if present. Try it from TCP payload if not.
    """
        data = self.http_payload if self.http_payload else self.tcp_payload

        if not self.http_payload:
            # hueristic for rpc_v2, 4 to 8 is header len
            data = self.tcp_payload
            hdr_len, = struct.unpack("!I", data[4:8])
            print "DEBUG: hdr_len %d" % hdr_len
            if hdr_len < 4:
                print "ERROR: hdr_len less than 4"
                return
            hdr = data[8:hdr_len + 8]
            proto_offset = 8 + hdr_len
        else:
            data = self.http_payload
            rpc_len, = struct.unpack("!I", data[0:4])
            # looking for rpc header (req or res); header size is in first 4 bytes
            print "DEBUG: rpc_len %d" % rpc_len
            if rpc_len < 4:
                print "ERROR: rpc_len less than 4"
                return
            hdr = data[4:rpc_len + 4]
            proto_offset = 4 + rpc_len

        self.rpc_header = RpcRequestHeader() if self.type == type.REQ else RpcResponseHeader()
        self.rpc_header.ParseFromString(hdr)
        if not self.rpc_header:
            print "ERROR: serializing RPC failed"
            return
        print "###!!! rpc %s" % self.rpc_header
        if self.type == type.REQ:
            self.rpc_method_name = self.rpc_header.method_name
            self.pcap.rpc_id_map[self.rpc_header.rpc_id] = self
            server_port = str(self.tcph['dport'])
        else:
            self.req_pkt = self.pcap.rpc_id_map.get(self.rpc_header.rpc_id, None)
            if self.req_pkt:
                self.rpc_method_name = self.req_pkt.rpc_method_name
                self.delta = self.ts - self.req_pkt.ts
            server_port = str(self.tcph['sport'])
        print server_port
        if self.rpc_method_name:
            proto_method = rpc_port_map[server_port]
            self.arg_proto = proto_method(self.rpc_method_name, self.type)()
        if self.arg_proto:
            print "proto_name " + self.arg_proto.__class__.__name__
            # print "##DEBUG: %s " % self.rpc_header
            proto_end_offset = proto_offset + self.rpc_header.protobuf_size
            self.arg_proto.ParseFromString(data[proto_offset:proto_end_offset])


class Filter():
    def __init__(self, ips=[], ports=[]):
        self.ips = ips
        self.ports = ports


class Pcap():
    def __init__(self, file=None, filter=None):
        self.handle = None
        if not file:
            try:
                self.handle = socket.socket(socket.AF_PACKET, socket.SOCK_RAW, socket.htons(0x3))
                self.handle.bind(('l0', 0))
            except:
                raise Exception("ERROR: Couldn't initialize the raw socket")
        else:
            try:
                self.handle = open(file, 'r')
            except Exception as e:
                raise Exception("ERROR: Opening file %s failed: %s" % (file, e))
            self.pcap_gh = {}
            self.read_header()
            if self.pcap_gh['magicnum'] != 0xd4c3b2a1:
                raise Exception("ERROR: The pcap file %s is corrupt" % file)
            self.offset = 24
        self.filter = filter
        self.initialised = True
        self.rpc_id_map = {}

    def read_header(self):
        self.handle.seek(0)
        hbuf = self.handle.read(24)
        gh = struct.unpack("!IHHiIII", hbuf)
        self.pcap_gh['magicnum'] = gh[0]
        self.pcap_gh['majorver'] = gh[1]
        self.pcap_gh['minorver'] = gh[2]
        self.pcap_gh['thiszone'] = gh[3]
        self.pcap_gh['sigfigs'] = gh[4]
        self.pcap_gh['snaplen'] = gh[5]
        self.pcap_gh['network'] = gh[6]


def scan():
    pcap = Pcap()
    while True:
        print "####"
        pkt = Packet(pcap)
        if not pkt.read_one():
            return
        pkt.parse_http()
        if not pkt.http_payload:
            continue
        print pkt.debug_info
        pkt.parse_rpc()
        delta = "(+ %s)" % pkt.delta.total_seconds() if pkt.delta else ""
        print "##INFO: %s%s %s" % (pkt.ts.strftime("%m%d%Y %H:%M:%S.%f"), delta, pkt.debug_info)
        if pkt.rpc_header:
            # print "##########INFO rpc id : %d" % pkt.rpc_header.rpc_id
            print "##INFO rpc header:\n%s" % (pkt.rpc_header)
            print "##INFO rpc Method: %s" % (pkt.rpc_method_name)
            proto_str = "%s" % pkt.arg_proto
            proto_str = proto_str.replace("\n", " ")
            print "##INFO rpc payload:\n%s\n%s" % (pkt.arg_proto.__class__.__name__, proto_str)


def read_pcap(f):
    pcap = Pcap(f)
    while True:
        print "\n####"
        pkt = Packet(pcap)
        if not pkt.read_one():
            break
        pkt.parse_http()
        # if not pkt.http_payload:
        # continue
        delta = "(+ %s)" % pkt.delta.total_seconds() if pkt.delta else ""
        print "##INFO: %s%s %s" % (pkt.ts.strftime("%m%d%Y %H:%M:%S.%f"), delta, pkt.debug_info)
        pkt.parse_rpc()
        if pkt.rpc_header:
            # print "##########INFO rpc id : %d" % pkt.rpc_header.rpc_id
            print "##INFO rpc header:\n%s" % (pkt.rpc_header)
            print "##INFO rpc Method: %s" % (pkt.rpc_method_name)
            proto_str = "%s" % pkt.arg_proto
            proto_str = proto_str.replace("\n", "\n\t")
            print "##INFO rpc payload:\n%s\n%s" % (pkt.arg_proto.__class__.__name__, proto_str)
    print "Total packets %d" % pkt.pkt_read
    print "Packets filtered out %d" % pkt.pkt_filtered


if __name__ == "__main__":
    # scan()
    read_pcap("/tmp/9161.pcap")