#
# collect cerebro activity traces around a replication
#
# Input: PD name, Remote Name
# Get the progressing replication metaop for the PD to the remote
# Get CerebroMasterReplicateMetaOp traces.
# Get the slaves handling work_ids.
# Get 'http:0:2020/?pd=27Feb_test_pd&op=74556'
import os
import sys
import time
import requests
import urllib
import re
from lxml import etree
for path in os.listdir("/usr/local/nutanix/lib/py"):
        sys.path.insert(0, os.path.join("/usr/local/nutanix/lib/py", path))
import gflags
from cerebro.interface.cerebro_interface_pb2 import *
from cerebro.master.cerebro_schedule_pb2 import CerebroOutOfBandScheduleProto
from cerebro.client.cerebro_interface_client import CerebroInterfaceTool, CerebroInterfaceError
from util.base.types import NutanixUuid
from util.misc.protobuf import pb2json
from util.nfs.nfs_client import NfsClient, NfsError
from cerebro.master.persistent_op_state_pb2 import PersistentOpBaseStateProto as op_state
from cerebro.master.persistent_op_state_pb2 import PersistentOpStateProto as Pstate
import util.base.command as command
import util.base.log as log

import google.protobuf.text_format as text_format

from prettytable import PrettyTable

gflags.DEFINE_string("pd", "", "pd name")
gflags.DEFINE_string("remote", "", "remote name")
gflags.DEFINE_string("dump_dir", "/home/nutanix/tmp/", "directory to dump the pages in")

FLAGS = gflags.FLAGS

RpcClient = CerebroInterfaceTool()

def get_cerebro_master():
  '''
  gets cerebro master handle
  '''
  arg = GetMasterLocationArg()
  ret = GetMasterLocationRet()
  try:
    ret = RpcClient.GetMasterLocation(arg)
  except CerebroInterfaceError as e:
    print "Error querying cerebro master %s" % e
  return ret.master_handle

class Replication():
  def __init__(self, pd, remote, dump_dir):
    self.pd = pd
    self.remote = remote
    self.dump_dir = dump_dir
    self.metaop_id = 0
    self.metaop_page_txt = ""
    self.replicate_file_op_page_txt_vec = []
    self.get_repl_metaop_id(pd, remote)
    self.state_cell_str = u""
    self.slave_info = []

  def get_repl_metaop_id(self, name, rem):
    '''
    returns ongoing replication metaop_id for the PD to the remote
    '''
    arg = QueryProtectionDomainArg()
    ret = QueryProtectionDomainRet()
    arg.protection_domain_name = name
    arg.fetch_executing_meta_ops = True
    arg.list_snapshot_handles.CopyFrom(QueryProtectionDomainArg.ListSnapshotHandles())
    try:
      ret = RpcClient.query_protection_domain(arg)
    except CerebroInterfaceError as e:
      print "Error querying pds %s" % e
    #print ret
    for exec_op in ret.executing_meta_op_vec:
      if exec_op.base_persistent_state.opcode == op_state.Opcode.Value("kReplicateMetaOp"):
        for repl in exec_op.base_persistent_state.reference_actions.replication:
          if repl.remote_name == rem:
            self.metaop_id = exec_op.meta_opid
            self.snap_handle = repl.snapshot_handle_vec[0]
            if repl.reference_snapshot_handle:
              self.ref_snap_handle = repl.reference_snapshot_handle
            break
  def get_slave_dump_path(self, comp, ip):
    fname = "%s_" % self.metaop_id + ip.replace('.', '_') + "_%s" % comp
    return self.dump_dir + fname


  def dump_to_file(self, path, data_bytes, convert=True):
    '''
    Truncate a file and dump the data
    '''
    l = len(data_bytes)
    flags = os.O_CREAT|os.O_RDWR|os.O_TRUNC
    fd = os.open(path, flags)
    res_l = os.write(fd, data_bytes)
    if not res_l == len(data_bytes):
      log.WARNING("couldn't dump to %s path completely" % path)
    os.close(fd)
    if not convert:
      return
    txt_path = path.split('.')[0]+".txt"
    cmd = "/usr/bin/links -dump %s" % path
    rv, txt_data_bytes, _ = command.timed_command(cmd, 60)
    if rv:
      return
    fd = os.open(txt_path, flags)
    res_l = os.write(fd, txt_data_bytes)
    if not res_l == len(txt_data_bytes):
      log.WARNING("couldn't dump to %s path completely" % txt_path)
    os.close(fd)

  def parse_replicate_metaop(self):
    '''
    Parse replicate meta op table
    '''
    html = etree.HTML(self.metaop_page_txt)
    tvec = html.xpath('//table/tr')
    wd_table = []
    for i, tr in enumerate(tvec):
      if len(tr) and tr[0].text:
        if "base" in tr[0].text:
          # base { has only one column
          for line in tr[0].itertext():
            self.state_cell_str += line.replace(u'\u00a0', ' ') + '\n'
        elif "Distribution" in tr[0].text:
          # this row is from Work Distrubution table
          wd_table = tr.getparent()
          break

    pstate = Pstate()
    _ = text_format.Merge(self.state_cell_str, pstate)

    for tr in wd_table:
      print "len %s" % len(wd_table)
      if "Work" in tr[0].text or "File" in tr[0].text:
        continue # two headers of the table
      si = {}
      si['file'] = tr[0].text
      for wd in pstate.replicate.work_descriptor_vec:
        if si['file'] in wd.file_path:
          si['work_id'] = wd.work_id
          si['slave id'] = wd.slave_incarnation_id
          si['file_vdisk_id'] = wd.file_vdisk_id
          si['ref_file_vdisk_id'] = wd.reference_file_vdisk_id
      si['cg'] = tr[1].text
      si['type'] = tr[2].text
      si['slave IP'] = tr[3][0].text.split(':')[0] #hlink
      self.slave_info.append(si)
      print self.slave_info

  def get_metaop_data(self):
    ip = get_cerebro_master()
    url="http://%s/?pd=%s&op=%d" % (ip,self.pd,self.metaop_id)
    web = urllib.urlopen(url)
    if web.getcode() != 200:
      print "ERROR: couldn't get the page"
      return None
    self.metaop_page_txt = web.read()
    self.metaop_page = self.dump_dir + "/%d_replicate_metaop" % self.metaop_id
    file_path = self.metaop_page + ".html"
    self.dump_to_file(file_path, self.metaop_page_txt, convert=True)
    self.parse_replicate_metaop()


  def get_replication_summary(self):
    summary = "Summary of replication of PD %s to Remote Site %s\n" % \
      (self.pd, self.remote)
    summary += "\tMetaop Op Id: %d\n" % self.metaop_id

    for si in self.slave_info:
      summary += '\t\twork id           : %s\n' % si['work_id']
      summary += '\t\tslave IP          : %s\n' % si['slave IP']
      summary += '\t\tSlave inc ID      : %d\n' % si['slave id']
      summary += '\t\tFile Path         : %s\n' % si['file']
      summary += '\t\tFile Vdisk ID     : %s\n' % si['file_vdisk_id']
      summary += '\t\tRef. File Vdisk ID: %s\n' % si['ref_file_vdisk_id']
      summary += '\n'
    else:
      summary += "\nNo Slaves Found"

    self.summary = summary
    file_path = self.dump_dir + "replication_summary.txt"
    self.dump_to_file(file_path, summary, convert=False)

  def get_slave_op_data(self):
    if len(self.slave_info):
      ipl = list(set([si.get('slave IP', None) for si in self.slave_info]))
      for ip in ipl:
        url="http://%s:2020/h/traces?c=cerebro_slave&expand=" % ip
        web = urllib.urlopen(url)
        if web.getcode() != 200:
          print "ERROR: couldn't get the page"
          continue
        path = self.get_slave_dump_path("cerebro_slave", ip)
        data = web.read()
        self.replicate_file_op_page_txt_vec.append(data)
        self.dump_to_file(path + '.html', data, convert=True)

        # c=vdisk_controller&a=completed&regex=VDiskMicroCerebroReplicateOp
        url='http://%s:2009/h/traces?c=vdisk_controller&' % ip
        vdisk_ext_read_ext = "low=256&a=completed&regex=VDiskMicroReadExtentsOp&expand="
        web = urllib.urlopen(url + vdisk_ext_read_ext)
        if web.getcode() != 200:
          print "ERROR: couldn't get the page"
          continue
        path = self.get_slave_dump_path("vdisk_read_extent", ip)
        self.dump_to_file(path + '.html', data, convert=True)

        vdisk_replicate_ext = "low=256&a=completed&regex=VDiskMicroCerebroReplicateOp"
        web = urllib.urlopen(url + vdisk_replicate_ext)
        if web.getcode() != 200:
          print "ERROR: couldn't get the page"
          continue
        path = self.get_slave_dump_path("vdisk_replicate", ip)
        self.dump_to_file(path + '.html', data, convert=True)

if __name__ == "__main__":
  argv = FLAGS(sys.argv)
  pd = FLAGS.pd
  remote = FLAGS.remote
  log.initialize()
  if FLAGS.dump_dir:
    if not os.path.lexists(FLAGS.dump_dir):
      os.mkdir(FLAGS.dump_dir)
    else:
      if not os.path.isdir(FLAGS.dump_dir):
        print "ERROR: %s is not a directory, exiting"
        sys.exit(1)
  rep = Replication(pd, remote, FLAGS.dump_dir)
  rep.get_metaop_data()
  rep.get_slave_op_data()
  rep.get_replication_summary()
