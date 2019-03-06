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
import util.base.command as command
import util.base.log as log

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
    self.metaop_page = ""
    self.replicate_op_page_vec = []
    self.get_repl_metaop_id(pd, remote)
    self.state_cell_str = u""
    self.slave_info = []
    self.timeout = 60

  def get_repl_metaop_id(self, name, rem):
    '''
    returns ongoing replication metaop_id for the PD to the remote
    '''
    print "pd=%s remtoe=%s" % (name, rem)
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
            self.snap_handle = repl.snapshot_handle_vec
            if repl.reference_snapshot_handle_vec:
              self.ref_snap_handle = repl.snapshot_handle_vec
            print "found"
            break


  def dump_to_file(self, path, convert=True):
    '''
    Truncate a file and dump the data
    '''
    data_bytes = self.text
    l = len(data_bytes)
    flags = os.O_CREAT|os.O_RDWR|os.O_TRUNC
    fd = os.open(path, flags)
    res_l = os.write(fd, data_bytes)
    if not res_l == len(data_bytes):
      log.WARNING("couldn't dump to %s path completely" % path)
    os.close(fd)
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
    html = etree.HTML(self.text)
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

    # r=r'work_descriptor_vec \{[^}]+\}'
    # re.findall(r' .*: (.*)\n', ss, re.M)
    # re.findall(r'.*(\bwork_descriptor_vec |\bwork_id|\bfile_path|\bslave_incarnation_id|\bfile_vdisk_id|\breference_file_vdisk_id)(.*)\n', sstr, re.M)
    mgroup = re.findall(r' *(\bwork_id|\bfile_path|\bslave_incarnation_id|\bfile_vdisk_id): \"*(.*)\n\"*', ss, re.M)
    #pat1=r' work_id: (\d+)\n.*slave_incarnation_id: (\d+)\n.*file_path: "(\S+)"'
    #mgroup1 = re.findall(pat, self.state_cell_str, re.M)
    rrplication = ReplicateMetaOpCkptStateProto()
    _ = text_format.Merge(re.findall(r'\{\n (.*)\n.*\}', sstr, re.M|re.S)[0],r)
    for tr in wd_table:
      print "len %s" % len(wd_table)
      if "Work" in tr[0].text or "File" in tr[0].text:
        continue # two headers of the table
      si = {}
      si['file'] = tr[0].text
      for match in mgroup:
        if si['file'] in match[1]:
          si['work_id'] = match[0]
          si['slave id'] = match[1]
      si['cg'] = tr[1].text
      si['type'] = tr[2].text
      si['slave IP'] = tr[3][0].text #hlink
      self.slave_info.append(si)
      print self.slave_info

  def get_metaop_data(self):
    ip = get_cerebro_master()
    url="http://%s/?pd=%s&op=%d" % (ip,self.pd,self.metaop_id)
    params={'timeout': self.timeout}
    ret = requests.get(url, params=params)
    web = urllib.urlopen(url)
    if web.getcode() != 200:
      print "ERROR: couldn't get the page"
      return None
    self.text = web.read()
    self.metaop_page = self.dump_dir + "/%d_replicate_metaop" % self.metaop_id
    file_path = self.metaop_page + ".html"
    self.dump_to_file(file_path, convert=True)
    self.parse_replicate_metaop()
    return ret

  def get_replicate_file_op_data(self):
    if len(self.slave_info):
      for si in self.slave_info:
        wid = si['work_id']
        ip = si['slave IP']


'''
  def get_slave_ops(self):
    if not os.lexists(self.metaop_page + ".txt"):
      print "ERROR: text metaop page doesn't exist"
      return
    filter1 = " work_id|slave_incarnation_id| file_path"
    cmd = "/usr/bin/egrep -e '%s' %s.txt" % (filter1,self.metaop_page)
    rv, data, err = command.timed_command(cmd, 60)
    if rv:
      print err
      return
'''

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
  rep = Replication(pd, remote, dump_dir)
  repl = rep.get_metaop_data()
