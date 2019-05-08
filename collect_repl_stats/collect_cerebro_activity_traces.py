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
import urllib2
import re
import json
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

default_policies = [

]

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
  def __init__(self, pd, remote, dump_dir, policy_path="./policy.json"):
    self.pd = pd
    self.remote = remote
    self.dump_dir = dump_dir
    self.metaop_id = 0
    self.metaop_page_txt = ""
    self.policies = []
    self.replicate_file_op_page_txt_vec = []
    self.load_policies(policy_path)
    self.get_repl_metaop_id(pd, remote)
    self.state_cell_str = u""
    self.slave_info = []

  def load_policies(self, path):
    if os.path.isfile(path):
      fp = open(path)
      self.policies = json.load(fp)
      #print self.policies

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

  def get_slave_dump_path(self, name, add_ts=True):
    fname = "%s-" % self.metaop_id + "%s" % name
    if add_ts:
      fname += time.strftime("-%Y%m%d_%H%M%S")
    return os.path.join(self.dump_dir, fname)

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
    params = {"pd": self.pd, "op": self.metaop_id}
    url="http://%s" % ip #includes port
    resp = requests.get(url, params=params)
    if not resp.ok:
      log.ERROR("Couldn't get metaop info for %s" % self.metaop_id)
      return None
    self.metaop_page_txt = resp.text
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

    if not len(self.slave_info):
      summary += "\nNo Slaves Found"

    self.summary = summary
    file_path = os.path.join(self.dump_dir, "replication_summary.txt")
    self.dump_to_file(file_path, summary, convert=False)

  def get_html_page_urls(self, ip, policy):
    '''
    Per policy, come up with unique url and name which when run will collect a file.
    '''
    done = False
    urldlist = []
    base_name = ip.replace(".", "_") + "-" + str(policy['port'])
    for si in self.slave_info:
      name = base_name
      urldic = {}
      urldic["url"] = "http://%s:%s" % (ip, policy['port'])
      if si.get("slave IP") == ip:
        my_si = si
        if policy.has_key("urlpath"):
          urlpath = policy["urlpath"]
          work_id = si.get("work_id", 0)
          paramsl = policy.get("params", [{}])
          params = paramsl[0]
          if "replicate_stats" in urlpath:
            if params and work_id:
              params["id"] = work_id
              name += "-" + str(work_id)
            name += "-" + "replicate_stats"
          elif "replication_stats" in urlpath:
            if params and work_id:
              params["work_id"] = work_id
              name += "-" + str(work_id)
            name += "-" + "replication_stats"
          else:
            continue
        urldic["urlpath"] = urlpath
        urldic["params"] = params
        urldic["name"] = name
        urldlist.append(urldic)
        done = True

    if done:
      return urldlist

    urlpath = policy.get("urlpath", "/")
    paremsl = policy.get("params", [{}])
    for params in paramsl:
      urldic = {}
      urldic["url"] = "http://%s:%s" % (ip, policy['port'])
      if "h/traces" in params.get("urlpath", ""):
        params["expand"] = ""
      urldic["urlpath"] = urlpath
      urldic["params"] = params
      comp = params.get("regex", params.get("c", "h-traces"))
      urldic["name"] = base_name + "-%s" % comp
      urldlist.append(urldic)
    return urldlist

  def get_html_page_dump(self, ip, urldlist):
    '''
    web = urllib2.urlopen(url)
    if web.getcode() != 200:
      print "ERROR: couldn't get the page" % name
      continue
    resp = request(url, params=params)
    '''
    for urldic in urldlist:
      log.INFO(urldic)
      name = urldic["name"]
      url = urldic["url"] + urldic["urlpath"]
      params = urldic["params"]
      path = self.get_slave_dump_path(name, ip)
      resp = requests.get(url, params=params)
      log.INFO("url: %s" % resp.url)
      if not resp.ok:
        log.ERROR("Couldn't get slave page dumped")
      self.dump_to_file(path + '.html', resp.text, convert=True)

  # stargate_slave_data = {"port": 2009, "c": "vdisk_controller", "low": 256, "regex": ["VDiskMicroReadExtentsOp", "VDiskMicroCerebroReplicateOp"]}
  # stargate_replicate_latency {"port": 2009, "url": "latency/replicate_stats", id: 567028}
  # stargate_replication_stats {"url": "http:0:2009/replication_stats", "work_id": 567028}
  # cerebro_slave_data = {"port": 2020, "c": "cerebro_slave", low: 256}

  #   . http://0:2009/replication_stats?work_id=567028 with work_id
  #   . http://0:2009/latency/replicate_stats?id=567028
  def get_slave_op_data(self):
    if len(self.slave_info):
      ipl = list(set([si.get('slave IP', None) for si in self.slave_info]))
      for ip in ipl:
        for policy in self.policies:
          urldlist = self.get_html_page_urls(ip, policy)
          self.get_html_page_dump(ip, urldlist)
        '''
        if not self.policies:
          url="http://%s:%s/h/traces?c=cerebro_slave&expand=" % (ip, "2020")
          web = urllib2.urlopen(url)
          if web.getcode() != 200:
            print "ERROR: couldn't get the page"
            continue
          path = self.get_slave_dump_path("%s_cerebro_slave" % ipreplace(".", "_"))
          data = web.read()
          self.replicate_file_op_page_txt_vec.append(data)
          self.dump_to_file(path + '.html', data, convert=True)

          # c=vdisk_controller&a=completed&regex=VDiskMicroCerebroReplicateOp
          url='http://%s:2009/h/traces?c=vdisk_controller&' % ip
          vdisk_ext_read_ext = "low=256&a=completed&regex=VDiskMicroReadExtentsOp&expand="
          web = urllib2.urlopen(url + vdisk_ext_read_ext)
          if web.getcode() != 200:
            print "ERROR: couldn't get the page"
            continue
          data = web.read()
          path = self.get_slave_dump_path("%s_vdisk_read_extent" % ipreplace(".", "_"))
          self.dump_to_file(path + '.html', data, convert=True)

          vdisk_replicate_ext = "low=256&a=completed&regex=VDiskMicroCerebroReplicateOp"
          web = urllib2.urlopen(url + vdisk_replicate_ext)
          if web.getcode() != 200:
            print "ERROR: couldn't get the page"
            continue
          data = web.read()
          path = self.get_slave_dump_path("%s_vdisk_replicate" % ip.replace(".", "_"))
          self.dump_to_file(path + '.html', data, convert=True)
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
  rep = Replication(pd, remote, FLAGS.dump_dir)
  rep.get_metaop_data()
  rep.get_slave_op_data()
  rep.get_replication_summary()
