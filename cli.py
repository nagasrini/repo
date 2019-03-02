import cmd

import os
import sys
for path in os.listdir("/usr/local/nutanix/lib/py"):
        sys.path.insert(0, os.path.join("/usr/local/nutanix/lib/py", path))
import util.base.log as log

from cerebro.interface.cerebro_interface_pb2 import *
from cerebro.master.cerebro_schedule_pb2 import CerebroOutOfBandScheduleProto
from cerebro.client.cerebro_interface_client import CerebroInterfaceTool, CerebroInterfaceError
from cerebro.master.persistent_op_state_pb2 import PersistentOpBaseStateProto as ost

RpcClient = CerebroInterfaceTool()
log.initialize("/home/nutanix/x.INFO")

def inherits(cls1, cls2):
  return cls2 in cls1.__bases__

class PD(cmd.Cmd):
  '''
  cmd pd: plugin for pd related objects
  '''
  def read_pd_names(self):
    arg = ListProtectionDomainsArg()
    try:
      ret = RpcClient.list_protection_domains(arg)
    except CerebroInterfaceError as e:
      print "Getting PD names Failed: %s" % e
      return None
    self.pd_names = [a for a in ret.protection_domain_name]
    return [a for a in ret.protection_domain_name]

  def get_cg(self):
    arg = QueryProtectionDomainArg()
    arg.protection_domain_name = self.value
    arg.list_consistency_groups.CopyFrom(QueryProtectionDomainArg.ListConsistencyGroups())
    try:
      ret = RpcClient.query_protection_domain(arg)
    except CerebroInterfaceError as e:
      print "Error querying pds %s" % e
    return ret

  def get_snap(self):
    arg = QueryProtectionDomainArg()
    arg.protection_domain_name = self.value
    arg.list_snapshot_handles.CopyFrom(QueryProtectionDomainArg.ListSnapshotHandles())
    try:
      ret = RpcClient.query_protection_domain(arg)
    except CerebroInterfaceError as e:
      print "Error querying pds %s" % e
    return ret

  def get_repl_stat(self):
    arg = QueryProtectionDomainArg()
    ret = QueryProtectionDomainRet()
    arg.protection_domain_name = name
    arg.fetch_executing_meta_ops = True
    try:
      ret = RpcClient.query_protection_domain(arg)
    except CerebroInterfaceError as e:
      print "Error querying pds %s" % e

  def complete_pd(self, text, line, bidx, eidx):
    l=self.read_pd_names()
    if text:
      l = [pd for pd in self.pd_names if pd.startswith(text)]
    return l

  def do_pd(self, line):
    if not len(line):
      s = ''
      for n in self.read_pd_names(): s += n + ' '
      print s
      return
    if not self.pd_names:
      l = self.read_pd_names()
    if line not in self.pd_names:
      print "Error: %s non-existent PD" % line
      return
    print "to do <pd %s>" % (line)
    self.context = "pd"
    self.value = line
    self.prompt = "%s:%s > " % (self.context, self.value)

  def do_cg(self, line):
    if not len(line):
      print self.get_cg()

  def do_snapshot(self, line):
    if not len(line):
      print self.get_snap()

  def do_repl_status(self, line)
    if not len(line):
      print self.get_repl_stat()

      


valid_context = {
  "main" : ["pd", "vm", "remote"],
  "pd" : ["cg", "snapshot", "schedule"],
  "vm" : [""],
  "remote" : [ "" ]
}

class Main(cmd.Cmd):
  def __init__(self, completekey='tab', filename=None):
    cmd.Cmd.__init__(self)
    self.prompt = "> "
    if filename:
      self.file = filename
    self.context = "main"
    self.pd_names = []
    Main.__bases__ += (PD,)

  def completenames(self, text, *ignored):
    l = valid_context[self.context]
    return l

  def precmd(self, line):
    if not line:
      return line
    cmd = line.split()[0]
    if not cmd:
      print "%s not a valid command" % cmd
    elif cmd == "main":
      return line
    elif not cmd in valid_context[self.context]:
      print "%s not a valid command in the context %s" % (cmd, self.context)
    return line

  def do_main(self, line):
    self.context = "main"
    self.value = ""
    self.prompt = "> "

  def do_EOF(self, d):
    print "bye!"
    sys.exit()

  do_quit = do_exit = do_EOF

if __name__ == "__main__":
  m=Main()
  m.cmdloop()
