import sys
sys.path.append("/home/nutanix/serviceability/bin")
import env
from util.storage.wal_cassandra_backend import WALCassandraBackend
from util.storage.write_ahead_log import WriteAheadLog
from cerebro.master.cerebro_master_WAL_pb2 import MasterWALRecordProto
import threading
from functools import partial

def recover_wal_record(ev, buf, is_delta):
  if not buf:
    ev.set()
    return
  wal_proto = MasterWALRecordProto()
  wal_proto.ParseFromString(buf[4:]) # upto 4 bytes the buf represents the size
  print "##\n%s" % wal_proto

def WAL_recovery():
  backend = WALCassandraBackend("cerebro_master")
  if not backend.initialize():
    print("Error initializing WAL")
    return
  wal = WriteAheadLog(backend)
  ev = threading.Event()
  recovery_cb = partial(recover_wal_record, ev)
  wal.start_recovery(recovery_cb)
  ev.wait()


if __name__ == "__main__":
  WAL_recovery()
