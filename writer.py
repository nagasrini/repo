import os
import sys
import re
sys.path.append('/home/nutanix/cluster/bin')
import env
import subprocess
import random
import time

import gflags

gflags.DEFINE_string("file", "", "file path")
gflags.DEFINE_string("pd_name", "", "pd name")
gflags.DEFINE_string("path", "", "NFS file path")
gflags.DEFINE_boolean("write", True, "write")
gflags.DEFINE_integer("size", 1024*1024*1024, "size of file")

FLAGS = gflags.FLAGS

def rand(num):
        fd = os.open("/dev/urandom", os.O_RDONLY)
        n = os.read(fd, 1);
        return (ord(n) % num)

def max(x,y):
    if x > y:
      return x
    return y

class writer():
  def __init__(self, file, offset, bs=4*1024, size=4*1024*1024*1024, mode="random"):
    self.fd = open(file, "w")
    self.file = file
    self.mode = mode
    self.blocksize = bs
    self.size = size
    self.offset = offset
    self.fd.seek(size)
    self.fd.write("x")
    self.fd.seek(0)
    self.fd.close()
    self.blkcnt = size / bs

  def write_one_block(self, offset):
    cmd = ["/usr/bin/dd", "if=/dev/urandom", "of=%s" % self.file, "count=1", "seek=%d" % offset, "conv=notrunc"]
    try:
      p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    except Exception as e:
      print e
      return
    o = p.communicate()

  def write(self):
    for n in range(self.blkcnt):
      offset = random.randint(0,self.blkcnt)
      name="off_%d" % offset
      p = mp.Process(target=write_one_block, name=name, args=(offset,))
      p.start()
      plist.append(p)

      if len(plist) < 200:
        continue

      while len(plist) > 100:
        for p in plist:
          if p.is_alive():
            continue
          p.join()
          plist.remove(p)


def frag_write(file, size):
    f=open(file, "w")
    f.seek(size)
    f.write("x")
    f.seek(0)
    f.close()
    off_list = []
    print "range %d" % (size/512)
    for n in range(size/512):
      off=random.randint(0,size/512)
      cmd = ["/usr/bin/dd", "if=/dev/urandom", "of=%s" % file, "count=1", "seek=%d" % off, "conv=notrunc"]
      try:
        p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
      except Exception as e:
        print e
        return
      p.communicate()
      off_list.append(off)
      #print p.returncode
    return

def protect(pd, path):
  cmd = ["/home/nutanix/prism/cli/ncli", "pd", "protect", "name=%s" % pd, "files=%s" % path]
  try:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  except Exception as e:
    print e
    return
  o=p.communicate()
  print o[0]
  return p.returncode == 0

def snapit(pd):
  cmd = ["/home/nutanix/prism/cli/ncli", "pd", "add-one-time-snapshot", "name=%s" % pd,  "retention-time=%d" % 100]
  try:
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
  except Exception as e:
    print e
    return
  o=p.communicate()
  print o[0]
  return p.returncode == 0



if __name__ == "__main__":
    args = gflags.FLAGS(sys.argv)
    if FLAGS.write:
      frag_write(FLAGS.file, FLAGS.size)
    else:
      protect(FLAGS.pd_name, FLAGS.path)
      print "protected"
      for i in range(2000):
        print "taking snapshot"
        snapit(FLAGS.pd_name)
        time.sleep(100)
