import os, sys
import struct
import time
from sys import argv

#
# http://www.cipa.jp/english/hyoujunka/kikaku/pdf/DC-008-2010_E.pdf
#

#size of defined Exif types indexed by the types value themselves
typ = ( 0, 1, 1, 2, 4, 8, 0, 1, 0, 4, 8 )
tagdict = {
    "0x100": "Image Width",
"0x101": "Image Height",
    "0x10e": "Image Description",
    "0x10f": "Manufacturer",
    "0x110": "Model",
    "0x112": "Orientation",
    "0x11a": "X Resolution",
    "0x11b": "Y Resolution",
    "0x128": "Resolution Unit",
    "0x131": "Software",
    "0x132": "Date Time Last Modified",
    "0x13e": "White Point",
    "0x13f": "Primary Chromaticities",
    "0x211": "YCbCr Coefficients",
    "0x213": "YCbCr Positioning",
    "0x214": "Reference Black White",
    "0x8298": "Copyright",
    "0x8769": "Exif Offset",
    "0x829a": "Exposure Time",
    "0x829d": "F-Number",
    "0x8822": "Exposure Program",
    "0x8827": "ISO Speed Ratings",
    "0x9000": "Exif Version",
    "0x9003": "Date Time Original",
    "0x9004": "Date Time Digitized",
    "0x9101": "Components Configuration",
    "0x9102": "Compressed BitsPerPixel ",
    "0x9201": "Shutter Speed Value",
    "0x9202": "Aperture Value",
    "0x9203": "Brightness Value",
    "0x9204": "Exposure BiasValue ",
    "0x9205": "Max Aperture Value",
    "0x9206": "Subject Distance",
    "0x9207": "Metering Mode",
    "0x9208": "Light Source",
    "0x9209": "Flash",
    "0x920a": "Focal Length",
    "0x927c": "Maker Note",
    "0x9286": "User Comment",
    "0x9290": "Subsec Time",
    "0x9291": "Subsec Time Original",
    "0x9292": "Subsec Time Digitized",
    "0xa000": "Flash Pix Version",
    "0xa001": "Color Space",
    "0xa002": "Exif Image Width",
    "0xa003": "Exif Image Height",
    "0xa004": "Related Sound File",
    "0xa005": "Exif Interoperability Offset",
    "0xa20e": "Focal Plane X Resolution",
    "0xa20f": "Focal Plane Y Resolution",
    "0xa210": "Focal Plane Resolution Unit",
    "0xa215": "Exposure Index",
    "0xa217": "Sensing Method",
    "0xa300": "File Source",
    "0xa301": "Scene Type",
    "0xa302": "CFA Pattern",
    "0xa401": "Custom Rendered",
    "0xa402": "Exposure Mode",
    "0xa403": "White Balance",
    "0xa404": "Digital Zoom Ratio",
    "0xa405": "Focal Length In 35mm",
    "0xa406": "Scene Capture Type",
    "0xa407": "Gain Control",
    "0xa408": "Contrast",
    "0xa409": "Saturation",
}
offset = 0

def unitsizeof(type):
  return typ[type]

def _read_byte(f):
  val = struct.unpack("B", f.read(1))[0]
  return val

def _read_short(f, bswap=1):
  format=">" if bswap == 1 else "<"
  format += "H"
  val = struct.unpack(format, f.read(2))[0]
  return val

def _read_int(f, bswap=1):
  format=">" if bswap == 1 else "<"
  format += "I"
  val = struct.unpack(format, f.read(4))[0]
  return val

def convert(tag, val):
  #print "tag %x" % (tag)
  if tag == 0xA408:
    if val == 0:
      return ("Normal")
    if val == 1:
      return ("Soft")
    if val == 2:
      return ("Hard")
  elif tag == 0xA409:
    if val == 0:
      return ("Normal")
    if val == 1:
      return ("Low")
    if val == 2:
      return ("High")
  elif tag == 0x9000 or tag == 0xa000:
      #Exif VErsion 0221 (
      #print ("Testing....");
      s = ""
      for bytpos in (0,1,2,3):
        s = (("%c") % ( val >> (bytpos*8) & 0xff )) + s
      return (s)
  elif tag == 0x8827:
    return((val>>16)&0xffff)
  else:
    return val

def reduce(l):
  if l[0] == 0:
    return ("%d/%d") % (l[1], l[0])
  b = l[1]/l[0]
  if b > 1:
    l[0] = 1
    l[1] = b
    return ("%d/%d") % (l[0], l[1])
  else:
    b = float(l[0])/float(l[1])
    return ("%.2f") % (b)

# convert time string from exif to struct_time
def time_str2time(time_str):
  return time.strptime(time_str, "%Y:%m:%d %H:%M:%S")

class Exif:
  def __init__(self, path, filter=None, debug=False, show=False):
    self.file = path
    self.show = True if show else debug
    self.filter = filter
    self.debug = debug
    self.width = -1
    self.height = -1
    self.attr = []
    if not self.read_exif(path):
      raise Exception("Not a suitable format")
    self.edic = self.revisit_attr()
    self.ctime = self.edic.get("Date Time Digitized", "")
    self.mtime = self.edic.get("Date Time Last Modified", None)
    self.orien = self.edic.get("Orientation", 0)
    self.orien = self.edic.get("Shutter Speed Value", 0)
    self.date = time_str2time(self.edic.get("Date Time Digitized", "").rstrip(u"\x00"))
    self.size = os.stat(self.file).st_size

  def __eq__(self, other):
    return self.date == other.date and self.height == other.height and \
      self.width == other.width and self.size == other.size

  def __ne__(self, other):
    return self.date != other.date

  def __lt__(self, other):
    return self.date < other.date

  def __le__(self, other):
    return self.date <= other.date

  def __gt__(self, other):
    return self.date > other.date

  def __ge__(self, other):
    return self.date >= other.date

  def dprint(self, msg):
    if self.debug:
      print "DEBUG: %s" % msg

  def set_image_size(self):
    '''
    read jpeg image size
    '''
    # start from beginning
    self.fp.seek(0,0)
    s = _read_short(self.fp)
    if s != 0xffd8 and s != 0xd8ff:
      self.dprint("Not a Jpeg File")
      return
    bswap = 1 if s == 0xffd8 else 0
    while True:
      marker = _read_short(self.fp, bswap)
      if marker >> 8 != 0xff:
        self.dprint("ERROR: Unexpected marker found 0x%x" % marker)
        self.fp.seek(0,0)
        return
      self.dprint("DEBUG: marker 0x%x " % marker)
      self.offset = self.fp.tell()
      # https://en.wikipedia.org/wiki/JPEG#cite_note-21
      if marker & 0xFF >= 0xd0 and marker <= 0xd7:
        self.dprint("passing 0x%x " % s)
        continue
      if marker & 0xFF == 0xd9:
        self.dprint("Encountered EOF")
        return
      if marker & 0xFF != 0xc0:
        self.dprint("app 0x%x" % marker)
        len = _read_short(self.fp, bswap)
        self.dprint("len x%x" % len)
        self.fp.seek(self.offset+len)
        self.dprint("seeking %x" % (offset + len))
        continue
      break
    _ = _read_short(self.fp)
    _ = _read_byte(self.fp)
    self.width = _read_short(self.fp)
    self.height = _read_short(self.fp)

  def read_tiff(self):
    global offset, bo
    self.offset = int(self.fp.tell())
    s = _read_short(self.fp)
    if s != 0x4949 and s != 0x4D4D:
      self.dprint("This has invalid byte-order")
      return
    if s == 0x4D4D:
      bo = 1 # 1 is big
    else:
      bo = 0 # 0 is little
    #print bo
    s = _read_short(self.fp, bo)
    #print "%x" % (s)
    if s != 42:
      self.dprint("This has invalid signature (not 42)")
      return
    s = _read_int(self.fp, bo)
    #print "Offset of next IFD 0x%x" % (s)
    saved_offset = int(self.fp.tell())
    actual_offset = self.offset + s

    # jump to where first IFD entry is
    self.fp.seek(actual_offset)
    s = _read_short(self.fp, bo)
    #print "Number of dir entries %d" % (s)

    # here is where class IDF come into picture
    l = []
    for n in range(s):
      d = {}
      val = ""
      tag = _read_short(self.fp, bo)
      self.dprint("Tag x%x" % (tag))
      d["tag"] = tag

      type = _read_short(self.fp, bo)
      #print "Type %x" % (type)
      d["type"] = type

      cnt = _read_int(self.fp, bo)
      #print "Count x%x" % (cnt)
      d["count"] = cnt

      offs = _read_int(self.fp, bo)
      if unitsizeof(type) * cnt <= 4:
        #print "size(type) %d" % unitsizeof(type)
        d["offset"] = 0
        d["val"] = val = offs
        #print "Val " + str(d["val"])
      else:
        d["offset"] = self.offset + offs
      #print "Offset x%x" % (d["offset"])

      l.append(d)
      if val == "":
        s_offset = int(self.fp.tell())
        self.fp.seek(self.offset + offs)

        if type == 2:
          val = self.fp.read(cnt)
          #print "Val (str) %s" % val
        elif type == 3:
          val = _read_short(self.fp, bo)
          #print "Val (short) " + str(val)
        elif type == 4:
          val = _read_int(self.fp, bo)
          #print "Val (int) " + str(val)
        elif type == 5:
          val = []
          val.append(_read_int(self.fp, bo))
          val.append(_read_int(self.fp, bo))
          reduce(val)
          #print "Val (long+long) " + str(val)
        self.fp.seek(s_offset)
      #print "echo...."
      sval = convert(tag, val);
      d["val"] = sval
    self.attr = l


  def read_SubIFD(self):
    global bo
    #print ""
    #print "Reading SubIFD..."
    count = _read_short(self.fp, bo)
    #print "Count x%x" % (count)

    for n in range(count):
      d = {}
      val = ""
      tag = _read_short(self.fp, bo)
      self.dprint("Tag x%x" % (tag))
      d["tag"] = tag

      type = _read_short(self.fp, bo)
      #print "Type %x" % (type)
      d["type"] = type

      cnt = _read_int(self.fp, bo)
      #print "Count x%x" % (cnt)
      d["count"] = cnt

      offs = _read_int(self.fp, bo)
      if unitsizeof(type) * cnt <= 4:
        d["offset"] = 0
        d["val"] = val = offs
        #print "Val " + str(d["val"])
      else:
        d["offset"] = self.offset + offs
      #print "Offset x%x" % (d["offset"])

      self.attr.append(d)
      if val == "":
        s_offset = int(self.fp.tell())
        self.fp.seek(self.offset + offs)

        if type == 2:
          val = self.fp.read(cnt)
          #print "Val (str) %s" % val
        elif type == 3:
          val = _read_short(self.fp, bo)
          #print "Val (short) " + str(val)
        elif type == 4:
          val = _read_int(self.fp, bo)
          #print "Val (int) " + str(val)
        elif type == 5 or type == 10:
          val = []
          val.append(_read_int(self.fp, bo))
          val.append(_read_int(self.fp, bo))
          val=reduce(val)
          #print "Val (long+long) " + str(val)

        self.fp.seek(s_offset)
      sval = convert(tag,val);
      d["val"] = sval

  def _read_jpeg_hdr(self):
    '''
    read jpeg header and gobble everything until
    TIFF app. Set the offset of TIFF app.
    '''
    # start from beginning
    self.fp.seek(0,0)
    s = _read_short(self.fp)
    if  s != 0xffd8 and s != 0xd8ff:
      self.dprint("Header Not ffd8 %x" % (s))
      return
    bswap = 1 if s == 0xffd8 else 0
    while True:
      marker = _read_short(self.fp, bswap)
      if marker >> 8 != 0xff:
        self.dprint("ERROR: Unexpected marker found 0x%x" % marker)
        return False
      self.dprint("DEBUG: marker 0x%x " % marker)
      self.offset = self.fp.tell()
      if marker & 0xFF == 0xd9:
        self.dprint("Encountered EOF")
        return False
      if marker & 0xFF >= 0xd0 and marker <= 0xd7:
        self.dprint("passing 0x%x " % s)
        continue
      if marker & 0xFF != 0xe1:
        self.dprint("app 0x%x" % marker)
        len = _read_short(self.fp, bswap)
        self.dprint("len x%x" % len)
        self.fp.seek(self.offset+len)
        self.dprint("seeking %x" % (offset + len))
        continue
      break
    self.dprint("App1 Marker (Exif) %x" % (marker))
    len = _read_short(self.fp)
    self.dprint("App1 Marker Size %d" % (len))
    str=self.fp.read(4)
    if str != "Exif":
      self.dprint("This has no Exif")
      return False
    s = _read_short(self.fp)
    if s != 0x0:
      self.dprint("This has invalid \"Exif\"")
      return False
    self.dprint("Exif Validated; bswap %d" % (bswap))
    return True

  def read_exif(self, path):
    '''
    read_exif(path) returns a dictionary of "tag" and "val" along with other
    metadata from the exif of the file.
    '''
    self.fp = open(path, "rb")
    self.set_image_size()
    self.dprint("width %d height %d" % (self.width,self.height))
    #print "Reading %s" % (self.file)
    if self._read_jpeg_hdr():
      self.read_tiff()
    else:
      self.dprint("No exif header found")
      attr = []
      return False
    for d in self.attr:
      # if SubIFD Tag is present, read SubIFD; value is offset to it.
      if d["tag"] == 0x8769:
        #print d["val"]
        self.fp.seek(d["val"] + self.offset)
        self.read_SubIFD()
    #print self.attr
    return True

  def revisit_attr(self):
    '''
    revisit_attr() takes attr from the read_exif and returns a filtered
    dictionary of "tag" and "val" fields.
    Filter: If specified, the "tags" to be filtered in list.
    '''
    if self.show:
      self.dprint("=============================")
    dic = {}
    for d in self.attr:
      tstr=hex(d["tag"])
      if tagdict.has_key(tstr):
        tdesc=tagdict[tstr]
      else:
        tdesc="Not Defined (%s)" % (tstr)
      tval = d["val"]
      if not self.filter or tdesc in self.filter:
        dic[tdesc] = tval
      if self.show:
        self.dprint("%30s: %s" % (tdesc, tval))
    dic["Path"] = self.file
    if self.show:
      self.dprint(dic)
    return dic

if __name__ == '__main__':
    path=argv[-1]
    e=Exif(path, debug=True)
#    print_attr(attr)
