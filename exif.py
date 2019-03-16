import os, sys
import struct
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

'''
def _read_int(f, bswap=1):
	b = array.array('L')
	print "item size %d" % ( b.itemsize)
	b.fromfile(f, 1)
	if bswap == 1:
		b.byteswap()
	size = b.pop()
	return size

def _read_short(f, bswap=1):
	b = array.array('H')
	b.fromfile(f,1)
	if bswap == 1:
		b.byteswap()
	val = b.pop()
	return val

def _read_byte(f):
	b = array.array('B')
	b.fromfile(f,1)
	val = b.pop()
	return val

def _read_int(f, bswap='big'):
	data = f.read(4)
	i = int.from_bytes(data, byteorder=bswap)
	return i

def _read_short(f, bswap='big'):
	data = f.read(2)
	i = int.from_bytes(data, byteorder=bswap)
	return i

def _read_byte(f):
	data = f.read(1)
	i = int.from_bytes(data, 'big')
	return i
'''
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

def _read_jpeg_hdr(f):
	'''
	read jpeg header and gobble everything until
	TIFF app. Set the offset of TIFF app.
	'''
	s = _read_short(f)
	if  s != 0xffd8 and s != 0xd8ff:
		print "Header Not ffd8 %x" % (s)
		return
	bswap = 1 if s == 0xffd8 else 0
	#print "File header %x" % (s)
        #print "bswap %d" % (bswap)
	while True:
                o = f.tell()
		#print "At %d" % o
		#print "At 0%o" % o
		#print "At 0x%x" % o
		marker = _read_byte(f)
		if marker != 0xff:
			print "Marker Not Found (0x%x)" % (marker)
			return False
		s = _read_byte(f)
		offset = f.tell()
		if s != 0xe1:
			len = _read_short(f, bswap)
			print "App marker 0x%x" % (s)
			#print "App len %d" % (len)
			f.seek(offset+len)
			continue
		break
	print "App1 Marker (Exif) %x" % (s)
	s = _read_short(f)
	print "App1 Marker Size %d" % (s)
	str=f.read(4)
	if str != "Exif":
		print "This has no Exif"
		return False
	s = _read_short(f)
	if s != 0x0:
		print "This has invalid \"Exif\""
	#print "Exif Validated; bswap %d" % (bswap)
	return True

def read_size(f):
	'''
	read jpeg image size
	'''
	s = _read_short(f)
	if s != 0xffd8 and s != 0xd8ff:
		return
	bswap = 1 if s == 0xffd8 else 0
	while True:
		marker = _read_short(f, bswap)
		if marker >> 8 != 0xff:
			print "marker not found 0x%x" % marker
			f.seek(0,0)
			return -1,-1
                print "marker 0x%x " % marker
		offset = f.tell()
		# https://en.wikipedia.org/wiki/JPEG#cite_note-21
                if marker & 0xFF >= 0xd0 and s <= 0xd7:
			print "passing 0x%x " % s
 			continue
		if marker & 0xFF != 0xc0:
			print "app 0x%x" % marker
			len = _read_short(f, bswap)
			print "len x%x" % len
			f.seek(offset+len)
			print "seeking %x" % (offset + len)
			continue
		break
	len = _read_short(f)
        b = _read_byte(f)
	w = _read_short(f)
	h = _read_short(f)
	f.seek(0,0)
        print "w %x l %x " % (w,h)
	return w,h


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
#            Exif VErsion 0221 (
		#print ("Testing....");
		s = ""
		for bytpos in (0,1,2,3):
			s=(("%c") % ( val >> (bytpos*8) & 0xff ))+s
		#print (s)
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
def read_tiff(f):
	global offset, bo
	offset = int(f.tell())
	s = _read_short(f)
	if s != 0x4949 and s != 0x4D4D:
		print "This has invalid byte-order"
		return
	#print "sss %x" % (s)
	if s == 0x4D4D:
		bo = 1 # 1 is big
	else:
		bo = 0 # 0 is little
	#print bo
	s = _read_short(f, bo)
	#print "%x" % (s)
	if s != 42:
		print "This has invalid signature (not 42)"
		return
	s = _read_int(f, bo)
	#print "Offset of next IFD 0x%x" % (s)
	saved_offset = int(f.tell())
	actual_offset = offset + s

	# jump to where first IFD entry is
	f.seek(actual_offset)
	s = _read_short(f, bo)
	#print "Number of dir entries %d" % (s)

	# here is where class IDF come into picture
	l = []
	for n in range(s):
		d = {}
		val = ""
		tag = _read_short(f, bo)
		#print ""
		print "Tag x%x" % (tag)
		d["tag"] = tag

		type = _read_short(f, bo)
		#print "Type %x" % (type)
		d["type"] = type

		cnt = _read_int(f, bo)
		#print "Count x%x" % (cnt)
		d["count"] = cnt

		offs = _read_int(f, bo)
		if unitsizeof(type) * cnt <= 4:
			#print "size(type) %d" % unitsizeof(type)
			d["offset"] = 0
			d["val"] = val = offs
			#print "Val " + str(d["val"])
		else:
			d["offset"] = offset + offs
		#print "Offset x%x" % (d["offset"])

		l.append(d)
		if val == "":
			s_offset = int(f.tell())
			f.seek(offset + offs)

			if type == 2:
				val = f.read(cnt)
				#print "Val (str) %s" % val
			elif type == 3:
				val = _read_short(f, bo)
				#print "Val (short) " + str(val)
			elif type == 4:
				val = _read_int(f, bo)
				#print "Val (int) " + str(val)
			elif type == 5:
				val = []
				val.append(_read_int(f, bo))
				val.append(_read_int(f, bo))
				reduce(val)
				#print "Val (long+long) " + str(val)
			f.seek(s_offset)
		#print "echo...."
		sval = convert(tag,val);
		d["val"] = sval
	return l


def read_SubIFD(f, l):
	global bo
	#print ""
	#print "Reading SubIFD..."
	count = _read_short(f, bo)
	#print "Count x%x" % (count)

	for n in range(count):
		d = {}
		val = ""
		tag = _read_short(f, bo)
		#print ""
		print "Tag x%x" % (tag)
		d["tag"] = tag

		type = _read_short(f, bo)
		#print "Type %x" % (type)
		d["type"] = type

		cnt = _read_int(f, bo)
		#print "Count x%x" % (cnt)
		d["count"] = cnt

		offs = _read_int(f, bo)
		if unitsizeof(type) * cnt <= 4:
			d["offset"] = 0
			d["val"] = val = offs
			#print "Val " + str(d["val"])
		else:
			d["offset"] = offset + offs
		#print "Offset x%x" % (d["offset"])

		l.append(d)
		if val == "":
			s_offset = int(f.tell())
			f.seek(offset + offs)

			if type == 2:
				val = f.read(cnt)
				#print "Val (str) %s" % val
			elif type == 3:
				val = _read_short(f, bo)
				#print "Val (short) " + str(val)
			elif type == 4:
				val = _read_int(f, bo)
				#print "Val (int) " + str(val)
			elif type == 5 or type == 10:
				val = []
				val.append(_read_int(f, bo))
				val.append(_read_int(f, bo))
				val=reduce(val)
				#print "Val (long+long) " + str(val)

			f.seek(s_offset)
		sval = convert(tag,val);
		d["val"] = sval


# convert time string from exif to struct_time
def time_str2time(time_str):
  return time.strptime(s, "%Y:%m:%d %H:%M:%S")


class Exif:
  def __init__(self, path, show=True, filter=None):
    self.file = path
    self.show = show
    self.filter = filter
    self.attr = self.read_exif(path)
    self.edic = self.revisit_attr()
    self.ctime = self.edic.get("Date Time Digitized", None)
    self.mtime = self.edic.get("Date Time Last Modified", None)
    self.orien = self.edic.get("Orientation", 0)
    self.orien = self.edic.get("Shutter Speed Value", 0)

  def read_exif(self,path):
    '''
    read_exif(path) returns a dictionary of "tag" and "val" along with other
    metadata from the exif of the file.
    '''
    f = open(path, "rb")
    w,h = read_size(f)
    print "width %d height %d" % (w,h)
    #print "Reading %s" % (self.file)
    sys.exit(0)
    if _read_jpeg_hdr(f):
      attr = read_tiff(f)
    else:
      print "No exif header found"
      attr = []
    for d in attr:
      if d["tag"] == 0x8769:
        #print d["val"]
        f.seek(d["val"] + offset)
        read_SubIFD(f, attr)
    #print attr
    return attr

  def revisit_attr(self):
    '''
    revisit_attr() takes attr from the read_exif and returns a filtered
    dictionary of "tag" and "val" fields.
    Filter: If specified, the "tags" to be filtered in list.
    '''
    if self.show:
      print "============================="
    l=self.attr
    dic = {}
    for d in l:
      tstr=hex(d["tag"])
      if tagdict.has_key(tstr):
        tdesc=tagdict[tstr]
      else:
        tdesc="Not Defined (%s)" % (tstr)
      tval = d["val"]
      if not self.filter or tdesc in self.filter:
        dic[tdesc] = tval
      if self.show:
        print "%30s: %s" % (tdesc, tval)
    dic["Path"] = self.file
    if self.show:
      print dic
    return dic

if __name__ == '__main__':
	path=argv[-1]
	e=Exif(path)
#	print_attr(attr)
