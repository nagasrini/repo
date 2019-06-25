
import sys
sys.path.append("/home/nutanix/serviceability/bin")
import env
import boto
from boto.s3.connection import S3Connection
from util.misc.cloud_credentials import CloudCredentials
from boto.s3.key import Key
from boto.exception import S3DataError
c=CloudCredentials()
k=c.get_aws_keys()
aws_key = k[0]
secret_key = k[1]
ep="s3.amazonaws.com"

#aws_key = 'AKIAIXS5UZSPRPRLV3VQ'
#secret_key = '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'

print aws_key, secret_key
#'AKIAIXS5UZSPRPRLV3VQ', 'vWERGs1ZiqBEDqOBDLMKJRR7EYDmFTmfdhqG8UDn'
s3conn = S3Connection(aws_key, secret_key, host=ep)
if not s3conn:
  print "ERROR: s3conn failed"
  sys.exit(1)
# s3conn.get_all_buckets()
bucket = s3conn.get_bucket("ntnx-3461279735544769517-77221342941104975-9")
if not bucket:
  print "ERROR: bucket not found"
  sys.exit(1)
key=Key(bucket)
key.key = 'health_check_key'
try:
  n=key.set_contents_from_string('health_check_key')
except S3DataError as e:
  print "error %s" % e
  sys.exit(1)
print n, "succeeded"
