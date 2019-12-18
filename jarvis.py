import sys
import json
import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

base_url="https://jarvis.eng.nutanix.com/api/v1"

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


'''
Run from any computer having access to jarvis
$ python ~/python/repo/jarvis.py name=asifali.jamadar

'''

def get_user_id(name="nagaraj.srinivasa"):
  print("Querying uid for name %s" % name)
  url = base_url + "/users?search=%s" % name
  r = requests.get(url, verify=False)
  if not r.ok:
    return None
  d = json.loads(r.content)
  if not len(d) or not len(d['data']):
    return None
  #print(d['data'])
  uid = d['data'][0]['_id']['$oid']
  return uid


def get_clusters(uid):
  url = base_url + "/users/%s/clusters" % uid
  r = requests.get(url, verify=False)
  if not r.ok:
    return None
  clusters_data = json.loads(r.content)['data']
  clusters = []
  for cl in clusters_data:
    cluster = {}
    cluster['id'] = cl['_id']
    cluster['name'] = cl['name']
    #print(cl['nodes_cache'])
    if cl.get('nodes_cache'):
      cluster['hv_type'] = cl['nodes_cache']['hypervisor_type']
      cluster['nos_version'] = cl['nodes_cache']['svm_version']
      if cl['nodes_cache']['hypervisor_type'] == 'vsphere':
        cluster['vcenter'] = cl['nodes_cache']['vsphere']['vcenter']
    #print("##%s %s" % (cl['name'], cl['_id']))
    url = base_url + "/clusters/%s/nodes" % cl['_id']['$oid']
    r = requests.get(url, verify=False)
    if not r.ok:
      print("%s nodes null" % cl['_id'])
    nodes_data = json.loads(r.content)['data']
    for node in nodes_data:
      node_dict = {}
      nodes = cluster.get('nodes', [])
      node_dict['svm_ip'] = node['svm_ip']
      node_dict['host_ip'] = node['hypervisor']['ip']
      nodes.append(node_dict)
      cluster['nodes'] = nodes
    clusters.append(cluster)
  return clusters

if __name__ == "__main__":
  try:
    sys.path.append('/home/nutanix/cluster/bin')
    import env
    import gflags
    gflags.DEFINE_string("name", "", "name")
    FLAGS = gflags.FLAGS
    argv = FLAGS(sys.argv)
    name = FLAGS.name
  except Exception as e:
    if len(sys.argv) > 1 and sys.argv[1].split('=')[0] == 'name':
      name = sys.argv[1].split('=')[1]
    else:
      name = "nagaraj.srinivasa"
  if not len(name):
    sys.exit(0)
  uid = get_user_id(name)
  if not uid:
    print("user id for %s not found" % name)
    sys.exit(0)
  print("User:\n\t%s, %s" % (name, uid))
  cs = get_clusters(uid)
  if not cs:
    print("No cluster found for %s" % name)
  for c in cs:
    print("%s (id = %s)\t%s" % (c['name'], c['id']['$oid'], c['hv_type']))
    if c['hv_type'] == 'vsphere':
      vc = c['vcenter']
      print("\tvCenter: ip: %s\t user: %s:%s" % (vc['ip'], vc['user'], vc['passwd']))
    for n in c['nodes']:
      print("\tsvm_ip: %s;\thost_ip: %s" % (n['svm_ip'], n['host_ip']))
    print("\n")