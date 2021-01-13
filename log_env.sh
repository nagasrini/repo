function get_cluster_id {
  cluster_ids=$(/bin/ls -d *-PE-* | sed 's/^.*-\([0-9]*\)-\([0-9]*\)-PE-.*/\2/' | sort -u | xargs)
  cnt=$(echo $cluster_ids | wc -w)
  if [[ $cnt != 1 ]]; then
    echo "WARN: Are there logs from many clusters? Make sure you have logs from one cluster"
  fi
  echo "cluster ids : $cluster_ids"
}

function get_zeus_config {
  id=$1
  get_cluster_id
  #echo "cluster ids : $cluster_ids"
  a_zc_path=$(find *$id*/cvm_logs/sysstats/zeus* | head -1)
}

function get_svm_ips {
  get_zeus_config
  svm_ips=$(grep service_vm_external_ip $a_zc_path | sort -u| cut -d'"' -f2)
  echo $svm_ips
}

function get_pd_list {
  get_cluster_id
  id=${1:cluster_id}
  a_pd_list_path=$(find *$id*/cvm_config/protection_domains/list_pds.txt | head -1)
  pd_list=$(sed 's/\"protection_domain_name/\n&/g' $a_pd_list_path | grep protection_domain | cut -d',' -f1 | cut -d'"' -f4| sort -u)
  echo $pd_list
}


## Process list_pds.txt to get schedules of each PD
read -r -d "" pd_sched_cmd <<EOF
import sys, json;
fp=open(sys.stdin)
jsd=json.load(fp)['entities']
for n in jsd:
  print(json.dumps(n['cron_schedules'], indent=2))
EOF
function get_pd_schedules {
  get_cluster_id
  id=${1:cluster_id}
  a_pd_list_path=$(find *$id*/cvm_config/protection_domains/list_pds.txt | head -1)
  print $a_pd_list_path
  cat $a_pd_list_path | python3.4 -c "$pd_sched_cmd"
}


## Process list_pds.txt to get details
read -r -d "" pd_cmd <<EOF
import sys, json;
jsd=json.load(sys.stdin)['entities']
for n in jsd:
  print("pd: %s active: %s vstore_id %s" % (n['name'], n['active'], n['vstore_id']))
EOF

function get_pd_details {
  get_cluster_id
  id=${1:cluster_id}
  a_pd_list_path=$(find *$id*/cvm_config/protection_domains/list_pds.txt | head -1)
  cat $a_pd_list_path | python3.4 -c "$pd_cmd"
}


read -r -d "" rs_cmd <<EOF
import sys, json;
jsd=json.load(sys.stdin)['entities']
for n in jsd:
  print("remote_site: %s remote_ip_ports: %s remote_cluster_id: %s vstore_map: %s" % (n['name'], n['remote_ip_ports'], n['cluster_id'], n['vstore_name_map']))
EOF

function get_rs_details {
  get_cluster_id
  id=${1:cluster_ids}
  a_rs_list_path=$(find *$id*/cvm_config/protection_domains/list_remote_sites.txt | head -1)
  cat $a_rs_list_path | python3.4 -c "$rs_cmd"
}


function churn_logs_for_metro {
  METRO_SIGN_FILE=/tmp/tmp.metro_cerebro
  cat <<EOF > $METRO_SIGN_FILE
METRO-TRNSTN
StretchPingRemote
EOF
  get_cluster_id
  id=${1:cluster_ids}
  # improve regex for cerebro/stargate files
  grep -f $METRO_SIGN_FILE *$id*/cvm_logs/[sc][te][ar]*.ntnx*INFO*
 
  rm $METRO_SIGN_FILE
}

function get_metro_events {
  #churn_logs_for_metro | sed -n 's/^.*PE-\([0-9]\.*\)\/cvm_logs\/\(.*\)\.ntnx.*INFO.*:\(I[0-9]* [0-9:\.]*\) [0-9]* \(.*\)\] \(.*\)/|| \2 | \2 | \3 | \4 | \5/p'
  churn_logs_for_metro | sed 's/^.*PE-\([0-9\.]*\)\/.*\/\(.*\)\.ntnx.*INFO.*:\(I[0-9]* [0-9:\.]*\) [0-9]* \(.*\)\] \(.*\)/| \1 | \2 | \3 | \4 | \5/'
}

## If this is run on live cluster environment

# gets space separated list of PDs
function get_pd_list {
  out=`cerebro_cli list_protection_domains list_entity_centric_protection_domains=true 2> /dev/null`;
  echo "$out `cerebro_cli list_protection_domains 2> /dev/null`" | grep name | cut -f2 -d'"'
}

# gets consistency groups of all active PDs
function get_pd_cgs {
  head="\nConsistency Group Info for all the active PDs\n"
  for pd in `get_pd_list`
  do
    out=`cerebro_cli query_protection_domain $pd list_consistency_groups=true 2> /dev/null`
    echo "$out" | grep -q "consistency_group_vec"
    if [[ $? == 0 ]]; then
      echo -e "$head"
      head=""
      echo -e "\nPD: $pd\n"
      echo -e "$out"
    fi
  done
}