#!/bin/sh
set -eu

command_name=${0##*/}
master_nodes=${MASTER_NODES:?MASTER_NODES is required}
node_ip=${NODE_IP:?NODE_IP is required}
index=-1
candidate_index=0
old_ifs=${IFS}
IFS=,
for candidate in ${master_nodes}; do
  if [ "${candidate}" = "${node_ip}" ]; then
    index=${candidate_index}
    break
  fi
  candidate_index=$((candidate_index + 1))
done
IFS=${old_ifs}
if [ "${index}" -lt 0 ] || [ "${index}" -gt 2 ]; then
  echo "fixture node ${node_ip} is not one of ${master_nodes}" >&2
  exit 1
fi

server_id() {
  case "$1" in
    0) printf '%s' aaaa0000-1111-2222-3333-444444444444 ;;
    1) printf '%s' bbbb0000-1111-2222-3333-444444444444 ;;
    2) printf '%s' cccc0000-1111-2222-3333-444444444444 ;;
  esac
}

node_at() {
  old_ifs=${IFS}
  IFS=,
  position=0
  for candidate in ${master_nodes}; do
    if [ "${position}" -eq "$1" ]; then
      printf '%s' "${candidate}"
      IFS=${old_ifs}
      return
    fi
    position=$((position + 1))
  done
  IFS=${old_ifs}
  exit 1
}

database_from_args() {
  case "$*" in
    *OVN_Southbound*|*ovnsb_db*) printf '%s' OVN_Southbound ;;
    *) printf '%s' OVN_Northbound ;;
  esac
}

cluster_status() {
  database=$1
  case "${database}" in
    OVN_Northbound) cluster_id=dddd0000-1111-2222-3333-444444444444; raft_port=6643 ;;
    OVN_Southbound) cluster_id=eeee0000-1111-2222-3333-444444444444; raft_port=6644 ;;
    *) echo "unsupported database ${database}" >&2; exit 1 ;;
  esac
  local_id=$(server_id "${index}")
  if [ "${index}" -eq 0 ]; then
    role=leader
    leader=self
  else
    role=follower
    leader=member-0
  fi
  printf 'Name: %s\n' "${database}"
  printf 'Cluster ID: %.4s (%s)\n' "${cluster_id}" "${cluster_id}"
  printf 'Server ID: %.4s (%s)\n' "${local_id}" "${local_id}"
  printf 'Address: tcp:%s:%s\n' "${node_ip}" "${raft_port}"
  printf 'Status: cluster member\nRole: %s\nTerm: 7\nLeader: %s\n' "${role}" "${leader}"
  printf 'Log: [1, 21]\nEntries not yet committed: 0\nEntries not yet applied: 0\nConnections:'
  for peer in 0 1 2; do
    if [ "${peer}" -ne "${index}" ]; then
      printf ' ->member-%s' "${peer}"
    fi
  done
  printf '\nServers:\n'
  for peer in 0 1 2; do
    peer_id=$(server_id "${peer}")
    printf 'member-%s (%.4s at tcp:%s:%s)' "${peer}" "${peer_id}" "$(node_at "${peer}")" "${raft_port}"
    if [ "${peer}" -eq "${index}" ]; then
      printf ' (self)'
    fi
    printf '\n'
  done
}

database_query() {
  database=$(database_from_args "$@")
  case "${database}" in
    OVN_Northbound) cluster_id=dddd0000-1111-2222-3333-444444444444; client_port=6641 ;;
    OVN_Southbound) cluster_id=eeee0000-1111-2222-3333-444444444444; client_port=6642 ;;
  esac
  if printf '%s' "$*" | grep -q 'columns.*connections'; then
    printf '[{"rows":[{"connections":["uuid","12345678-1111-2222-3333-444444444444"]}]},{"rows":[{"_uuid":["uuid","12345678-1111-2222-3333-444444444444"],"target":"ptcp:%s:0.0.0.0"}]}]\n' "${client_port}"
    return
  fi
  local_id=$(server_id "${index}")
  if [ "${index}" -eq 0 ]; then leader=true; else leader=false; fi
  printf '[{"rows":[{"name":"%s","model":"clustered","connected":true,"leader":%s,"cid":["uuid","%s"],"sid":["uuid","%s"],"index":20}]}]\n' "${database}" "${leader}" "${cluster_id}" "${local_id}"
}

case "${command_name}" in
  ovs-appctl)
    case "$*" in
      *cluster/status*) cluster_status "${5}" ;;
      *ovsdb-server/list-remotes*)
        case "$*" in *ovnsb_db*) printf 'ptcp:6642:0.0.0.0\n' ;; *) printf 'ptcp:6641:0.0.0.0\n' ;; esac
        ;;
      *) echo "unsupported ovs-appctl request: $*" >&2; exit 1 ;;
    esac
    ;;
  ovsdb-client)
    case "$*" in
      *backup*) echo 'fixture does not perform native backups' >&2; exit 1 ;;
      *) database_query "$@" ;;
    esac
    ;;
  ovn-appctl)
    case "$*" in
      *nb-connection-status*|*sb-connection-status*) printf 'connected\n' ;;
      *status*) if [ "${index}" -eq 0 ]; then printf 'Status: active\n'; else printf 'Status: standby\n'; fi ;;
      *) echo "unsupported ovn-appctl request: $*" >&2; exit 1 ;;
    esac
    ;;
  *) echo "unsupported fixture command ${command_name}" >&2; exit 1 ;;
esac
