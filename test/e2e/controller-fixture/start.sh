#!/usr/bin/env sh
set -eu

# The controller handoff test uses the same native OVSDB processes as the
# member provisioner. The smoke script pre-seeds one durable clustered database
# member on each control-plane node before the Chart starts.
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
  echo "controller fixture node ${node_ip} is not one of the three control-plane addresses" >&2
  exit 1
fi

data_dir=/etc/ovn
run_dir=/var/run/ovn
mkdir -p "${data_dir}" "${run_dir}"
chmod 0700 "${data_dir}" "${run_dir}"

create_database() {
  file_name=$1
  raft_port=$2
  path="${data_dir}/${file_name}.db"
  if [ -e "${path}" ]; then
    local_address="tcp:${node_ip}:${raft_port}"
    if [ "$(ovsdb-tool db-local-address "${path}")" != "${local_address}" ]; then
      echo "${path} is bound to a different local address" >&2
      exit 1
    fi
    return
  fi
  echo "${path} was not pre-seeded for controller fixture member ${index}" >&2
  exit 1
}

create_database ovnnb 6643
create_database ovnsb 6644

nb_cid=$(ovsdb-tool db-cid "${data_dir}/ovnnb.db")
sb_cid=$(ovsdb-tool db-cid "${data_dir}/ovnsb.db")
client_addresses() {
  port=$1
  addresses=
  old_ifs=${IFS}
  IFS=,
  for candidate in ${master_nodes}; do
    if [ -n "${addresses}" ]; then
      addresses="${addresses},"
    fi
    addresses="${addresses}tcp:${candidate}:${port}"
  done
  IFS=${old_ifs}
  printf '%s' "${addresses}"
}
nb_clients=$(client_addresses 6641)
sb_clients=$(client_addresses 6642)

ovsdb-server --no-chdir \
  --pidfile="${run_dir}/ovnnb_db.pid" \
  --unixctl="${run_dir}/ovnnb_db.ctl" \
  --remote="punix:${run_dir}/ovnnb_db.sock" \
  --remote="ptcp:6641:${node_ip}" \
  "${data_dir}/ovnnb.db" &
nb_pid=$!
ovsdb-server --no-chdir \
  --pidfile="${run_dir}/ovnsb_db.pid" \
  --unixctl="${run_dir}/ovnsb_db.ctl" \
  --remote="punix:${run_dir}/ovnsb_db.sock" \
  --remote="ptcp:6642:${node_ip}" \
  "${data_dir}/ovnsb.db" &
sb_pid=$!

# Wait for both local IDL sockets before starting northd. This keeps the
# northd readiness signal meaningful and avoids a transient failed process
# racing the controller's first observation.
for _ in $(seq 1 60); do
  if ovsdb-client --timeout=1 query "unix:${run_dir}/ovnnb_db.sock" '["OVN_Northbound",{"op":"select","table":"NB_Global","where":[],"columns":["_uuid"]}]' >/dev/null 2>&1 \
    && ovsdb-client --timeout=1 query "unix:${run_dir}/ovnsb_db.sock" '["OVN_Southbound",{"op":"select","table":"SB_Global","where":[],"columns":["_uuid"]}]' >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

ovn-northd --no-chdir \
  --pidfile="${run_dir}/ovn-northd.pid" \
  --ovnnb-db="cid:${nb_cid},${nb_clients}" \
  --ovnsb-db="cid:${sb_cid},${sb_clients}" &
northd_pid=$!

# shellcheck disable=SC2329
cleanup() {
  kill "${northd_pid}" "${nb_pid}" "${sb_pid}" 2>/dev/null || true
  wait "${northd_pid}" 2>/dev/null || true
  wait "${nb_pid}" 2>/dev/null || true
  wait "${sb_pid}" 2>/dev/null || true
}
trap cleanup TERM INT EXIT

while kill -0 "${nb_pid}" 2>/dev/null && kill -0 "${sb_pid}" 2>/dev/null && kill -0 "${northd_pid}" 2>/dev/null; do
  sleep 1
done
exit 1
