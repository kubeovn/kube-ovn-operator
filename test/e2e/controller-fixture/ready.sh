#!/usr/bin/env sh
set -eu

# Read the local server metadata on followers too. A newly running process
# cannot make the serial rollout advance before both databases are connected.
for database in OVN_Northbound OVN_Southbound; do
  case "${database}" in
    OVN_Northbound) stem=ovnnb ;;
    OVN_Southbound) stem=ovnsb ;;
  esac
  ovsdb-client --no-leader-only --timeout=1 query "unix:/var/run/ovn/${stem}_db.sock" \
    '["_Server",{"op":"select","table":"Database","where":[["name","==","'"${database}"'"],["connected","==",true]],"columns":["name"]}]' |
    grep -q '"name"[[:space:]]*:[[:space:]]*"'"${database}"'"'
done
