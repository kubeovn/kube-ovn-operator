#!/usr/bin/env python3
"""Exercise real agents only in the disposable controller-handoff Kind fixture."""

import datetime
import http.client
import json
import re
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path


CONTEXT = "kind-kube-ovn-operator-controller-e2e"
NAMESPACE = "kube-ovn-operator-system"
INSTALLATION = "controller-retirement"
DATABASES = (("OVN_Northbound", "ovnnb", "NB_Global"),
             ("OVN_Southbound", "ovnsb", "SB_Global"))
MARKER = "operator-fence-e2e"


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def eventually(description, predicate, timeout=40):
    deadline = time.monotonic() + timeout
    while True:
        if predicate():
            return
        if time.monotonic() >= deadline:
            raise AssertionError(f"timed out: {description}")
        time.sleep(0.5)


def kubectl(*arguments, data=None):
    result = subprocess.run(
        ["kubectl", "--context", CONTEXT, "--namespace", NAMESPACE,
         "--request-timeout=15s", *arguments],
        input=data, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=25,
        check=True,
    )
    return result.stdout


def resource(kind, name):
    return json.loads(kubectl("get", kind, name, "-o", "json"))


class Fixture:
    def __init__(self, certificate_dir):
        installation = resource("koi", INSTALLATION)
        self.installation_uid = installation["metadata"]["uid"]
        pods = json.loads(kubectl("get", "pods", "-l", "app=ovn-central", "-o", "json"))
        self.pods = [pod for pod in pods["items"]
                     if not pod["metadata"].get("deletionTimestamp")]
        check(len(self.pods) == 3, "expected exactly three live central Pods")
        check(len({pod["spec"]["nodeName"] for pod in self.pods}) == 3,
              "central Pods must span three Kind nodes")
        self.endpoints = {}
        lease_names = set()
        for pod in self.pods:
            check(any(c["type"] == "Ready" and c["status"] == "True"
                      for c in pod["status"].get("conditions", [])),
                  "central Pod is not ready")
            agents = [c for c in pod["spec"]["containers"] if c["name"] == "kube-ovn-fence-agent"]
            check(len(agents) == 1, "central Pod lacks its fence-agent")
            lease_names.update(a.split("=", 1)[1] for a in agents[0]["args"]
                               if a.startswith("--lease-name="))
            self.endpoints[pod["metadata"]["name"]] = f'https://{pod["status"]["podIP"]}:8443'
        check(len(lease_names) == 1, "agents must observe the same named Lease")
        self.lease_name = lease_names.pop()
        record = resource("lease", self.lease_name)
        check(any(o.get("uid") == self.installation_uid and o.get("kind") == "KubeOVNInstallation"
                  for o in record["metadata"].get("ownerReferences", [])),
              "Lease is not owned by the fixture installation")
        self.record = record
        tls = ssl.create_default_context(cafile=str(certificate_dir / "ca.crt"))
        tls.minimum_version = ssl.TLSVersion.TLSv1_3
        tls.load_cert_chain(certificate_dir / "client.crt", certificate_dir / "client.key")
        anonymous_tls = ssl.create_default_context(cafile=str(certificate_dir / "ca.crt"))
        anonymous_tls.minimum_version = ssl.TLSVersion.TLSv1_3
        anonymous = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=anonymous_tls))
        for endpoint in self.endpoints.values():
            try:
                with anonymous.open(endpoint + "/healthz", timeout=5):
                    raise AssertionError("agent accepted a client without an mTLS certificate")
            except (urllib.error.URLError, ssl.SSLError, http.client.RemoteDisconnected):
                pass
        self.http = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=tls))

    def request(self, pod, path, body=None, headers=None):
        request = urllib.request.Request(self.endpoints[pod] + path, data=body, headers=headers or {})
        try:
            with self.http.open(request, timeout=15) as response:
                return response.status, response.read()
        except urllib.error.HTTPError as error:
            return error.code, error.read()

    def hold(self, holder, duration=600):
        # Replace with the observed resourceVersion to detect an unexpected
        # reconciler instead of silently overwriting a concurrent Lease writer.
        self.record["spec"].update({
            "holderIdentity": holder,
            "leaseDurationSeconds": duration,
            "leaseTransitions": self.record["spec"].get("leaseTransitions", 0) + 1,
            "renewTime": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="microseconds").replace("+00:00", "Z"),
        })
        self.record = json.loads(kubectl("replace", "-f", "-", "-o", "json",
                                         data=json.dumps(self.record).encode()))
        return {"installationUID": self.installation_uid,
                "leaseUID": self.record["metadata"]["uid"], "scope": "central",
                "holder": holder, "epoch": self.record["spec"]["leaseTransitions"]}

    def execute(self, pod, database, stem, token, operation="cluster/leave"):
        command = ["ovs-appctl", "--timeout=5", "-t", f"/var/run/ovn/{stem}_db.ctl",
                   operation, database]
        if operation == "cluster/kick":
            # A nonexistent member is a harmless way to wait until the current
            # token reaches admission without removing any real member.
            command.append("00000000-0000-0000-0000-000000000000")
        return self.request(pod, "/v1/fence/execute",
                            json.dumps({"command": command, "token": token}).encode(),
                            {"Content-Type": "application/json"})

    def admitted(self, pod, token):
        status, body = self.execute(pod, "OVN_Northbound", "ovnnb", token, "cluster/kick")
        response = json.loads(body)
        return status == 200 or (status == 500 and response.get("code") == "execution_failed"
                                 and "native OVN command failed" in response.get("message", ""))

    def wait_armed(self, token):
        for pod in self.endpoints:
            eventually(f"{pod} arms the current token", lambda: self.admitted(pod, token))
            check(self.request(pod, "/readyz")[0] == 200, "armed agent is not ready")

    def restore(self, pod, database, token, backup):
        headers = {"Content-Type": "application/octet-stream", "X-Kube-OVN-Database": database}
        for key, name in (("installationUID", "Installation-UID"), ("leaseUID", "Lease-UID"),
                          ("scope", "Fence-Scope"), ("holder", "Fence-Holder"), ("epoch", "Fence-Epoch")):
            headers[f"X-Kube-OVN-{name}"] = str(token[key])
        return self.request(pod, "/v1/fence/restore", backup, headers)

    def native(self, pod, *command):
        return kubectl("exec", pod, "-c", "ovn-central", "--", *command)

    def status(self, pod, database, stem):
        text = self.native(pod, "ovs-appctl", "--timeout=5", "-t",
                           f"/var/run/ovn/{stem}_db.ctl", "cluster/status", database).decode()
        fields = dict(re.findall(r"^([^:\n]+): (.*)$", text, re.MULTILINE))
        check(fields.get("Status") == "cluster member", "native DB is not a cluster member")
        members = sorted(re.findall(r"\([0-9a-f]{4} at ([^\s()]+)\)", text.split("Servers:\n", 1)[1]))
        return fields, members

    def topology(self):
        result = {}
        for database, stem, _ in DATABASES:
            leaders = 0
            member_lists = []
            for pod in self.endpoints:
                fields, members = self.status(pod, database, stem)
                check(len(members) == 3, "fenced request changed the three-member configuration")
                leaders += fields["Role"] == "leader"
                check(fields["Entries not yet committed"] == "0"
                      and fields["Entries not yet applied"] == "0", "RAFT entries have not converged")
                result[pod, database] = (fields["Cluster ID"], fields["Server ID"], members)
                member_lists.append(members)
            check(leaders == 1 and all(m == member_lists[0] for m in member_lists),
                  "members disagree on the live RAFT configuration")
        return result

    def transact(self, pod, database, stem, operation):
        result = json.loads(self.native(pod, "ovsdb-client", "--timeout=10", "transact",
                                       f"unix:/var/run/ovn/{stem}_db.sock",
                                       json.dumps([database, operation])))
        check(len(result) == 1 and "error" not in result[0], f"OVSDB transaction failed: {result}")
        return result[0]

    def marker(self, pod, database, stem, table):
        # Query reads locally applied state on every member, including followers.
        result = json.loads(self.native(pod, "ovsdb-client", "--no-leader-only", "--timeout=5", "query",
                                       f"unix:/var/run/ovn/{stem}_db.sock", json.dumps([
                                           database, {"op": "select", "table": table,
                                                      "where": [], "columns": ["external_ids"]}])))
        check(len(result) == 1 and "error" not in result[0], "native marker query failed")
        rows = result[0]["rows"]
        check(len(rows) == 1, f"expected one {table} row")
        return dict(rows[0]["external_ids"][1])

    def write_marker(self, pod, database, stem, table, value):
        values = self.marker(pod, database, stem, table)
        values[MARKER] = value
        result = self.transact(pod, database, stem, {"op": "update", "table": table,
                              "where": [], "row": {"external_ids": ["map", sorted(values.items())]}})
        check(result.get("count") == 1, "marker write did not update exactly one global row")
        self.wait_marker(database, stem, table, value)

    def wait_marker(self, database, stem, table, value):
        for pod in self.endpoints:
            eventually(f"{database} marker {value} applied on {pod}",
                       lambda: self.marker(pod, database, stem, table).get(MARKER) == value)


def rejected(reply, code):
    status, body = reply
    check(status == 409 and json.loads(body).get("code") == code,
          f"expected HTTP 409/{code}, got {status}: {body!r}")


def run(certificate_dir):
    fixture = Fixture(certificate_dir)
    old = fixture.hold("fence-e2e-original")
    fixture.wait_armed(old)
    before = fixture.topology()
    current = fixture.hold("fence-e2e-takeover")
    fixture.wait_armed(current)
    for pod in fixture.endpoints:
        for database, stem, _ in DATABASES:
            rejected(fixture.execute(pod, database, stem, old), "stale_fence")
        for field, value in (("installationUID", "wrong-installation"), ("leaseUID", "wrong-lease"),
                             ("scope", "installation"), ("holder", old["holder"]),
                             ("epoch", old["epoch"])):
            # Isolate each identity field; even a correct holder cannot reuse an
            # old epoch or a token from a different Lease/installation.
            rejected(fixture.execute(pod, "OVN_Northbound", "ovnnb", current | {field: value}),
                     "stale_fence")
    check(fixture.topology() == before, "stale leave changed native membership identity")
    print("Lease takeover: old holder and each mismatched token field rejected on all three agents", flush=True)

    backups = {}
    for database, stem, table in DATABASES:
        leaders = [pod for pod in fixture.endpoints
                   if fixture.status(pod, database, stem)[0]["Role"] == "leader"]
        check(len(leaders) == 1, "restore requires one native leader")
        leader = leaders[0]
        fixture.write_marker(leader, database, stem, table, "snapshot")
        backup = fixture.native(leader, "ovsdb-client", "--timeout=10", "backup",
                                f"unix:/var/run/ovn/{stem}_db.sock", database)
        check(backup, "native backup is empty")
        backups[database] = backup
        fixture.write_marker(leader, database, stem, table, "changed")
        for pod in fixture.endpoints:
            rejected(fixture.restore(pod, database, old, backup), "stale_fence")
        fixture.wait_marker(database, stem, table, "changed")
        status, body = fixture.restore(leader, database, current, backup)
        check(status == 200 and json.loads(body) == {}, f"native restore failed: {status}: {body!r}")
        fixture.wait_marker(database, stem, table, "snapshot")
        print(f"{database}: stale restores rejected on all agents; current-token native restore replicated to all three nodes", flush=True)
    check(fixture.topology() == before, "native restore changed cluster/server membership identities")

    expiring = fixture.hold("fence-e2e-expiring", duration=15)
    fixture.wait_armed(expiring)
    for pod in fixture.endpoints:
        eventually(f"{pod} fails readiness after Lease expiry",
                   lambda: fixture.request(pod, "/readyz")[0] == 503, timeout=30)
        check(fixture.request(pod, "/healthz")[0] == 200, "expired agent lost liveness")
        for database, stem, _ in DATABASES:
            rejected(fixture.execute(pod, database, stem, expiring), "fence_not_armed")
            rejected(fixture.restore(pod, database, expiring, backups[database]), "fence_not_armed")
    recovered = fixture.hold("fence-e2e-reacquired")
    fixture.wait_armed(recovered)
    for pod in fixture.endpoints:
        rejected(fixture.execute(pod, "OVN_Northbound", "ovnnb", expiring), "stale_fence")
    check(fixture.topology() == before, "expired requests changed native membership")
    for database, stem, table in DATABASES:
        fixture.wait_marker(database, stem, table, "snapshot")
    print("Lease expiry: readiness failed closed, membership and restore rejected; reacquisition armed all three agents", flush=True)


if __name__ == "__main__":
    check(len(sys.argv) == 2, "usage: fence-takeover-restore.py CERTIFICATE_DIRECTORY")
    run(Path(sys.argv[1]))
