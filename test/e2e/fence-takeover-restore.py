#!/usr/bin/env python3
"""Exercise real agents only in the disposable controller-handoff Kind fixture."""

import contextlib
import datetime
import http.client
import json
import os
import re
import select
import ssl
import subprocess
import sys
import time
import tempfile
import urllib.error
import urllib.parse
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


def api_command(*arguments):
    command = ["kubectl", "--context", CONTEXT]
    if server := os.environ.get("KUBE_OVN_ACCEPTANCE_API_SERVER"):
        command.append("--server=" + server)
    return [*command, *arguments]


def kubectl(*arguments, data=None):
    result = subprocess.run(
        api_command("--namespace", NAMESPACE, "--request-timeout=15s", *arguments),
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



def patch_installation(patch):
    return json.loads(kubectl("patch", "koi", INSTALLATION, "--type=merge",
                              "-p", json.dumps({"spec": patch}), "-o", "json"))


def controllers(fixture):
    objects = json.loads(kubectl("get", "deployments,statefulsets", "-l",
                                f"operator.kubeovn.io/installation-uid={fixture.installation_uid}",
                                "-o", "json"))["items"]
    check(len(objects) == 3, "expected one Deployment and two replacement StatefulSets")
    return objects


def controller_state(objects):
    return {item["metadata"]["uid"]: (item["spec"].get("paused", False),
            item["spec"].get("updateStrategy")) for item in objects}


def create_recovery(name, backup_ref):
    # Wait for the CRD watch cache before creating a request. A create can
    # precede lazy cache initialization, making its resourceVersion too old
    # for a subsequent watch even when only a few seconds have elapsed.
    path = f"/apis/operator.kubeovn.io/v1alpha1/namespaces/{NAMESPACE}/kubeovnrecoveryrequests"
    cached = json.loads(kubectl("get", "--raw", path + "?resourceVersion=0"))
    check(cached.get("metadata", {}).get("resourceVersion"), "recovery cache list lacks a resourceVersion")
    print(f"Recovery watch cache ready at {cached['metadata']['resourceVersion']} before creating {name}", flush=True)
    request = {"apiVersion": "operator.kubeovn.io/v1alpha1", "kind": "KubeOVNRecoveryRequest",
               "metadata": {"name": name, "namespace": NAMESPACE},
               "spec": {"installationRef": INSTALLATION, "reason": "Disposable Kind native restore acceptance",
                        "mode": "reconvert", "backupRef": backup_ref, "approvedBy": "kind-e2e-audit",
                        "expiresAt": (datetime.datetime.now(datetime.timezone.utc)
                                      + datetime.timedelta(minutes=15)).isoformat().replace("+00:00", "Z")}}
    return json.loads(kubectl("create", "-f", "-", "-o", "json", data=json.dumps(request).encode()))


def recovery_statuses(request, timeout=240):
    # Start from the create response's resourceVersion: even phases that finish
    # before this watch connects remain observable from the API event history.
    query = urllib.parse.urlencode({"watch": "true", "resourceVersion": request["metadata"]["resourceVersion"],
                                    "fieldSelector": "metadata.name=" + request["metadata"]["name"],
                                    "timeoutSeconds": timeout})
    path = f"/apis/operator.kubeovn.io/v1alpha1/namespaces/{NAMESPACE}/kubeovnrecoveryrequests?{query}"
    with tempfile.TemporaryFile() as errors:
        process = subprocess.Popen(api_command(f"--request-timeout={timeout + 5}s",
                                               "get", "--raw", path), stdout=subprocess.PIPE, stderr=errors)
        try:
            pending = b""
            last_status = {}
            deadline = time.monotonic() + timeout
            while True:
                remaining = deadline - time.monotonic()
                check(remaining > 0, f"controller recovery watch timed out: {last_status}")
                ready, _, _ = select.select([process.stdout], [], [], remaining)
                check(ready, f"controller recovery watch timed out: {last_status}")
                chunk = os.read(process.stdout.fileno(), 65536)
                if not chunk:
                    errors.seek(0)
                    raise AssertionError(f"controller recovery watch ended before completion: {errors.read().decode()}")
                pending += chunk
                while b"\n" in pending:
                    line, pending = pending.split(b"\n", 1)
                    if not line.strip():
                        continue
                    event = json.loads(line)
                    check(event.get("type") != "ERROR", f"recovery watch API error: {event}")
                    current = event["object"]
                    check(current["metadata"]["uid"] == request["metadata"]["uid"], "recovery request was replaced")
                    last_status = current.get("status", {})
                    yield last_status
        finally:
            if process.poll() is None:
                process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=5)
            process.stdout.close()


def native_membership(topology):
    # Rollout may replace a Pod while the native database, Node, workload and
    # storage identities remain pinned by the controller's topology verifier.
    return {(database, value[1]): value for (_, database), value in topology.items()}


def run_controller_recovery(certificate_dir, backup_dir):
    fixture = Fixture(certificate_dir)
    before = native_membership(fixture.topology())
    original_state = controller_state(controllers(fixture))
    for database, stem, table in DATABASES:
        leader = next(p for p in fixture.endpoints if fixture.status(p, database, stem)[0]["Role"] == "leader")
        fixture.write_marker(leader, database, stem, table, "controller-snapshot")
    previous = resource("koi", INSTALLATION).get("status", {}).get("lastBackup", {}).get("id")
    patch_installation({"upgrade": {"backup": {"required": True, "destination": backup_dir.resolve().as_uri(), "schedule": "1m"}}})

    def captured():
        backup = resource("koi", INSTALLATION).get("status", {}).get("lastBackup", {})
        return backup.get("id") and backup["id"] != previous

    eventually("controller captures a fresh scheduled native backup pair", captured, timeout=180)
    backup = resource("koi", INSTALLATION)["status"]["lastBackup"]
    check(set(backup["databaseRefs"]) == {d[0] for d in DATABASES}, "scheduled backup lacks NB/SB references")
    installation = patch_installation({"upgrade": {"backup": {"schedule": ""}}})
    # The completed member journal is still reconciled. It must not replace a
    # newer scheduled pair with the older pre-replacement backup summary.
    time.sleep(5)
    check(resource("koi", INSTALLATION)["status"]["lastBackup"]["id"] == backup["id"],
          "completed member lifecycle overwrote the newer scheduled backup")
    print("Controller scheduled backup: fresh native NB/SB pair remains pinned after scheduling is disabled", flush=True)
    for database, stem, table in DATABASES:
        leader = next(p for p in fixture.endpoints if fixture.status(p, database, stem)[0]["Role"] == "leader")
        fixture.write_marker(leader, database, stem, table, "controller-changed")

    create_recovery("controller-invalid-backup", "sha256:" + "f" * 64)
    eventually("invalid backup request fails admission",
               lambda: resource("kubeovnrecoveryrequest", "controller-invalid-backup").get("status", {}).get("phase") == "Failed")
    invalid = resource("kubeovnrecoveryrequest", "controller-invalid-backup")["status"]
    check(any(c.get("reason") == "BackupMismatch" for c in invalid["conditions"]),
          "invalid backup was rejected for an unrelated reason")
    check(not invalid.get("execution"), "invalid backup reached execution")
    check(controller_state(controllers(fixture)) == original_state, "rejected request changed rollout state")
    for database, stem, table in DATABASES:
        fixture.wait_marker(database, stem, table, "controller-changed")

    for attempt in range(2):
        if attempt:
            for database, stem, table in DATABASES:
                leader = next(p for p in fixture.endpoints if fixture.status(p, database, stem)[0]["Role"] == "leader")
                fixture.write_marker(leader, database, stem, table, "controller-changed")
        name = "controller-native-restore" + ("-repeat" if attempt else "")
        request = create_recovery(name, backup["id"])
        seen = set()
        plan = None
        with contextlib.closing(recovery_statuses(request)) as statuses:
            for status in statuses:
                check(status.get("phase") not in ("Failed", "Expired"), f"controller recovery failed: {status}")
                if status.get("plan"):
                    if plan is None:
                        plan = status["plan"]
                        check(plan["installationUID"] == fixture.installation_uid
                              and plan["installationGeneration"] == installation["metadata"]["generation"]
                              and plan["backupRef"] == backup["id"]
                              and plan["databaseRefs"] == backup["databaseRefs"]
                              and plan["topologyDigest"] == backup["topologyDigest"], "recovery plan lost its admitted bindings")
                    check(status["plan"] == plan, "immutable recovery plan changed during execution")
                phase = status.get("execution", {}).get("phase")
                if phase and phase not in seen:
                    seen.add(phase)
                    print(f"Controller recovery phase: {phase}", flush=True)
                if phase == "Restoring":
                    for item in controllers(fixture):
                        annotations = item["metadata"].get("annotations", {})
                        check(annotations.get("operator.kubeovn.io/rollback-quiesced") == plan["digest"],
                              "restore began without the operation-bound rollout quiesce marker")
                        check("operator.kubeovn.io/rollback-state" in annotations, "original rollout state was not persisted")
                        if item["kind"] == "Deployment":
                            check(item["spec"].get("paused"), "restore began before Deployment pause")
                        else:
                            check(item["spec"]["updateStrategy"]["type"] == "OnDelete", "restore began before StatefulSet freeze")
                if status.get("phase") == "Succeeded":
                    break
        check({"Restoring", "Workloads", "Verifying", "Succeeded"} <= seen,
              f"recovery skipped durable execution phases: {seen}")
        check(status.get("completedAt") and status["execution"].get("startedAt")
              and status["execution"].get("completedAt"), "recovery completion lacks audit timestamps")
        fixture = Fixture(certificate_dir)
        check(controller_state(controllers(fixture)) == original_state, "recovery did not restore original rollout state")
        for item in controllers(fixture):
            check(not any(key.startswith("operator.kubeovn.io/rollback-") for key in item["metadata"].get("annotations", {})),
                  "completed recovery left durable quiesce markers")
        for database, stem, table in DATABASES:
            fixture.wait_marker(database, stem, table, "controller-snapshot")
        check(native_membership(fixture.topology()) == before, "controller recovery changed native cluster/server identities")
        def installation_ready():
            current = resource("koi", INSTALLATION)
            conditions = {condition["type"]: condition for condition in current.get("status", {}).get("conditions", [])}
            return all(conditions.get(kind, {}).get("status") == expected
                       and conditions[kind].get("observedGeneration") == current["metadata"]["generation"]
                       for kind, expected in (("Ready", "True"), ("Degraded", "False")))

        eventually("routine installation reconciliation returns to Ready after recovery", installation_ready)
        print("Controller RecoveryRequest succeeded: rejected foreign backup; restored NB/SB snapshot on all three nodes; rollout state and membership preserved; installation Ready", flush=True)


if __name__ == "__main__":
    check(len(sys.argv) in (2, 4), "usage: fence-takeover-restore.py CERTIFICATE_DIRECTORY [controller-recovery BACKUP_DIRECTORY]")
    if len(sys.argv) == 4:
        check(sys.argv[2] == "controller-recovery", "unknown acceptance mode")
        run_controller_recovery(Path(sys.argv[1]), Path(sys.argv[3]))
    else:
        run(Path(sys.argv[1]))
