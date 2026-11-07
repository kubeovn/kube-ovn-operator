"""Check API watch history and cleanup without starting Kubernetes."""

import contextlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location("fence_acceptance", Path(__file__).with_name("fence-takeover-restore.py"))
acceptance = importlib.util.module_from_spec(spec)
spec.loader.exec_module(acceptance)


class RecoveryWatchTest(unittest.TestCase):
    request = {"metadata": {"name": "restore", "uid": "request-uid", "resourceVersion": "123"}}

    def watch(self, events, *, wait=False, timeout=2):
        payload = b"".join(json.dumps(event).encode() + b"\n" for event in events)
        launch = subprocess.Popen
        children = []

        def fake_kubectl(command, **kwargs):
            self.assertIn("resourceVersion=123", command[-1])
            self.assertIn("metadata.name%3Drestore", command[-1])
            script = f"import os,time; os.write(1, {payload!r}); time.sleep({10 if wait else 0})"
            child = launch([sys.executable, "-c", script], **kwargs)
            children.append(child)
            return child

        with patch.object(acceptance.subprocess, "Popen", side_effect=fake_kubectl):
            try:
                with contextlib.closing(acceptance.recovery_statuses(self.request, timeout)) as statuses:
                    for status in statuses:
                        yield status
            finally:
                self.assertTrue(children)
                self.assertIsNotNone(children[0].poll(), "watch child was not reaped")

    def event(self, phase, uid="request-uid"):
        return {"type": "MODIFIED", "object": {"metadata": {"uid": uid},
                "status": {"execution": {"phase": phase}}}}

    def test_fast_phases_survive_one_stream_chunk_and_early_close(self):
        phases = ["Quiescing", "Restoring", "Workloads", "Verifying", "Succeeded"]
        with contextlib.closing(self.watch([self.event(phase) for phase in phases], wait=True)) as statuses:
            seen = []
            for status in statuses:
                seen.append(status["execution"]["phase"])
                if seen[-1] == "Succeeded":
                    break
        self.assertEqual(seen, phases)

    def test_api_error_and_replaced_request_are_rejected(self):
        for event, reason in [({"type": "ERROR", "object": {"code": 410}}, "API error"),
                              (self.event("Succeeded", uid="replacement"), "replaced")]:
            with self.subTest(reason=reason), self.assertRaisesRegex(AssertionError, reason):
                list(self.watch([event]))

    def test_eof_and_timeout_fail_and_reap_watch(self):
        with self.assertRaisesRegex(AssertionError, "ended before completion"):
            list(self.watch([]))
        with self.assertRaisesRegex(AssertionError, "timed out"):
            list(self.watch([], wait=True, timeout=0.1))


if __name__ == "__main__":
    unittest.main()
