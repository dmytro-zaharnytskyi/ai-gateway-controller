import os
import runpy
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


DRIVER = Path(__file__).with_name("e2e-ipp-translation-migration.py")


class ConfigMapRestorePatchTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.evidence_dir = tempfile.TemporaryDirectory(prefix="ipp-migration-patch-test-")
        cls.previous_evidence = os.environ.get("LOCAL_ENV_MIGRATION_EVIDENCE")
        os.environ["LOCAL_ENV_MIGRATION_EVIDENCE"] = cls.evidence_dir.name
        cls.driver = runpy.run_path(str(DRIVER), run_name="migration_driver_test")

    @classmethod
    def tearDownClass(cls):
        if cls.previous_evidence is None:
            os.environ.pop("LOCAL_ENV_MIGRATION_EVIDENCE", None)
        else:
            os.environ["LOCAL_ENV_MIGRATION_EVIDENCE"] = cls.previous_evidence
        cls.evidence_dir.cleanup()

    def test_restores_annotations_when_controller_recreated_configmap(self):
        current = {"data": {"extproc.yaml": "baseline"}, "metadata": {"labels": {"owner": "grid"}}}
        original = {
            "data": {"extproc.yaml": "baseline"},
            "metadata": {
                "labels": {"owner": "grid"},
                "annotations": {"opendatahub.io/managed": "false"},
            },
        }

        patch = self.driver["configmap_restore_patch"](current, original)

        self.assertEqual(patch, [{
            "op": "add",
            "path": "/metadata/annotations",
            "value": {"opendatahub.io/managed": "false"},
        }])

    def test_replaces_data_and_removes_run_added_annotations(self):
        current = {
            "data": {"extproc.yaml": "temporary", "run-only.yaml": "temporary"},
            "metadata": {
                "labels": {"owner": "grid", "run-id": "current"},
                "annotations": {"run.example/id": "current"},
            },
        }
        original = {
            "data": {"extproc.yaml": "baseline"},
            "metadata": {"labels": {"owner": "grid"}, "annotations": {}},
        }

        patch = self.driver["configmap_restore_patch"](current, original)

        self.assertEqual(patch, [
            {"op": "add", "path": "/data", "value": {"extproc.yaml": "baseline"}},
            {"op": "add", "path": "/metadata/labels", "value": {"owner": "grid"}},
            {"op": "remove", "path": "/metadata/annotations"},
        ])

    def test_returns_empty_patch_when_snapshot_matches(self):
        snapshot = {
            "data": {"extproc.yaml": "baseline"},
            "metadata": {"labels": {"owner": "grid"}, "annotations": {}},
        }

        self.assertEqual(self.driver["configmap_restore_patch"](snapshot, snapshot), [])

    def test_optional_configmap_read_distinguishes_absence_from_failure(self):
        get_configmap = self.driver["get_configmap"]
        calls = []

        def fake_kubectl(result):
            def read(*args, **kwargs):
                calls.append((args, kwargs))
                return result
            return read

        absent = subprocess.CompletedProcess([], 0, stdout="", stderr="")
        denied = subprocess.CompletedProcess([], 1, stdout="", stderr="Forbidden")
        present = subprocess.CompletedProcess([], 0, stdout='{"data":{"extproc.yaml":"ok"}}', stderr="")
        with patch.dict(get_configmap.__globals__, {"kubectl": fake_kubectl(absent)}):
            self.assertIsNone(get_configmap(check=False))
        with patch.dict(get_configmap.__globals__, {"kubectl": fake_kubectl(denied)}):
            with self.assertRaisesRegex(RuntimeError, "Forbidden"):
                get_configmap(check=False)
        with patch.dict(get_configmap.__globals__, {"kubectl": fake_kubectl(present)}):
            self.assertEqual(get_configmap(check=False)["data"], {"extproc.yaml": "ok"})
        self.assertTrue(all("--ignore-not-found" in args and kwargs["check"] is False
                            for args, kwargs in calls))


if __name__ == "__main__":
    unittest.main()
