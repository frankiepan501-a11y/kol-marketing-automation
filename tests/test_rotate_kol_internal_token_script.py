import json
import os
from pathlib import Path
import shutil
import subprocess
import unittest


class RotateKolInternalTokenScriptTests(unittest.TestCase):
    def test_self_test_covers_http_and_code_node_references(self):
        repo = Path(__file__).resolve().parents[1]
        script = repo / "scripts" / "rotate_kol_internal_token_20260824.ps1"
        shell = shutil.which("pwsh.exe") or shutil.which("powershell.exe")
        self.assertIsNotNone(shell, "PowerShell executable is required")

        env = os.environ.copy()
        for name in ("N8N_BASE_URL", "N8N_API_KEY", "ZEABUR_API_KEY"):
            env.pop(name, None)

        result = subprocess.run(
            [
                shell,
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-File",
                str(script),
                "-SelfTest",
            ],
            cwd=repo,
            env=env,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=30,
        )

        self.assertEqual(0, result.returncode, result.stderr or result.stdout)
        lines = [line for line in result.stdout.splitlines() if line.strip()]
        data = json.loads(lines[-1])
        self.assertEqual("self-test", data["mode"])
        self.assertEqual(1, data["http_reference_count"])
        self.assertEqual(1, data["code_reference_count"])
        self.assertEqual(2, data["total_reference_count"])
        self.assertTrue(data["code_token_updated"])
        self.assertTrue(data["unrelated_code_token_preserved"])
        self.assertTrue(data["cross_object_token_preserved"])
        self.assertEqual(0, data["block_cross_object_reference_count"])
        self.assertTrue(data["block_cross_object_token_preserved"])
        self.assertEqual(0, data["nested_sibling_reference_count"])
        self.assertTrue(data["nested_sibling_token_preserved"])
        self.assertEqual(0, data["function_sibling_reference_count"])
        self.assertTrue(data["function_sibling_token_preserved"])
        self.assertEqual(0, data["comment_reference_count"])
        self.assertTrue(data["comment_token_preserved"])
        self.assertTrue(data["header_before_url_supported"])
        self.assertTrue(data["code_only_workflow_supported"])
        self.assertTrue(data["fixture_readback_verified"])
        self.assertTrue(data["code_only_readback_verified"])
        self.assertTrue(data["structure_preserved"])
        self.assertTrue(data["canonical_hash_order_independent"])
        self.assertEqual(0, data["secrets_printed"])


if __name__ == "__main__":
    unittest.main()
