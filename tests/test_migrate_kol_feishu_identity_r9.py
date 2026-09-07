import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "migrate_kol_feishu_identity_r9.py"
SPEC = importlib.util.spec_from_file_location("migrate_kol_r9", SCRIPT)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class R9N8nMigrationTests(unittest.TestCase):
    def test_target_token_has_no_switch_or_legacy_identity(self):
        body = module._target_token_body()
        self.assertIn("FEISHU_KOL_ASSISTANT_APP_ID", body)
        self.assertNotIn("KOL_FEISHU_BASE_ENABLED", body)
        self.assertNotIn("LEGACY", body)

    def test_phase2_removes_r8_base_fallback(self):
        workflow = {"id": "hgM7unABBW7hr5dw", "nodes": [{
            "name": "Insert Crawl Task (Phase 2)",
            "parameters": {"jsCode": (
                "const USE_KOL_ASSISTANT=$env.KOL_FEISHU_BASE_ENABLED === '1';\n"
                "const APP_ID=USE_KOL_ASSISTANT ? $env.FEISHU_KOL_ASSISTANT_APP_ID : $env.FEISHU_KOL_LEGACY_BITABLE_APP_ID, "
                "APP_SECRET=USE_KOL_ASSISTANT ? $env.FEISHU_KOL_ASSISTANT_APP_SECRET : $env.FEISHU_KOL_LEGACY_BITABLE_APP_SECRET;\n"
                "const x=1;"
            )},
        }]}
        self.assertEqual(["Insert Crawl Task (Phase 2)"], module.patch_phase2(workflow))
        code = workflow["nodes"][0]["parameters"]["jsCode"]
        self.assertNotIn("KOL_FEISHU_BASE_ENABLED", code)
        self.assertNotIn("LEGACY", code)

    def test_daily_removes_card_switch_and_old_frankie_id(self):
        workflow = {"id": "KNx", "nodes": [
            {"name": "HTTP — token KOL", "parameters": {"jsonBody": (
                '={{ { "app_id": $env.KOL_FEISHU_BASE_ENABLED === "1" ? '
                '$env.FEISHU_KOL_ASSISTANT_APP_ID : $env.FEISHU_KOL_LEGACY_BITABLE_APP_ID } }}'
            )}},
            {"name": "HTTP — token influencers", "parameters": {"jsonBody": (
                '={"app_id":"cli_old_one","app_secret":"old"}'
            )}},
            {"name": "Code — 日报引擎", "parameters": {"jsCode": (
                "const KOL_CARDS_ENABLED = $env.KOL_FEISHU_CARDS_ENABLED === '1';\n"
                "const FRANKIE_LEGACY_OPEN_ID = 'ou_old';\n"
                "const FRANKIE_UNION_ID = $env.KOL_ASSISTANT_FRANKIE_UNION_ID;\n"
                "if (KOL_CARDS_ENABLED && !FRANKIE_UNION_ID) throw new Error('KOL assistant Frankie union_id missing');\n"
                "if (SEND_FRANKIE) targets.push(KOL_CARDS_ENABLED ? ['union_id', FRANKIE_UNION_ID] : ['open_id', FRANKIE_LEGACY_OPEN_ID]);\n"
                "const headers={Authorization: 'Bearer ' + (KOL_CARDS_ENABLED ? t2 : t1)};"
            )}},
        ]}
        changed = module.patch_daily_discovery(workflow)
        self.assertEqual(
            ["HTTP — token KOL", "HTTP — token influencers", "Code — 日报引擎"],
            changed,
        )
        code = workflow["nodes"][2]["parameters"]["jsCode"]
        self.assertNotIn("KOL_CARDS_ENABLED", code)
        self.assertNotIn("FRANKIE_LEGACY_OPEN_ID", code)
        self.assertIn("targets.push(['union_id', FRANKIE_UNION_ID])", code)

    def test_event_hub_keeps_explicit_history_compatibility(self):
        code = (
            "const useKolAssistant = av._delivery_identity === 'kol_assistant';\n"
            "const app=useKolAssistant ? $env.FEISHU_KOL_ASSISTANT_APP_ID : "
            "'cli_a9457898bd78dccc';\n"
            "const secret=useKolAssistant ? $env.FEISHU_KOL_ASSISTANT_APP_SECRET : "
            "'historical-secret';\n"
            "const url='&delivery_identity=' + encodeURIComponent("
            "useKolAssistant ? 'kol_assistant' : 'app3');"
        )
        workflow = {"nodes": [
            {"name": "Warm Recap Handler", "parameters": {"jsCode": code}},
            {"name": "Draft Action Handler", "parameters": {"jsCode": code}},
            {"name": "TP Action Handler", "parameters": {"jsCode": code}},
        ]}
        self.assertEqual([], module.validate_event_hub(workflow))

    def test_event_hub_rejects_missing_app3_history_branch(self):
        code = (
            "const useKolAssistant = av._delivery_identity === 'kol_assistant';\n"
            "const app=$env.FEISHU_KOL_ASSISTANT_APP_ID;"
        )
        workflow = {"nodes": [
            {"name": "Warm Recap Handler", "parameters": {"jsCode": code}},
            {"name": "Draft Action Handler", "parameters": {"jsCode": code}},
            {"name": "TP Action Handler", "parameters": {"jsCode": code}},
        ]}
        with self.assertRaisesRegex(RuntimeError, "historical App 3"):
            module.validate_event_hub(workflow)

    def test_event_hub_rejects_missing_secret_branch(self):
        code = (
            "const useKolAssistant = av._delivery_identity === 'kol_assistant';\n"
            "const app=useKolAssistant ? $env.FEISHU_KOL_ASSISTANT_APP_ID : "
            "'cli_a9457898bd78dccc';"
        )
        workflow = {"nodes": [
            {"name": "Warm Recap Handler", "parameters": {"jsCode": code}},
            {"name": "Draft Action Handler", "parameters": {"jsCode": code}},
            {"name": "TP Action Handler", "parameters": {"jsCode": code}},
        ]}
        with self.assertRaisesRegex(RuntimeError, "App secret"):
            module.validate_event_hub(workflow)

    def test_event_hub_rejects_missing_delivery_identity_forwarding(self):
        common = (
            "const useKolAssistant = av._delivery_identity === 'kol_assistant';\n"
            "const app=useKolAssistant ? $env.FEISHU_KOL_ASSISTANT_APP_ID : "
            "'cli_a9457898bd78dccc';\n"
            "const secret=useKolAssistant ? $env.FEISHU_KOL_ASSISTANT_APP_SECRET : "
            "'historical-secret';"
        )
        workflow = {"nodes": [
            {"name": "Warm Recap Handler", "parameters": {"jsCode": common}},
            {"name": "Draft Action Handler", "parameters": {"jsCode": common}},
            {"name": "TP Action Handler", "parameters": {"jsCode": common}},
        ]}
        with self.assertRaisesRegex(RuntimeError, "not forwarded"):
            module.validate_event_hub(workflow)


if __name__ == "__main__":
    unittest.main()
