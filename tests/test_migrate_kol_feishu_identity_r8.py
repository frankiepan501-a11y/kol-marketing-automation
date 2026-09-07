import importlib.util
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "migrate_kol_feishu_identity_r8.py"
SPEC = importlib.util.spec_from_file_location("migrate_kol_r8", SCRIPT)
module = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(module)


class R8N8nMigrationTests(unittest.TestCase):
    def test_phase2_replaces_literal_credentials(self):
        workflow = {
            "id": "hgM7unABBW7hr5dw",
            "nodes": [{
                "name": "Insert Crawl Task (Phase 2)",
                "parameters": {"jsCode": "const APP_ID=$env.FEISHU_KOL_ASSISTANT_APP_ID, APP_SECRET=$env.FEISHU_KOL_ASSISTANT_APP_SECRET;\nconst x=1;"},
            }],
        }
        self.assertEqual(["Insert Crawl Task (Phase 2)"], module.patch_phase2(workflow))
        code = workflow["nodes"][0]["parameters"]["jsCode"]
        self.assertIn("$env.FEISHU_KOL_ASSISTANT_APP_ID", code)
        self.assertIn("KOL_FEISHU_BASE_ENABLED", code)

    def test_event_hub_keeps_legacy_fallback_for_old_cards(self):
        line = (
            "const tr3 = await HR({ method: 'POST', url: 'x', "
            "body: { app_id: 'old3', app_secret: 'secret3' }, json: true });"
        )
        code = "const data = x;\nconst av = data.card_action || {};\n" + line
        updated = module._patch_handler(code, has_base_token=False, helper="HR")
        self.assertIn("av._delivery_identity === 'kol_assistant'", updated)
        self.assertIn("$env.FEISHU_KOL_ASSISTANT_APP_ID", updated)
        self.assertIn(": 'old3'", updated)

    def test_token_node_changes_only_named_kol_token(self):
        workflow = {
            "nodes": [
                {"name": "HTTP — token influencers", "parameters": {"jsonBody": "old1"}},
                {"name": "HTTP — token KOL", "parameters": {"jsonBody": "old2"}},
            ]
        }
        self.assertEqual(["HTTP — token KOL"], module._patch_token_node(workflow))
        self.assertEqual("old1", workflow["nodes"][0]["parameters"]["jsonBody"])
        self.assertIn("FEISHU_KOL_ASSISTANT_APP_ID", workflow["nodes"][1]["parameters"]["jsonBody"])
        self.assertIn("FEISHU_APP_ID", workflow["nodes"][1]["parameters"]["jsonBody"])

    def test_full_put_body_preserves_static_data(self):
        workflow = {
            "name": "wf",
            "nodes": [{"name": "schedule"}],
            "connections": {"schedule": {}},
            "settings": {"executionOrder": "v1"},
            "staticData": {"node:Cron": {"recurrencyRules": 1}},
        }
        body = module._workflow_update_body(workflow)
        self.assertEqual(workflow["staticData"], body["staticData"])
        self.assertIs(workflow["nodes"], body["nodes"])


if __name__ == "__main__":
    unittest.main()
