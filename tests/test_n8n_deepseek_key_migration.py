import unittest

from scripts.migrate_n8n_deepseek_keys import (
    N8N_PRODUCTION_SERVICE_ID,
    SECRET_RE,
    migrate_workflow,
    resolve_service_by_domain,
    verify_required_variables,
    verify_workflow,
)


class N8nDeepSeekKeyMigrationTests(unittest.TestCase):
    secret = "sk" + "-example0123456789"

    def test_http_header_becomes_channel_env_expression(self):
        workflow = {"nodes": [{"name": "AI", "parameters": {
            "url": "https://api.deepseek.com/chat/completions",
            "headerParameters": {"parameters": [
                {"name": "Authorization", "value": "Bearer " + self.secret}
            ]}
        }}]}
        migrated, nodes = migrate_workflow(workflow, "SEO_DEEPSEEK_API_KEY")
        value = migrated["nodes"][0]["parameters"]["headerParameters"]["parameters"][0]["value"]
        self.assertIn("$env.SEO_DEEPSEEK_API_KEY", value)
        self.assertIn("[DEEPSEEK_KEY_MISSING]", value)
        self.assertEqual(["AI"], nodes)
        self.assertIsNone(SECRET_RE.search(str(migrated)))

    def test_code_node_gets_missing_key_guard_and_no_literal(self):
        workflow = {"nodes": [{"name": "Code", "parameters": {
            "jsCode": f"const DS_KEY='{self.secret}';\nconst url='https://api.deepseek.com/chat/completions';\nconst h={{'Authorization':'Bearer {self.secret}'}};"
        }}]}
        migrated, nodes = migrate_workflow(workflow, "KOL_DEEPSEEK_API_KEY")
        source = migrated["nodes"][0]["parameters"]["jsCode"]
        self.assertIn("$env.KOL_DEEPSEEK_API_KEY", source)
        self.assertIn("[DEEPSEEK_KEY_MISSING]", source)
        self.assertNotIn(self.secret, source)
        self.assertEqual(["Code"], nodes)

    def test_non_deepseek_sk_token_is_not_rewritten(self):
        workflow = {"nodes": [{"name": "Other API", "parameters": {
            "jsCode": f"const OTHER_API_KEY='{self.secret}';"
        }}]}
        migrated, nodes = migrate_workflow(workflow, "SEO_DEEPSEEK_API_KEY")
        self.assertEqual([], nodes)
        self.assertIn(self.secret, migrated["nodes"][0]["parameters"]["jsCode"])

    def test_existing_unguarded_env_header_is_upgraded(self):
        workflow = {"nodes": [{"name": "AI", "type": "n8n-nodes-base.httpRequest", "parameters": {
            "url": "https://api.deepseek.com/chat/completions",
            "headerParameters": {"parameters": [{
                "name": "Authorization",
                "value": "={{ 'Bearer ' + $env.SEO_DEEPSEEK_API_KEY }}",
            }]},
        }}]}
        migrated, nodes = migrate_workflow(workflow, "SEO_DEEPSEEK_API_KEY")
        value = migrated["nodes"][0]["parameters"]["headerParameters"]["parameters"][0]["value"]
        self.assertEqual(["AI"], nodes)
        self.assertIn("[DEEPSEEK_KEY_MISSING]", value)

    def test_readback_requires_active_published_guarded_workflow(self):
        workflow = {
            "active": True,
            "versionId": "v2",
            "activeVersionId": "v2",
            "nodes": [{"name": "AI", "type": "n8n-nodes-base.httpRequest", "parameters": {
                "url": "https://api.deepseek.com/chat/completions",
                "headerParameters": {"parameters": [{
                    "name": "Authorization",
                    "value": "={{ (() => { const k = $env.SEO_DEEPSEEK_API_KEY; if (!k) throw new Error('[DEEPSEEK_KEY_MISSING] SEO_DEEPSEEK_API_KEY not configured'); return 'Bearer ' + k; })() }}",
                }]},
            }}],
        }
        self.assertEqual(1, verify_workflow(workflow, "SEO_DEEPSEEK_API_KEY", ["AI"]))
        workflow["activeVersionId"] = "v1"
        with self.assertRaisesRegex(ValueError, "activeVersionId"):
            verify_workflow(workflow, "SEO_DEEPSEEK_API_KEY", ["AI"])

    def test_readback_scans_unmapped_deepseek_nodes_for_literals(self):
        workflow = {
            "active": True,
            "versionId": "v2",
            "activeVersionId": "v2",
            "nodes": [
                {"name": "Safe", "type": "n8n-nodes-base.code", "parameters": {
                    "jsCode": "const __DEEPSEEK_API_KEY__=$env.SEO_DEEPSEEK_API_KEY; if (!__DEEPSEEK_API_KEY__) throw new Error('[DEEPSEEK_KEY_MISSING]');"
                }},
                {"name": "Forgotten", "type": "n8n-nodes-base.httpRequest", "parameters": {
                    "url": "https://api.deepseek.com/chat/completions",
                    "header": "Bearer " + self.secret,
                }},
            ],
        }
        with self.assertRaisesRegex(ValueError, "anywhere in workflow"):
            verify_workflow(workflow, "SEO_DEEPSEEK_API_KEY")

    def test_readback_rejects_unguarded_code_env_reference(self):
        workflow = {
            "active": True,
            "versionId": "v2",
            "activeVersionId": "v2",
            "nodes": [{"name": "Code", "type": "n8n-nodes-base.code", "parameters": {
                "jsCode": "const k=$env.SEO_DEEPSEEK_API_KEY; const url='https://api.deepseek.com/chat/completions';"
            }}],
        }
        with self.assertRaisesRegex(ValueError, "Code node missing"):
            verify_workflow(workflow, "SEO_DEEPSEEK_API_KEY")

    def test_readback_rejects_wrong_channel_variable(self):
        workflow = {
            "active": True,
            "versionId": "v2",
            "activeVersionId": "v2",
            "nodes": [
                {"name": "Safe", "type": "n8n-nodes-base.code", "parameters": {
                    "jsCode": "const k=$env.SEO_DEEPSEEK_API_KEY; if (!k) throw new Error('[DEEPSEEK_KEY_MISSING]');"
                }},
                {"name": "Crossed", "type": "n8n-nodes-base.code", "parameters": {
                    "jsCode": "const k=$env.KOL_DEEPSEEK_API_KEY; if (!k) throw new Error('[DEEPSEEK_KEY_MISSING]'); const url='https://api.deepseek.com/chat/completions';"
                }},
            ],
        }
        with self.assertRaisesRegex(ValueError, "wrong DeepSeek channel variable"):
            verify_workflow(workflow, "SEO_DEEPSEEK_API_KEY")

    def test_readback_scans_entire_named_deepseek_node(self):
        workflow = {
            "active": True,
            "versionId": "v2",
            "activeVersionId": "v2",
            "nodes": [
                {"name": "Safe", "type": "n8n-nodes-base.code", "parameters": {
                    "jsCode": "const k=$env.SEO_DEEPSEEK_API_KEY; if (!k) throw new Error('[DEEPSEEK_KEY_MISSING]');"
                }},
                {"name": "DeepSeek via upstream URL", "type": "n8n-nodes-base.httpRequest", "parameters": {
                    "url": "={{ $json.url }}",
                    "header": "Bearer " + self.secret,
                }},
            ],
        }
        with self.assertRaisesRegex(ValueError, "anywhere in workflow"):
            verify_workflow(workflow, "SEO_DEEPSEEK_API_KEY")

    def test_readback_requires_nonempty_version_ids(self):
        workflow = {
            "active": True,
            "nodes": [{"name": "Code", "type": "n8n-nodes-base.code", "parameters": {
                "jsCode": "const k=$env.SEO_DEEPSEEK_API_KEY; if (!k) throw new Error('[DEEPSEEK_KEY_MISSING]');"
            }}],
        }
        with self.assertRaisesRegex(ValueError, "versionId is missing"):
            verify_workflow(workflow, "SEO_DEEPSEEK_API_KEY")

    def test_production_service_is_resolved_by_domain_not_stale_name(self):
        services = [
            {"_id": "69856f0d2e156a6efa59a9aa", "name": "n8n", "status": "SUSPENDED", "domains": []},
            {
                "_id": N8N_PRODUCTION_SERVICE_ID,
                "name": "n8n-hual",
                "status": "RUNNING",
                "domains": [{"domain": "frankiepan501.zeabur.app"}],
            },
        ]
        resolved = resolve_service_by_domain(services, "frankiepan501.zeabur.app")
        self.assertEqual(N8N_PRODUCTION_SERVICE_ID, resolved["_id"])
        self.assertEqual("n8n-hual", resolved["name"])

    def test_stale_service_id_is_rejected_even_if_given_production_domain(self):
        services = [{
            "_id": "69856f0d2e156a6efa59a9aa",
            "name": "n8n",
            "status": "RUNNING",
            "domains": [{"domain": "frankiepan501.zeabur.app"}],
        }]
        with self.assertRaisesRegex(ValueError, "unexpected service ID"):
            resolve_service_by_domain(services, "frankiepan501.zeabur.app")

    def test_commit_preflight_rejects_missing_channel_variable(self):
        rows = [{"key": "KOL_DEEPSEEK_API_KEY", "value": "configured"}]
        with self.assertRaisesRegex(ValueError, "SEO_DEEPSEEK_API_KEY"):
            verify_required_variables(rows, {"SEO_DEEPSEEK_API_KEY"})


if __name__ == "__main__":
    unittest.main()
