import unittest

from scripts.patch_n8n_eventhub_draft_regen import patch_workflow, verify_workflow


OLD_JS = r"""
async function markCard(label, msg) {
  return true;
}

let reply = '';
if (action === 'draft_regen') {
  const fb = encodeURIComponent(String(fv.regen_feedback || '').trim());
  let rr = {}; let err = null;
  try {
    rr = await HR({ method: 'POST', url: 'https://kol-auto.zeabur.app/draft/regen?record_id=' + rid + '&feedback=' + fb + '&async_mode=true', headers: { 'Authorization': 'Bearer hidden' }, json: true, timeout: 15000 });
  } catch (e) { err = 'safe error'; }
  if (err) { reply = '⚠️ 重生调用异常: ' + err + ' (by ' + approver + ')'; }
  else if (rr && rr.ok && (rr.accepted || rr.job_id)) { const note = rr.already_running ? '已有重生任务在处理中' : '已提交后台重生任务'; reply = '🔁 ' + note + (decodeURIComponent(fb) ? ' [已按你填的方向]' : '') + ', 稍后新审核卡会发来 (by ' + approver + ')'; await markCard('退回重生', reply); }
else if (rr && rr.ok && rr.new_rid) { reply = '🔁 已真重新生成新版草稿(第' + (rr.retries || 1) + '次)' + (decodeURIComponent(fb) ? ' [已按你填的方向]' : '') + ', 稍后新审核卡会发来 (by ' + approver + ')'; await markCard('退回重生', reply); }
  else if (rr && rr.skip) { reply = 'ℹ️ ' + rr.skip + ' (by ' + approver + ')'; await markCard('退回重生', reply); }
  else { reply = '⚠️ 重生未完成'; }
}
"""


class EventHubDraftRegenPatchTests(unittest.TestCase):
    def workflow(self):
        return {
            "name": "Event Hub",
            "active": True,
            "nodes": [
                {"name": "Before", "parameters": {}},
                {"name": "Draft Action Handler", "parameters": {"jsCode": OLD_JS}},
            ],
            "connections": {"Before": {"main": [[{"node": "Draft Action Handler", "type": "main", "index": 0}]]}},
            "settings": {},
        }

    def test_patch_keeps_workflow_shape_and_changes_callback_contract(self):
        before = self.workflow()
        after, changed = patch_workflow(before)
        verify_workflow(before, after)
        source = after["nodes"][1]["parameters"]["jsCode"]

        self.assertTrue(changed)
        self.assertEqual(before["connections"], after["connections"])
        self.assertIn("rr.suppress_reply", source)
        self.assertIn("return [];", source)
        self.assertIn("[重生处理中]", source)
        self.assertIn("[重生失败]", source)
        self.assertIn("&message_id=", source)
        self.assertIn("&operator_open_id=", source)
        self.assertNotIn("await markCard('退回重生', reply)", source)

    def test_patch_is_idempotent(self):
        before = self.workflow()
        once, first_changed = patch_workflow(before)
        twice, second_changed = patch_workflow(once)

        self.assertTrue(first_changed)
        self.assertFalse(second_changed)
        self.assertEqual(once, twice)

    def test_verify_rejects_non_target_node_drift(self):
        before = self.workflow()
        after, _ = patch_workflow(before)
        after["nodes"][0]["parameters"]["unexpected"] = True

        with self.assertRaisesRegex(ValueError, "non-target node changed"):
            verify_workflow(before, after)


if __name__ == "__main__":
    unittest.main()
