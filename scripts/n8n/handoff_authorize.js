// Deployment substitutes __HANDOFF_INTERNAL_TOKEN__ in memory; never commit credentials.
const rows = $input.all();
const actions = new Set(['draft_approve', 'draft_reject', 'draft_regen', 'draft_tracking', 'draft_uploadreg', 'warm_recap_send']);
for (const row of rows) {
  if (!actions.has(row.json.card_action?.action)) continue;
  const result = await this.helpers.httpRequest({
    method: 'POST', url: 'https://kol-auto.zeabur.app/handoff/authorize-kol',
    headers: { Authorization: 'Bearer __HANDOFF_INTERNAL_TOKEN__' },
    body: row.json, json: true, timeout: 25000,
  });
  if (result?.allowed !== true) {
    throw new Error('交接权限校验未通过，未执行业务操作：' + (result?.reason || 'unknown'));
  }
}
return rows;
