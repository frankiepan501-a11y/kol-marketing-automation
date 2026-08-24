const balance = $input.first().json || {};
const infos = Array.isArray(balance.balance_infos) ? balance.balance_infos : [];
const cnyRows = infos.filter((row) => String(row && row.currency || '').toUpperCase() === 'CNY');
const rawTotal = cnyRows.length === 1 ? cnyRows[0].total_balance : null;
const total = rawTotal === null || rawTotal === undefined || String(rawTotal).trim() === ''
  ? Number.NaN
  : Number(rawTotal);

if (balance.is_available !== true || cnyRows.length !== 1 || !Number.isFinite(total) || total < 10) {
  throw new Error(
    `[SEO_COST_GATE] DeepSeek unavailable or balance below CNY 10; ` +
    `is_available=${String(balance.is_available)}, cny_rows=${cnyRows.length}, ` +
    `total_balance=${Number.isFinite(total) ? total.toFixed(2) : 'invalid'}`
  );
}

const build = $('Build 2-Article Prompt').first().json || {};
if (!build.requestBody || typeof build.requestBody !== 'object') {
  throw new Error('[SEO_COST_GATE] generation requestBody is missing');
}

return [{
  json: {
    ...build,
    deepseek_preflight: { is_available: true, total_balance: total },
  },
}];
