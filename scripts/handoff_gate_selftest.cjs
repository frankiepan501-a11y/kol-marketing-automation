const fs = require('node:fs');
const assert = require('node:assert/strict');
const AsyncFunction = Object.getPrototypeOf(async function(){}).constructor;
const source = fs.readFileSync(__dirname + '/n8n/handoff_authorize.js', 'utf8');
const run = new AsyncFunction('$input', source);
(async () => {
  const rows = [{json:{card_action:{action:'draft_approve'}}}];
  const input = {all:()=>rows};
  assert.equal(await run.call({helpers:{httpRequest:async()=>({allowed:true})}}, input), rows);
  for (const result of [{allowed:false},{},{allowed:'true'}]) {
    await assert.rejects(run.call({helpers:{httpRequest:async()=>result}},input));
  }
  await assert.rejects(run.call({helpers:{httpRequest:async()=>{throw Error('offline')}}},input));
  rows[0].json.card_action.action='unrelated';
  assert.equal(await run.call({helpers:{httpRequest:async()=>{throw Error('must not call')}}},input), rows);
  console.log('6 gate assertions passed');
})().catch(e=>{console.error(e.message);process.exitCode=1});
