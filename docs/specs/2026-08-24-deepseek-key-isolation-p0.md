# DeepSeek API Key isolation P0

## Goal

Remove live DeepSeek keys from this public repository and from active n8n
workflow JSON.  Route calls through four channel-specific secret variables so
one compromised workflow can be revoked without stopping every business line.

## Channels

| Channel | Secret variable | Workflows/service |
|---|---|---|
| KOL | `KOL_DEEPSEEK_API_KEY` | KOL service and influencer discovery |
| SEO | `SEO_DEEPSEEK_API_KEY` | News, commercial article, topic brief |
| Feishu | `FEISHU_EVENTHUB_DEEPSEEK_API_KEY` | Event hub, SKU and Sorftime commands |
| Manual | `MANUAL_TOOLS_DEEPSEEK_API_KEY` | Investment/manual tools and Amazon Listing |

## Safety requirements

- No new key may enter chat, Git, n8n workflow JSON, screenshots or docs.
- Migration CLI defaults to dry-run; `--commit` is explicit.
- A migrated workflow must fail before an API request if its channel variable
  is missing.
- n8n changes use complete GET -> minimal node edit -> PUT and preserve all
  unrelated nodes, connections and settings.
- `uvBfJBtGH93FPa6w` remains inactive.
- P0 does not trigger real publishing, emails, cards or Bitable writes.
- Frankie confirmed the previously exposed account key was revoked in the
  DeepSeek console before this migration began.  Historical Git cleanup is
  optional defense-in-depth and does not replace server-side revocation.

## Verification seams

1. KOL `chat_json()` sends only the KOL key.
2. Investment `_call_deepseek()` sends only the Manual key.
3. The n8n transformer removes all secret-like DeepSeek key literals and adds
   an explicit missing-variable guard to Code nodes.
4. Production readback reports zero hardcoded DeepSeek keys in the nine mapped
   active workflows.
