# FUNLAB Direct Community Vote — 2026-09-11 Runbook

This file is the deployment and evidence runbook. The authoritative feature requirements and acceptance criteria are tracked in [GitHub Issue #13](https://github.com/frankiepan501-a11y/kol-marketing-automation/issues/13).

## Purpose

Use a player-first Nintendo Direct discussion to invite voluntary interest in the existing FUNLAB private controller test. No member receives a DM until they click the opt-in button.

## Member flow

1. FUN Bot publishes two adjacent messages: the original FUNLAB visual, official Direct links and opt-in button first, then the native Discord poll immediately below it.
2. A member votes in the native Discord poll.
3. The member may click `DM Me Tester Details` on the first message.
4. FUN Bot sends one duplicate-safe DM containing a Zelda 40th Anniversary interest question.
5. Choosing one answer updates the DM and reveals `Apply For Private Product Test`.
6. The existing two-step application opens. A successful application keeps the self-reported `报名来源` and records the Direct choice in `申请理由`.

## Safety and content boundaries

- The attached visual is an original FUNLAB asset under `app/assets/`.
- The post contains no Nintendo screenshots, logos, characters, Triforce, game UI, or official fonts.
- Nintendo content is represented only by factual title text and links to official Nintendo Direct pages.
- `allowed_mentions.parse` is empty and the post does not contain `@everyone`.
- The post says that the discussion and product test are not affiliated with or endorsed by Nintendo.
- DM delivery failure is shown to the clicking member; repeated clicks do not send a second campaign DM.

## Publish command

Run inside the deployed `kol-automation` container so the Discord token remains in the production secret environment:

```text
python -m app.discord_direct_campaign --channel-name tester-staff-rehearsal
python -m app.discord_direct_campaign --channel-name tester-staff-rehearsal --commit
python -m app.discord_direct_campaign --channel-name general
python -m app.discord_direct_campaign --channel-name general --commit --rehearsal-message-id <verified_hidden_message_id> --rehearsal-user-id <staff_tester_discord_user_id>
```

If the Zeabur web terminal is unavailable, the same publisher is exposed through the existing internal Bearer-authenticated service route:

```text
POST https://kol-auto.zeabur.app/discord/tester/admin/direct-campaign
Authorization: Bearer <INTERNAL_TOKEN>
Content-Type: application/json

{"channel_name":"tester-staff-rehearsal","commit":false}
{"channel_name":"tester-staff-rehearsal","commit":true}
{"channel_name":"general","commit":false}
{"channel_name":"general","commit":true,"rehearsal_message_id":"<verified_hidden_message_id>","rehearsal_user_id":"<staff_tester_discord_user_id>"}
```

Send one JSON body per request. Only `tester-staff-rehearsal` and `general` are accepted. The route calls the same duplicate-safe publisher and does not bypass the public rehearsal evidence gate. Treat a request as successful only after the response confirms `ok=true`, the expected `channel_id`, both stored IDs (`message_id` for the visual/link/button message and `poll_message_id` for the adjacent poll), `mention_everyone=false`, `poll_mention_everyone=false`, and both official URLs under `official_preview_urls`. A client timeout may leave one or both messages already posted; retrying is safe because each message has its own channel-scoped nonce and campaign marker, and the publisher resumes only the missing half.

The non-commit command validates FUN Bot identity, resolves one exact text channel, checks Discord history back to the campaign start for both campaign markers, validates the image, and returns the two final payloads without posting. The commit command publishes the two adjacent messages and reads both back: the first must contain the original image, opt-in button, both official Direct previews and `mention_everyone=false`; the second must contain the native poll, no attachment and `mention_everyone=false`. A public commit is blocked unless it is given the verified hidden main-message ID, the matching hidden poll exists, and a staff tester's FUN Bot DM shows an answered Zelda choice plus the contextual application button.

The short campaign funnel is measurable without storing message content: `cta_clicked`, `dm_opt_in`, `dm_failed`, `interest_selected`, and `application_saved` are emitted as `FUNLAB_DIRECT_EVENT` records in Zeabur runtime logs. Member IDs are pseudonymized. Completed applications remain in the existing Feishu application table with the self-reported `报名来源` and the Direct choice in `申请理由`.

## Verification

- Automated regression: `python -m unittest discover -s tests -p 'test_*.py'`
- Expected result on 2026-09-11 baseline: 1,027 tests pass.
- In the hidden employee channel, click the opt-in button, confirm one DM, choose one Zelda option, and open the two-step application.
- Before public posting, rerun the dry-run against `general`.

## Recovery

- Code rollback: revert the deployment commit and redeploy `kol-automation`.
- Message recovery: if the immutable poll is wrong, remove the poll message and rerun the publisher; it preserves the verified main message and recreates only the missing poll. Each half has its own stable marker and nonce.
- Do not restore the older paused `@everyone` reminder automations.

## Production evidence

- Deployment commit: pending
- Hidden rehearsal main message ID: pending
- Hidden rehearsal poll message ID: pending
- Public main message ID: pending
- Public poll message ID: pending
- Public channel ID: pending
