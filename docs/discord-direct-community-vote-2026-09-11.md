# FUNLAB Direct Community Vote — 2026-09-11 Runbook

This file is the deployment and evidence runbook. The authoritative feature requirements and acceptance criteria are tracked in [GitHub Issue #13](https://github.com/frankiepan501-a11y/kol-marketing-automation/issues/13).

## Purpose

Use a player-first Nintendo Direct discussion to invite voluntary interest in the existing FUNLAB private controller test. No member receives a DM until they click the opt-in button.

## Member flow

1. A member votes in the native Discord poll.
2. The member may click `DM Me Tester Details`.
3. FUN Bot sends one duplicate-safe DM containing a Zelda 40th Anniversary interest question.
4. Choosing one answer updates the DM and reveals `Apply For Private Product Test`.
5. The existing two-step application opens. A successful application keeps the self-reported `报名来源` and records the Direct choice in `申请理由`.

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
POST /discord/tester/admin/direct-campaign
{"channel_name":"tester-staff-rehearsal","commit":false}
```

Only `tester-staff-rehearsal` and `general` are accepted. The route calls the same duplicate-safe publisher and does not bypass the public rehearsal evidence gate.

The non-commit command validates FUN Bot identity, resolves one exact text channel, checks Discord history back to the campaign start for duplicates, validates the image, and returns the final payload without posting. The commit command posts once and reads the stored Discord message back to verify the author, image, poll, button, both official Direct previews, and `mention_everyone=false`. A public commit is blocked unless it is given the verified hidden rehearsal message ID and a staff tester whose FUN Bot DM shows an answered Zelda choice plus the contextual application button.

The short campaign funnel is measurable without storing message content: `cta_clicked`, `dm_opt_in`, `dm_failed`, `interest_selected`, and `application_saved` are emitted as `FUNLAB_DIRECT_EVENT` records in Zeabur runtime logs. Member IDs are pseudonymized. Completed applications remain in the existing Feishu application table with the self-reported `报名来源` and the Direct choice in `申请理由`.

## Verification

- Automated regression: `python -m unittest discover -s tests -p 'test_*.py'`
- Expected result on 2026-09-11 baseline: 1,025 tests pass.
- In the hidden employee channel, click the opt-in button, confirm one DM, choose one Zelda option, and open the two-step application.
- Before public posting, rerun the dry-run against `general`.

## Recovery

- Code rollback: revert the deployment commit and redeploy `kol-automation`.
- Message recovery: remove the public Discord message if the immutable poll is wrong. The stable campaign marker prevents accidental duplicate posts in the same channel.
- Do not restore the older paused `@everyone` reminder automations.

## Production evidence

- Deployment commit: pending
- Hidden rehearsal message ID: pending
- Public message ID: pending
- Public channel ID: pending
