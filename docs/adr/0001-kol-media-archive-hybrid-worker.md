# ADR 0001: Hybrid KOL media archive worker

- Status: accepted
- Date: 2026-08-24

## Context

KOL upload rows already store public URLs and some historical Feishu file links, but the production service does not download, inspect, upload, or backfill media automatically. Real source files can exceed several gigabytes and TikTok/Instagram may require a residential network, proxy, cookies, or browser state.

Running the entire operation in a synchronous cloud request would exceed the shortest gateway window and would put large temporary files on ephemeral cloud storage. Running the whole workflow only on a local PC would make queue rules, YouTube metrics, and failures hard to audit remotely.

## Decision

Use a hybrid architecture:

- The Zeabur service is the controller. It validates the work table, selects one archive master per source-work group, refreshes YouTube metrics, writes snapshots, and exposes dry-run/replay endpoints.
- A single Windows worker claims explicitly enabled jobs and performs yt-dlp download, ffprobe/FFmpeg inspection, Feishu Drive multipart upload, and result writeback.
- Feishu Bitable is the durable queue and audit log. A worker crash must leave enough state for the same record to be replayed without scanning the whole table.
- Grey release uses an explicit `允许自动归档` checkbox and first runs on Frankie's old terminal. Production moves to Chen Xiangyu's deployment computer only after platform probes and one end-to-end sample pass.
- Three n8n schedules call safe controller ticks for queue scanning, YouTube metrics, and an external worker-health audit. The queue and metrics ticks are no-ops while `MEDIA_ARCHIVE_ENABLED=0`.

## Media quality decision

The worker requests the highest platform format available to its authenticated/network context and does not enforce a minimum resolution. It locks the audited video/audio format IDs for the download, then compares actual resolution and frame rate. It records expected maximum resolution, actual resolution, codecs, frame rate, bitrate, duration, file size, and SHA-256. It must not upscale or deliberately recompress the archive master.

Historical YouTube works receive one baseline snapshot labelled with the work's real age. Current counts are never backfilled as fake D0/D1 history. New works are observed at exact review milestones when the daily schedule reaches them.

## Alternatives rejected

- **Cloud-only download/upload:** unsuitable for multi-gigabyte files, long downloads, ephemeral storage, and platform anti-bot constraints.
- **Local-only orchestration:** makes business rules, YouTube metric schedules, queue health, and failures difficult to observe and replay remotely.
- **One file per platform row:** duplicates the same creative and wastes storage; source-work groups keep one master while preserving platform-specific URLs and metrics.

## Consequences

- The local worker requires yt-dlp, FFmpeg/ffprobe, lark-cli, enough disk space, and platform-specific proxy/cookie configuration.
- A cloud dead-man check is required because the local worker can be offline while the queue still exists.
- A stale or late worker callback must match the active job ID, source group, KOL, and product before any same-source rows are updated.
- TikTok/Instagram extraction can fail independently of the deterministic business rules and must report a platform-specific exception instead of asking operations to re-enter the whole record.
