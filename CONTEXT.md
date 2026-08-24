# KOL marketing automation domain context

This service treats Feishu Bitable as the source of truth for KOL/media workflow facts and Feishu Drive as the source of truth for archived reusable media files.

## Upload work

An **upload work** is one public post on one platform. One YouTube, TikTok, or Instagram URL occupies one row even when the same creative was cross-posted elsewhere.

Required business facts:

- one platform and one public URL;
- one linked KOL and one linked product;
- one brand;
- one source-work group.

## Source-work group

A **source-work group** joins platform rows that contain the same underlying creative. It is not used for different edits or separate shoots by the same KOL.

Only one **archive master** is stored for a source-work group. Source priority is `YouTube -> Instagram -> TikTok`; a lower-priority source is used only when the higher-priority source is absent or has exhausted retries. Every row in the group receives the same direct Feishu file URL.

## Archive job

An **archive job** is a deterministic local-worker task for one archive master:

1. validate row format and relations;
2. inspect platform formats;
3. download the highest available video and audio without intentional quality reduction;
4. inspect the resulting file with ffprobe/FFmpeg;
5. upload it to `红人素材 / 视频 / 品牌 / 产品`;
6. write the direct `/file/{file_token}` URL and evidence back to every row in the source-work group.

The quality gate has no minimum resolution. A low-resolution source may be archived, but the system must record the platform maximum and actual media specification. Upscaling or lossy recompression must not be used to make a file appear clearer.

## Metrics snapshot

The upload-work row stores the latest public metrics. A **metrics snapshot** stores the historical observation used for partnership review. YouTube metrics use the official YouTube Data API. Public share/save counts are not available from that API and remain blank rather than being inferred.

## Execution boundary

The cloud service validates rows, selects archive masters, refreshes YouTube metrics, and exposes queue state. A Windows local worker performs downloads, media inspection, and large Feishu uploads. Grey release runs on Frankie's old terminal before the same bundle is moved to Chen Xiangyu's deployment computer.

The worker must be observable and replayable: every job has a run ID, worker ID, stage, retry count, timestamps, error evidence, and a single-record replay command.

Scheduled queue and metrics entrypoints stay inert until the production feature flag is enabled. Worker completion/failure callbacks must match the active job ID. A verified local upload receipt allows a callback retry without blindly reusing an unrelated same-name Drive file.
