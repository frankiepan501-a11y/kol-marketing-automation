# NYXI X 低频增量采集运维说明

## 业务规则

- 正常期：北京时间每周一 09:30。
- 新品期：已确认 E1 正式开售日 `D0±30` 天内，加跑周三、周五 09:30。
- 每次从上次成功时间向前重叠 48 小时，按 `X + tweet_id` 去重。
- 只写竞品帖子表与 X 配置水位；不自动写 KOL 主表、不触达 KOL。

## 服务接口

- `POST /x-history/incremental/run?commit=true&force=false&async_mode=true`：启动增量任务。
- `GET /x-history/jobs/{job_id}`：查看任务进度与结果。
- `GET /x-history/jobs/{job_id}/assert`：供 n8n 轮询；运行中返回 409，失败返回 503，成功或正常跳过返回 200。
- `GET /x-history/status`：查看代码版本、查询组、运行状态和禁写边界。

以上接口均使用现有 KOL 服务内部 Bearer token，不新增或提交任何凭据。

## credits 告警

X API 返回 HTTP 402 时：

1. 本轮不写帖子，不推进成功水位；
2. 配置表记录 `credits_required` 与失败时间；
3. 通过聪哥分身1号向 Frankie 私聊发送 `AUDIT·P1` 告警；
4. 24 小时内相同告警不重复发送；
5. 充值后下次定时任务从旧水位自动续跑。

## 回滚

1. 在 n8n 停用 X 增量 workflow，即可停止新任务。
2. 回滚本服务到部署前 commit；不要删除历史帖子。
3. 因唯一键去重和 48 小时重叠，恢复后重复执行不会重复建帖。

