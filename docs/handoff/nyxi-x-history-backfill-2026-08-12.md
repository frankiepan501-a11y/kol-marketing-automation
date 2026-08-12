# NYXI X 历史补采交接

## 目标与边界

- 用 X 官方 Full Archive Search 补采公开可发现的 NYXI 竞品相关帖子。
- 写入现有竞品帖子表 `KINabIENjak8fRsB6AHcIDALntc/tblCDbvLtnLzdxEp`。
- 以 `7:<tweet_id>` 作为唯一键，重复运行不重复建帖。
- 不写 KOL 主表 `tblMMhnj2hEbhF6y`，不修改合作阶段，不发邮件或飞书消息。
- “全量”指已确认查询矩阵下的公开可发现并集；删除、私密、地区限制及没有文本线索的内容不在此口径内。

## 查询口径

当前版本 `nyxi-x-full-v3` 用 3 个全历史查询组：

1. NYXI 品牌/官方 handle/官网域名 + 游戏手柄语境。
2. Hyperion、Wizard、Warrior 型号组 + 游戏手柄语境。
3. Master P1、Chaos Pro、NJ12、NYXI Flexi、Athena、Striker、Imperial 等型号组 + 游戏手柄语境。

每组从 2006-03-21 查到运行时刻，用 X 返回的分页标记继续。不再按年拆分，以免重叠查询浪费 Full Archive 配额。

## 飞书字段

- `采集来源 = X API`
- `X命中查询词`：保留帖子命中的查询组证据。
- `X查询时间窗`：保留本次全历史查询边界。
- `X历史进度`：位于关键词配置表 `tblgWfvdPgbkq541`，按查询组保存下一个索引。

## 运行与回放入口

- `GET /x-history/probe`：只读检查 Full Archive 权限。
- `POST /x-history/run`：启动预演或正式补采。
- `GET /x-history/jobs/{job_id}`：查看单次运行进度、限额恢复时间和结果。
- `POST /x-history/jobs/{job_id}/stop`：请求停止运行。
- `GET /x-history/status`：查看最近任务和查询配置。

使用 `commit=false` 时不写入飞书。正式运行用 `commit=true&resume=true`；每完成一个查询组才推进飞书检查点，失败后可继续未完成查询组。

## 已验证安全性

- 小样预演写入数为 0。
- 任务开始前 KOL 主表为 7,517 条，程序对主表只读计数。
- X Bearer Token 只从 Zeabur 环境变量读取，不写日志、Git、飞书或交接文档。
- 遇到 HTTP 429 时根据 `x-rate-limit-reset` 自动等待，不盲目循环重试。

## 本次生产结果

待正式任务完成后回填：新增/更新数、日期范围、非官方作者数、重复键及 KOL 主表前后数量。
