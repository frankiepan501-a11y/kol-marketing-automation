# Dave 未匹配竞品证据作者补全 P0 交接

## 结论

P0 已完成。系统可以从已验证的 NYXI 非官方帖子作者中锁定高证据样本，读取公开主页资料，并在写入 KOL 主表前完成国家、语言、内容相关性、官方身份、公开邮箱、全局重复和戴夫历史评测检查。

最终生产只读任务：`launchruntime-c84194113d49`。

- 样本：20 人
- 公开主页可读取：17 人
- 公开商务邮箱：6 人
- 通过全部预写入条件：3 人
- 全局邮箱重复：1 人
- 已评测/上稿戴夫：1 人
- 业务写入：0
- 活动参与记录写入：0
- 邮件草稿：0
- 邮件发送：0

## 通过预写入条件的 3 人

| KOL | 国家/语言 | 公开主页 | 结论 |
|---|---|---|---|
| Mekel Kasanova | US / en | `http://www.youtube.com/@MekelKasanova` | 可进入受控导入下一步 |
| Alec Hansen | US / en | `http://www.youtube.com/@Itsdadmode` | 可进入受控导入下一步 |
| Professor Shario | DE / de | `http://www.youtube.com/@professorshario` | 可进入受控导入下一步 |

“通过”只代表写前条件满足。本 P0 没有把三人写入主表或活动参与记录，也没有创建草稿或发送邮件。

## 正确拦下的历史对象

`NEED 4 NINTENDO`没有被当成新开发对象：公开邮箱已属于KOL主表记录`recvhwpbze5cv2`，且系统已找到戴夫评测历史；同时其国家为CA，不属于本次活动目标市场。下一步只能归并既有关系，不能重复发开发信。

## 代码与运行入口

- `app/relabel.py`：YouTube公开主页、国家、语言和公开邮箱解析。
- `app/launch_competitor_evidence.py`：帖子作者平台、主页和身份归并。
- `app/launch_candidate_preview.py`：KOL＋媒体人双主表、邮箱、目标产品历史和写前硬闸。
- `app/main.py`：后台只读入口`POST /launch/runtime/evidence-author-enrichment`及任务状态查询。

主要提交：

- `4899747 feat(kol): gate Dave evidence author enrichment`
- `1c5c23f fix(kol): reuse verified evidence author sample`
- `d00c0db fix(kol): parse populated YouTube about profile`

## 已解决的生产问题

1. 飞书`1254607 Data not ready`不能被误报为“帖子不存在”。
2. 20人资料补全不再重新验证近3000条竞品帖子，而是复用版本一致的完成态证据样本。
3. YouTube页面可能包含多个`aboutChannelViewModel`；解析器现在选择字段最完整的模型，不再取到空壳。
4. 判断“新KOL”同时覆盖KOL主表、媒体人主表、邮箱和目标产品历史，不能只比较作者ID。

## 验证

- 聚焦测试：108项＋3个子样例通过。
- 全仓测试：554项通过；唯一失败为未改动的Zeabur watchdog旧日期窗口fixture，与本P0无关。
- 生产回读：`read_only=true / writes=0 / participation_writes=0 / drafts_created=0 / emails_sent=0`。

## 后续待办

- P1：为3名通过者设计“受控导入但默认不触发自动邮件”的隔离入口，先单条回读再决定是否进入戴夫活动。
- P1：补X平台公开资料富化；本次X作者因国家、语言和邮箱证据不足而保持拦截。
- P2：将该模型抽象为活动级可选竞品来源；每个活动仍可选“新调查 / 引用历史 / 不使用竞品”，不得固定为NYXI。

## 证据边界

按当前业务规则，NYXI非官方渠道帖子默认作为合作线索参与排序；公开资料只能证明作者发布过相关内容，不能单独证明付费合作或正式商业关系。
