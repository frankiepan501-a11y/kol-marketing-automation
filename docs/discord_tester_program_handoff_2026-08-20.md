# FUNLAB Discord 新品体验官 P0 交接

## 结论

现有 FUN Bot 已增加新品体验官报名与封闭测试能力，不新建 Bot。公开招募仍由人工发布闸控制；系统的 P0 部署只创建隐藏预演区和员工测试消息，不会向公开频道发送公告。

## 业务交接界面

| 环节 | 系统自动完成 | 人工只需处理 | 合格标准 |
|---|---|---|---|
| 公开报名 | Discord 三步 Modal、硬门槛、客观预评分、写报名台账 | 发布公告、看边界项 | Discord 返回报名编号，台账有同一用户记录 |
| 入围核验 | 生成限时专属核验链接、保存遮挡后的凭证 | 核对 FUNLAB/Prime 等真实性 | 核验状态和附件写回申请记录 |
| 配送 | 独立配送表，不在 Discord 收地址电话 | 运营安排物流 | 配送字段只进入报名台账 |
| 测试反馈 | 收货、D2–3、D7、D10–14、安全和物流表单 | 产品/运营处理边界项 | 每条反馈可按申请编号追踪 |
| 安全事件 | 明确立即停用，单独记录 P0 | 技术/质量负责人判断 | 不要求用户复现危险问题 |

## 正式资源

- Discord Guild：`1009762946437619742`
- FUN Bot：`1485906070248493116`
- 报名台账：Base `KINabIENjak8fRsB6AHcIDALntc` / Table `tblt8oRYMtaa8B4v`
- 反馈台账：Base `KINabIENjak8fRsB6AHcIDALntc` / Table `tblVwzVNXVGu6ef5`
- 交互入口：`POST /discord/tester/interactions`
- 隐私告知：`GET /discord/tester/privacy`
- 体验官规则：`GET /discord/tester/rules`
- 限时表单：`GET|POST /discord/tester/forms/{kind}`
- 生成专属链接：`POST /discord/tester/admin/invitations`
- 隐藏区预览/创建：`POST /discord/tester/admin/setup?commit=false|true`
- 到期资料清理预览/执行：`POST /discord/tester/admin/retention?scope=verification|unselected|selected&commit=false|true`

## 环境变量

必须放在 Zeabur 环境变量，不能写入 Git：

- `DISCORD_BOT_TOKEN`：现有 FUN Bot Token
- `DISCORD_FUNLAB_GUILD_ID`
- `DISCORD_FUN_BOT_USER_ID`
- `DISCORD_TESTER_PUBLIC_ANNOUNCEMENT_CHANNEL_ID`
- `DISCORD_APPLICATION_PUBLIC_KEY`
- `DISCORD_TESTER_SIGNING_SECRET`：至少 32 位随机值
- `DISCORD_TESTER_APPLICATION_BASE_TOKEN`
- `DISCORD_TESTER_APPLICATION_TABLE_ID`
- `DISCORD_TESTER_FEEDBACK_TABLE_ID`

## 上线验证

1. `/health` 正常，并能打开隐私与规则页面。
2. Discord Developer Portal 接受交互地址。
3. `admin/setup?commit=false` 只返回计划；`commit=true` 只创建隐藏分类、角色、频道和 STAFF TEST 消息。
4. 员工账号完整走一次三步报名；Discord 先显示提交中，再返回报名编号。
5. 飞书报名台账出现测试记录，敏感地址/电话字段为空。
6. 用专属测试链接分别验证核验、配送和安全表单进入正确字段/表。
7. 公告发布前确认 `public_announcement_sent=false`。
8. 用 `admin/retention` 分别预演核验原件、未入选资料和活动结束资料的到期清理；默认 `commit=false` 只列受影响记录。

## 回退

- 代码回退：恢复到本次提交前版本并重新部署。
- Discord 回退：隐藏分类可继续保持隐藏；若确认无其他内容，再由管理员删除本次新增的隐藏角色/频道。
- 数据回退：只删除带明确 STAFF TEST 标记的测试记录，不触碰真实报名数据。
