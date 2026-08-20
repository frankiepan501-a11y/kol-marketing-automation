# FUNLAB Discord 体验官简版报名设计

日期：2026-08-21  
状态：已获 Frankie 口头确认，待规格复审与最终文件确认

## 1. 目标

把现有 Discord 原生报名从 3 步 15 题缩短为 2 步 10 题，降低中途放弃率，同时保留资格判断、消费经验、设备覆盖、配件偏好和样品匹配所需信息。新增“最喜欢的游戏 IP/系列”字段，最多填写 3 个，用于运营筛选后的产品外观匹配。

本次只修改隐藏员工演练和后续公开报名共用的 FUN Bot 交互、飞书报名台账字段及评分/路线推导。不得发布公开招募，不收集地址、电话、邮箱、订单截图或 Prime 截图。

## 2. 问卷结构

### Step 1 Of 2 — Eligibility（5题）

| 字段 | Discord 标签 | 输入示例 | 校验与用途 |
|---|---|---|---|
| 国家或地区 | Country Or Region | `United States` | 必须属于当前可配送国家；对外不公布国家配额 |
| 年龄 | Are You 18 Or Older? | `YES` | 必须为 YES |
| 设备 | Devices You Actively Use | `Switch 2, Steam Deck, PC Steam` | Switch 1 或 Switch 2 至少一种；其他设备用于路线加分 |
| Amazon经验 | Amazon Video Games Purchase In 24 Months? | `YES` | 必须为 YES |
| 总承诺 | 14-Day Test, Privacy And Rules? | `YES` | 一次确认14天测试、保密、隐私告知、体验官规则及不要求评价 |

### Step 2 Of 2 — Match And Preferences（5题）

| 字段 | Discord 标签 | 输入示例 | 校验与用途 |
|---|---|---|---|
| 消费档案 | Amazon, FUNLAB And Prime Profile | `COUNT=4-6; FUNLAB=YES; PRIME=YES` | COUNT只能为1/2-3/4-6/7+；FUNLAB为YES/NO；PRIME为YES/NO/PREFER NOT |
| 游戏时长 | Weekly Play Profile | `SWITCH=6-10; PC=2-5; CROSS=YES` | 延用现有时长档位并规范为飞书标准选项 |
| 游戏IP偏好 | Favorite Game IPs Or Franchises | `Pokémon; Zelda; Mario` | 自由文本，最多3个；分号或逗号分隔；每项去首尾空格、去重，超出3个时明确报错 |
| 使用场景 | Games, Platforms And Controllers | `Mario Kart World—Switch 2; Hades—Steam Deck; Nintendo Pro Controller` | 合并原“拟测试游戏”和“近24个月使用手柄”两题 |
| 配件关注点 | What Matters Most To You In Gaming Accessories? | `Comfort; low latency; durability` | 简短填写最注重的1–3个方面，用于了解产品偏好，不作为反馈能力考试 |

### 输入清洗规则

- 国家不区分大小写并去除空格和标点。允许：`US/USA/United States/United States of America`、`CA/Canada`、`MX/Mexico`、`UK/United Kingdom/Great Britain`、`DE/Germany`、`FR/France`、`IT/Italy`、`ES/Spain`，统一写入两位代码。
- 设备不区分大小写。识别 `Switch/Switch 1/Switch1`、`Switch 2/Switch2`、`Steam Deck/SteamDeck`、`PC/PC Steam/Steam`；Switch 1 与 Switch 2 必须分开记账。
- 消费档案键名和值不区分大小写，允许逗号或分号分隔并忽略等号两侧空格。`COUNT`只能为`1/2-3/4-6/7+`；`FUNLAB`只能为`YES/NO`；`PRIME`只能为`YES/NO/PREFER NOT/PREFER NOT TO SAY`。
- Switch时长输入允许`UNDER 2/2-5/6-10/11-20/20+`；PC时长允许`0/UNDER 2/2-5/6-10/10+`。`2-5/6-10`分别写成飞书标准值`2–5/6–10`。
- IP先做 Unicode NFKC 规范化，再把中文或英文逗号、分号统一为分隔符；去首尾空格，按不区分大小写的规范值去重，保留首次输入的显示形式。清洗后必须有1–3项，每项最多80字符，总长度最多240字符。
- 配件关注点采用与IP相同的分隔、去空格和去重规则；清洗后必须有1–3项，每项最多60字符，总长度最多180字符。允许自然语言短语，不限定固定选项。

## 3. 删除与合并

- 删除独立“Amazon购买品类”：最近24个月 Amazon Video Games 已是硬门槛，购买次数足以辅助消费经验排序。
- 删除独立“跨平台手柄经验”：设备、CROSS、使用场景已覆盖。
- 合并“游戏与平台”和“过去24个月使用手柄”。
- 删除“断连记录”和“功能测试方法”，改为更简单的“最注重游戏配件的哪些方面”。
- 删除报名者手填“主测试路线”；由系统按设备自动建议，运营仍可在飞书改选。
- 把 RULES 同意并入 Step 1 的总承诺，不再在最后要求填写键值串。

## 4. 自动测试路线

系统只生成初始建议，不自动决定最终样品款式。本节的“有 Switch”明确指设备中存在 Switch 1 或 Switch 2 任意一种。优先级如下：

1. 有 Switch 且有 Steam Deck → `Switch + Steam Deck`
2. 否则有 Switch 且有 PC / Steam，且 PC 每周手柄时长不少于 2 小时 → `Switch + PC Steam`
3. 否则有 Switch 2 → `Switch 2`
4. 否则 → `Switch`

| Switch 1 | Switch 2 | Steam Deck | PC≥2小时 | 自动路线 |
|---|---|---|---|---|
| 任一 | 任一 | 是 | 任意 | Switch + Steam Deck |
| 任一 | 任一 | 否 | 是 | Switch + PC Steam |
| 否 | 是 | 否 | 否 | Switch 2 |
| 是 | 否 | 否 | 否 | Switch |

运营可结合样本覆盖、产品兼容性和 IP 偏好调整路线。游戏 IP 只用于推荐设计，不代表承诺提供某个 IP，也不得绕过产品库 IP 合规状态。

## 5. 飞书台账

在 `FUNLAB新品体验官报名台账`（`tblt8oRYMtaa8B4v`）新增两个文本字段：

- `喜爱游戏IP`：保存清洗后的最多3项，以 `; ` 连接。
- `配件关注点`：保存清洗后的最多3项，以 `; ` 连接。

现有字段映射调整：

- `游戏与手柄使用经验`、`拟测试场景`：均保存“Games, Platforms And Controllers”完整答案，兼容现有两个运营视图。
- `Amazon购买品类`、`断连问题回答`、`功能测试回答`、`申请理由`：新报名明确写空字符串；旧记录未重新报名时保持原值。
- `主测试路线`：由系统推导。
- `理解非抽奖且不要求评价`、`承诺完成测试`、`同意保密`：Step 1 总承诺为 YES 时同时写 true。
- `可选加入Tester Alumni`：新报名写 false；后续完成测试后再单独征求自愿加入，不在公开报名阶段询问。

保存继续按“Discord用户ID + 活动批次”幂等更新：同一用户同一批次重新提交会更新原记录，不新增重复行。新表单会按上面的完整字段矩阵覆盖新字段并清空已废弃字段，防止残留旧三步答案；没有重新报名的旧记录不迁移、不覆盖。表中现有 ASCII 短横线重复选项不在本次删改，以免影响旧记录；新报名统一写标准选项。

## 6. 评分口径

客观预评分仍以 47 分为上限：

- Amazon购买次数：`1=6`、`2–3=9`、`4–6=12`、`7+=15`。
- 设备与跨平台覆盖：任一Switch基础5分；Switch 2加7分；Steam Deck加5分；PC / Steam且PC时长为`2–5/6–10/10+`加5分；`CROSS=YES`加3分；本部分最高25分。
- 完整承诺4分、两步表单完整提交3分，共7分。

人工评分53分：FUNLAB历史购买核验15分、Prime核验8分、IP与可提供产品匹配10分、配件关注点与可提供产品匹配10分、Discord历史与社区可信度10分。系统说明不得继续提“两道反馈题”或“反馈能力考试”。IP和配件关注点只供人工参考；本次不接产品库查询、AI匹配或自动分款。

## 7. 交互与错误处理

- 表单版本为`v2`。起始按钮仍使用稳定入口`tester_apply_start`，但只打开`tester_apply_v2_step1`；继续按钮和最终Modal使用带签名状态的`tester_apply_v2_continue2.*`与`tester_apply_v2_step2.*`。
- Step 1 的国家与设备等紧凑状态通过现有签名token传到 Step 2；服务端同时保留30分钟草稿，token被篡改、过期或服务重启导致草稿不存在时，提示“Form expired. Please start again.”。
- 部署后，旧演练消息上的稳定起始按钮直接打开v2。已经打开但未完成的v1 `tester_apply_step1/step2/step3`、旧继续按钮统一返回“旧表单已更新，请重新点击 Test Application Flow”，不得继续写飞书。
- Step 1 提交成功后显示 `Continue To Final Step`，打开 Step 2。
- Step 2 保存成功后返回飞书 `record_id` 作为申请编号。
- Discord交互必须在3秒内响应；飞书写入继续使用后台任务和完成回执。
- 任一格式错误只返回给当前报名者的临时消息，并给出可复制的正确示例。Discord Modal提交后不能原地恢复全部输入，因此错误回执同时提供`Restart Application`按钮；用户重新打开该步并复制修正，不承诺自动预填旧内容。
- IP或配件关注点为空、超过3项、单项/总长度超限，以及消费档案格式错误、设备不满足或未同意规则时，均不得写飞书。
- 不在 Discord 消息或日志中回显敏感资料；本流程本身不收集敏感资料。

## 8. 验收与测试

1. 按钮打开 `Step 1 Of 2`，第一步正好5题。
2. 第一步通过后只出现一次继续按钮，打开 `Step 2 Of 2`，第二步正好5题。
3. 只填 Switch 2 时资格通过且飞书不额外写 Switch 1。
4. IP输入 `Pokémon; Zelda; Mario` 写入 `喜爱游戏IP`；配件关注点输入 `Comfort; low latency; durability` 写入`配件关注点`；两者重复项去重，第4项触发错误且不写台账。
5. 自动路线分别覆盖 Steam Deck、PC Steam、Switch 2、Switch 四条分支。
6. ASCII时长输入写入标准飞书选项，PC使用加分不丢失。
7. 使用场景写入`游戏与手柄使用经验`和`拟测试场景`，配件关注点写入`配件关注点`；`Amazon购买品类`、`断连问题回答`、`功能测试回答`、`申请理由`四个废弃字段必须显式写空字符串。
8. 完成一次隐藏区虚拟报名，核对全部字段后删除测试记录。
9. 公开公告保持未发送。

补充负例和精确断言：

- 同一Discord用户同一活动批次提交两次，飞书仍为同一`record_id`，废弃字段为空，新字段为第二次答案。
- v1旧Modal、旧继续按钮、篡改token、过期草稿均不能写表，并返回重新开始提示。
- 四条路线按上表逐项断言；PC `2–5/6–10/10+`分别断言包含5分，低于2小时或0不加分。
- Amazon四档分别断言6/9/12/15分；完整组合断言总分且不超过47。
- IP空值、第4项、单项超80字符、总长度超240字符均拒绝；配件关注点空值、第4项、单项超60字符、总长度超180字符均拒绝；大小写与中英文标点重复项正确去重。
- 飞书后台保存失败时先向Discord返回已受理，再通过完成回执明确告知保存失败和隐私邮箱，不得显示虚假申请编号。
- 交互单元测试断言处理函数在3秒响应预算内返回；真实隐藏区演练记录端到端耗时。

## 9. 发布与回退

- 发布顺序是强制闸：先由当前飞书用户在目标表创建`喜爱游戏IP`和`配件关注点`两个文本字段并回读类型；再用生产所用2号App/现有Bitable通道做“新增测试记录→读取字段一致→删除”权限smoke；任一步失败即停止，不部署代码。
- 字段与Bot smoke通过后，才运行本地专项测试和全量测试、提交并推送生产`master`。
- 等 Zeabur 部署状态为 RUNNING 后回读线上端点。
- 在隐藏员工频道重新发送或更新演练消息，使用新按钮完成虚拟报名。
- 隐藏区虚拟报名必须核对2步10题、申请编号、IP字段、配件关注点、路线、分数和废弃字段清空，然后删除测试记录；通过前禁止公开发布。
- 回退方式：回退该单一代码提交；飞书新增文本字段可以保留为空，不影响旧流程。
