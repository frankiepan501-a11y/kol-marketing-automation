# -*- coding: utf-8 -*-
"""KOL 漏斗统一阶段模型 (v4 地基) — 单一真相源.

⚠️ 本文件由 SSOT 飞书表 tbltdvn4F8VgezCb 程序化生成 (生成器: C:/tmp/gen_stage_model.py).
   改阶段模型请改飞书表后重新生成, 不要手改本文件 (会与表漂移).
   飞书表 = 人编辑的设计稿; 本文件 = 代码消费的派生枚举.

消费方 (待接): reply_monitor 分类器(只从 INBOUND_REPLY_LABELS 选) / 卡片 funnel_stage /
             draft_router force_review(FORCE_REVIEW_LABELS) / sop_gap(FALLBACK_LABEL).
"""

# 驱动方 (值与飞书表「驱动方」列严格一致)
DRIVER_INBOUND = "入站回复"      # 对方回信触发, 分类器从这些里选
DRIVER_SYSTEM = "系统流程"       # dispatch/auto_send/sla/退信处理器 自动置位
DRIVER_PROACTIVE = "主动跟进"    # 我方按时间/SLA 主动发

# 是否人审 (值与「是否人审」列严格一致)
REVIEW_FORCE = "强制人审"
REVIEW_LOW_CONF = "低置信人审"
REVIEW_NONE = "否"

FALLBACK_LABEL = "unclassified_fallback"  # 分类器无法判断时的兜底 label

# label -> {funnel_stage, driver, review, name_cn, trigger, template_id}
SCENARIO_MODEL = {
    "interested_no_rate": {
        "seq": 1, "funnel_stage": "报价前", "driver": "入站回复",
        "review": "否", "template_id": "T01_INTERESTED_NO_RATE",
        "name_cn": "感兴趣但没报价",
        "trigger": "对方说 interested / would love to learn more / ask details，但没给 rate",
    },
    "details_requested": {
        "seq": 2, "funnel_stage": "报价前", "driver": "入站回复",
        "review": "否", "template_id": "T02_DETAILS_REQUESTED",
        "name_cn": "对方问 brief/deliverables",
        "trigger": "经纪人问具体创意、brief、timeline、内容要求",
    },
    "future_opportunity": {
        "seq": 3, "funnel_stage": "报价前", "driver": "入站回复",
        "review": "否", "template_id": "T03_FUTURE_OPPORTUNITY",
        "name_cn": "拒绝但态度好",
        "trigger": "竞品合作、档期、暂时不能做",
    },
    "not_product_fit": {
        "seq": 4, "funnel_stage": "报价前", "driver": "入站回复",
        "review": "否", "template_id": "T04_NOT_PRODUCT_FIT",
        "name_cn": "产品不匹配",
        "trigger": "不会，不适合当前产品",
    },
    "auto_reply": {
        "seq": 5, "funnel_stage": "报价前", "driver": "入站回复",
        "review": "否", "template_id": "T05_AUTO_REPLY",
        "name_cn": "自动回复/OOO",
        "trigger": "out of office、vacation、auto reply",
    },
    "usage_question_asked": {
        "seq": 6, "funnel_stage": "报价前", "driver": "入站回复",
        "review": "低置信人审", "template_id": "T06_USAGE_QUESTION_ASKED",
        "name_cn": "主动问 usage/exclusivity",
        "trigger": "\"Will there be any usage or exclusivity?\"",
    },
    "rate_acceptable": {
        "seq": 7, "funnel_stage": "报价谈判", "driver": "入站回复",
        "review": "否", "template_id": "T07_RATE_ACCEPTABLE",
        "name_cn": "已报价-价格可接受",
        "trigger": "对方 rate 在预算内，或你们愿意接受",
    },
    "negotiation_needed": {
        "seq": 8, "funnel_stage": "报价谈判", "driver": "入站回复",
        "review": "强制人审", "template_id": "T08_NEGOTIATION_NEEDED",
        "name_cn": "已报价-需讲价",
        "trigger": "rate 高于预算，但仍想合作",
    },
    "too_expensive": {
        "seq": 9, "funnel_stage": "报价谈判", "driver": "入站回复",
        "review": "低置信人审", "template_id": "T09_TOO_EXPENSIVE",
        "name_cn": "报价过高-暂放弃",
        "trigger": "rate 明显超预算，比如几千/几万美金",
    },
    "accepted_next_steps": {
        "seq": 10, "funnel_stage": "报价谈判", "driver": "入站回复",
        "review": "否", "template_id": "T10_ACCEPTED_NEXT_STEPS",
        "name_cn": "接受报价",
        "trigger": "\"That works for me / happy to move forward\"",
    },
    "contract_needed": {
        "seq": 11, "funnel_stage": "合同", "driver": "主动跟进",
        "review": "强制人审", "template_id": "T11_CONTRACT_NEEDED",
        "name_cn": "合同发送",
        "trigger": "条件确认后发合同",
    },
    "contract_revision_requested": {
        "seq": 12, "funnel_stage": "合同", "driver": "入站回复",
        "review": "强制人审", "template_id": "T12_CONTRACT_REVISION_REQUESTED",
        "name_cn": "合同修改/条款疑问",
        "trigger": "VAT、kill fee、usage、liability、付款节点等",
    },
    "waiting_signed_contract": {
        "seq": 13, "funnel_stage": "合同", "driver": "主动跟进",
        "review": "否", "template_id": "T13_WAITING_SIGNED_CONTRACT",
        "name_cn": "等签回合同",
        "trigger": "已发合同，对方说没问题但未签回",
    },
    "contract_chase_stalled": {
        "seq": 14, "funnel_stage": "合同", "driver": "主动跟进",
        "review": "否", "template_id": "T14_CONTRACT_CHASE_STALLED",
        "name_cn": "签约遇阻(已读不回)",
        "trigger": "已发合同并跟进多次，但对方已读不回",
    },
    "ready_to_ship": {
        "seq": 15, "funnel_stage": "寄样物流", "driver": "入站回复",
        "review": "否", "template_id": "T15_READY_TO_SHIP",
        "name_cn": "已签合同/收齐地址",
        "trigger": "对方发回合同和地址",
    },
    "tracking_sent": {
        "seq": 16, "funnel_stage": "寄样物流", "driver": "主动跟进",
        "review": "否", "template_id": "T16_TRACKING_SENT",
        "name_cn": "发货/给tracking",
        "trigger": "已发货，需要通知",
    },
    "delivery_check": {
        "seq": 17, "funnel_stage": "寄样物流", "driver": "主动跟进",
        "review": "否", "template_id": "T17_DELIVERY_CHECK",
        "name_cn": "包裹将到/已到",
        "trigger": "tracking 显示预计到达或已签收",
    },
    "brief_sent": {
        "seq": 18, "funnel_stage": "brief拍摄", "driver": "主动跟进",
        "review": "否", "template_id": "T18_BRIEF_SENT",
        "name_cn": "brief发送",
        "trigger": "包裹到达或即将到达，发 brief",
    },
    "brief_question": {
        "seq": 19, "funnel_stage": "brief拍摄", "driver": "入站回复",
        "review": "低置信人审", "template_id": "T19_BRIEF_QUESTION",
        "name_cn": "拍摄前确认细节",
        "trigger": "问语言、时长、是否露脸、voiceover、hook、产品卖点",
    },
    "draft_due_reminder": {
        "seq": 20, "funnel_stage": "草稿", "driver": "主动跟进",
        "review": "否", "template_id": "T20_DRAFT_DUE_REMINDER",
        "name_cn": "草稿到期前提醒",
        "trigger": "draft due date 快到了",
    },
    "draft_overdue": {
        "seq": 21, "funnel_stage": "草稿", "driver": "主动跟进",
        "review": "否", "template_id": "T21_DRAFT_OVERDUE",
        "name_cn": "草稿逾期",
        "trigger": "到时间没给 draft",
    },
    "delay_acknowledged": {
        "seq": 22, "funnel_stage": "草稿", "driver": "入站回复",
        "review": "低置信人审", "template_id": "T22_DELAY_ACKNOWLEDGED",
        "name_cn": "对方说忙/会延迟",
        "trigger": "对方解释 忙、在编辑、需要更多时间",
    },
    "video_submitted": {
        "seq": 23, "funnel_stage": "草稿", "driver": "入站回复",
        "review": "强制人审", "template_id": "T23_VIDEO_SUBMITTED",
        "name_cn": "收到视频草稿",
        "trigger": "对方发 draft/video",
    },
    "revision_needed": {
        "seq": 24, "funnel_stage": "草稿", "driver": "入站回复",
        "review": "强制人审", "template_id": "T24_REVISION_NEEDED",
        "name_cn": "草稿需修改",
        "trigger": "缺少过程、caption、字幕、tag、hashtag、卖点",
    },
    "approved_to_post": {
        "seq": 25, "funnel_stage": "发布收口", "driver": "入站回复",
        "review": "否", "template_id": "T25_APPROVED_TO_POST",
        "name_cn": "草稿通过/准备发布",
        "trigger": "视频 ok，提醒发布要求",
    },
    "live_link_needed": {
        "seq": 26, "funnel_stage": "发布收口", "driver": "主动跟进",
        "review": "否", "template_id": "T26_LIVE_LINK_NEEDED",
        "name_cn": "已发布未给链接",
        "trigger": "对方说 posted 或应该已发(可事先查看下网红账户)",
    },
    "payment_or_code_question": {
        "seq": 27, "funnel_stage": "发布收口", "driver": "入站回复",
        "review": "强制人审", "template_id": "T27_PAYMENT_OR_CODE_QUESTION",
        "name_cn": "付款/折扣码/佣金",
        "trigger": "问 payment、code usage、bank info、PayPal",
    },
    "objection_correction": {
        "seq": 28, "funnel_stage": "异常处理", "driver": "入站回复",
        "review": "强制人审", "template_id": "T28_OBJECTION_CORRECTION",
        "name_cn": "质疑/澄清(纠错)[我方独有]",
        "trigger": "I've never made X / That's not my channel / You have me confused with someone else",
    },
    "unsubscribe": {
        "seq": 29, "funnel_stage": "异常处理", "driver": "入站回复",
        "review": "否", "template_id": "T29_UNSUBSCRIBE",
        "name_cn": "退订[我方独有]",
        "trigger": "unsubscribe / please remove me / stop emailing",
    },
    "unclassified_fallback": {
        "seq": 30, "funnel_stage": "异常处理", "driver": "入站回复",
        "review": "低置信人审", "template_id": "T30_UNCLASSIFIED_FALLBACK",
        "name_cn": "无法判断(兜底)[我方独有]",
        "trigger": "无法判断意图 / 系统兜底",
    },
    "candidate_sourced": {
        "seq": 31, "funnel_stage": "线索筛选", "driver": "系统流程",
        "review": "否", "template_id": "S31_CANDIDATE_SOURCED",
        "name_cn": "KOL候选已发现/入库",
        "trigger": "爬虫(专题9)/搜索/手工录入，未评分未触达",
    },
    "candidate_qualified": {
        "seq": 32, "funnel_stage": "线索筛选", "driver": "系统流程",
        "review": "否", "template_id": "S32_CANDIDATE_QUALIFIED",
        "name_cn": "评分通过/派单就绪",
        "trigger": "6维评分≥阈值(80)+已生cold草稿待发; 70-79待人审, <70退回重生(≤2次)",
    },
    "cold_sent_awaiting_reply": {
        "seq": 33, "funnel_stage": "线索筛选", "driver": "系统流程",
        "review": "否", "template_id": "S33_COLD_SENT_AWAITING_REPLY",
        "name_cn": "cold已发待回复",
        "trigger": "cold邮件已发出，等KOL首次回复(此后分叉进入站回复27场景)",
    },
    "live_link_received": {
        "seq": 34, "funnel_stage": "发布收口", "driver": "入站回复",
        "review": "强制人审", "template_id": "T34_LIVE_LINK_RECEIVED",
        "name_cn": "达人主动发回上稿链接",
        "trigger": "KOL 发 'here's my video: [link]' / 主动给上稿URL",
    },
    "repeat_collab_invite": {
        "seq": 35, "funnel_stage": "发布收口", "driver": "主动跟进",
        "review": "否", "template_id": "T35_REPEAT_COLLAB_INVITE",
        "name_cn": "二次合作/复购邀约",
        "trigger": "已上稿+ROI好/合作顺畅 → 主动邀约下一轮",
    },
    "our_email_broken_feedback": {
        "seq": 36, "funnel_stage": "异常处理", "driver": "入站回复",
        "review": "强制人审", "template_id": "T36_OUR_EMAIL_BROKEN_FEEDBACK",
        "name_cn": "对方反馈我们邮件空白/损坏",
        "trigger": "KOL 回复说收到空白/损坏/未发全的邮件(实例: mafastudios 'didnt sent the full email' / ctatechdesk '2 blank emails this week')",
    },
    "email_bounced": {
        "seq": 37, "funnel_stage": "异常处理", "driver": "系统流程",
        "review": "否", "template_id": "S37_EMAIL_BOUNCED",
        "name_cn": "退信/无效地址",
        "trigger": "mailer-daemon 退信(Undelivered/Delivery Failure/Returned); 90天数据53封(47硬退,多为编辑猜测地址如 elegantweapon@pro)",
    },
}

# ---- 派生集合 (供消费方直接用) ----
SCENARIO_LABELS = frozenset(SCENARIO_MODEL)
INBOUND_REPLY_LABELS = [k for k, v in SCENARIO_MODEL.items() if v["driver"] == DRIVER_INBOUND]
FORCE_REVIEW_LABELS = frozenset(k for k, v in SCENARIO_MODEL.items() if v["review"] == REVIEW_FORCE)
LOW_CONF_REVIEW_LABELS = frozenset(k for k, v in SCENARIO_MODEL.items() if v["review"] == REVIEW_LOW_CONF)

def funnel_stage_of(label): return (SCENARIO_MODEL.get(label) or {}).get("funnel_stage", "")
def driver_of(label): return (SCENARIO_MODEL.get(label) or {}).get("driver", "")
def is_force_review(label): return label in FORCE_REVIEW_LABELS
def is_known_label(label): return label in SCENARIO_LABELS

SCENARIO_COUNT = 37  # 同步校验锚点; 改表后重新生成此文件应同步更新
assert len(SCENARIO_MODEL) == SCENARIO_COUNT, "stage_model 行数与锚点不符, 需重新生成"
