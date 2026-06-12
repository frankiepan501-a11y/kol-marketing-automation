"""配置 - 全部从环境变量读

⚠️ 设计决策 (2026-05-11 拆 dtc-weekly service 时改):
原来 required=True 在 import 时 raise, 导致 dtc-weekly service (不用 KOL env)
启动就 crash. 改为: required=True 时仅 warn, 返回空字符串.
调用方用到该字段时自然会因空值失败 (401 / API error), 不影响 KOL 系统行为,
但允许同一 image 跑在没有完整 env 的 service 上.
"""
import logging
import os

_log = logging.getLogger("config")


def env(k, default=None, required=False):
    v = os.environ.get(k, default)
    if required and not v:
        _log.warning("env %s not set (required); usage will fail at call site", k)
        return ""
    return v

# 飞书 App 2号 (多维表格 + 消息)
FEISHU_BITABLE_APP_ID = env("FEISHU_BITABLE_APP_ID", required=True)
FEISHU_BITABLE_APP_SECRET = env("FEISHU_BITABLE_APP_SECRET", required=True)
# 飞书 App 1号 (通知 - open_id 归属此 App)
FEISHU_NOTIFY_APP_ID = env("FEISHU_NOTIFY_APP_ID", required=True)
FEISHU_NOTIFY_APP_SECRET = env("FEISHU_NOTIFY_APP_SECRET", required=True)
# 飞书 App 3号 (n8n 事件中心 - 发交互卡 + 收 card.action 回调; 回调只回发卡 app)
# 用于 warm_recap 暖信卡: 运营粘 UpPromote 券码 → 提交回 n8n event-hub YjTXaoWAcy89xZpT.
# ⚠️ repo 公开, secret 只能走 env, 不硬编码默认值.
FEISHU_APP3_ID = env("FEISHU_APP3_ID", required=True)
FEISHU_APP3_SECRET = env("FEISHU_APP3_SECRET", required=True)

FEISHU_APP_TOKEN = env("FEISHU_APP_TOKEN", required=True)  # Bitable app token

# 表 IDs
T_KOL = env("T_KOL", required=True)
T_EDITOR = env("T_EDITOR", required=True)
T_DRAFT = env("T_DRAFT", required=True)
T_KOL_FU = env("T_KOL_FU", required=True)
T_EDITOR_FU = env("T_EDITOR_FU", required=True)
T_DASH = env("T_DASH", required=True)
T_PRODUCT = env("T_PRODUCT", required=True)
T_TASK_KOL = env("T_TASK_KOL", required=True)
T_TASK_EDITOR = env("T_TASK_EDITOR", required=True)

# SKU 产品库 (采购治理源, 独立 wiki) — 产品英文名引用拼接用 (2026-06-02).
# 非 secret, 给默认值避免漏配; 聪哥2号(bitable app)已是该库协作者.
SKU_LIB_APP_TOKEN = env("SKU_LIB_APP_TOKEN", "MvtZb6OE9aJFaisO913cWSErnFe")
SKU_LIB_TABLE_ID = env("SKU_LIB_TABLE_ID", "tblwJ3BRkIuHDuSK")

# 2026-06-02 Fix B: 旧回复唤醒守卫. reply_monitor 处理的入站回复若 receivedTime(KOL 真发信时间,
# 非处理时间)距今 ≥ 此天数 = 被 recon 翻出的"久未互动旧回复" → reply_drafter 不自动生成 ship_confirm
# (改轻预热 stale_rewarm) + 强制人审, 防唐突寄样(mrbrian 反馈). 设 0 关闭.
try:
    STALE_REPLY_DAYS = int(env("STALE_REPLY_DAYS", "30") or 0)
except (ValueError, TypeError):
    STALE_REPLY_DAYS = 30

# Zoho (per brand)
ZOHO_FUNLAB_CLIENT_ID = env("ZOHO_FUNLAB_CLIENT_ID", required=True)
ZOHO_FUNLAB_CLIENT_SECRET = env("ZOHO_FUNLAB_CLIENT_SECRET", required=True)
ZOHO_FUNLAB_REFRESH_TOKEN = env("ZOHO_FUNLAB_REFRESH_TOKEN", required=True)
ZOHO_FUNLAB_ACCOUNT_ID = env("ZOHO_FUNLAB_ACCOUNT_ID", required=True)
ZOHO_FUNLAB_ALIAS = env("ZOHO_FUNLAB_ALIAS", "partner@fireflyfunlab.com")

ZOHO_POWKONG_CLIENT_ID = env("ZOHO_POWKONG_CLIENT_ID", required=True)
ZOHO_POWKONG_CLIENT_SECRET = env("ZOHO_POWKONG_CLIENT_SECRET", required=True)
ZOHO_POWKONG_REFRESH_TOKEN = env("ZOHO_POWKONG_REFRESH_TOKEN", required=True)
ZOHO_POWKONG_ACCOUNT_ID = env("ZOHO_POWKONG_ACCOUNT_ID", required=True)
ZOHO_POWKONG_ALIAS = env("ZOHO_POWKONG_ALIAS", "partner@powkong.com")

# 白牌 (Linyuvo, 2026-06-08) — 中性外联身份, 给白牌产品发 cold. 选填:
# env 未设(client_id 空) → 不挂此品牌, POWKONG/FUNLAB 不受影响。
ZOHO_WHITELABEL_CLIENT_ID = env("ZOHO_WHITELABEL_CLIENT_ID", "")
ZOHO_WHITELABEL_CLIENT_SECRET = env("ZOHO_WHITELABEL_CLIENT_SECRET", "")
ZOHO_WHITELABEL_REFRESH_TOKEN = env("ZOHO_WHITELABEL_REFRESH_TOKEN", "")
ZOHO_WHITELABEL_ACCOUNT_ID = env("ZOHO_WHITELABEL_ACCOUNT_ID", "")
ZOHO_WHITELABEL_ALIAS = env("ZOHO_WHITELABEL_ALIAS", "support@linyuvo.com")

ZOHO_REGION = env("ZOHO_REGION", ".com")  # .com / .com.cn / .eu

# 2026-06-01 修 reply_monitor alias 盲区 (审计实证: 手动从 marketing@/frankie@ 外联的回复
# 0% 捕获 — reply_monitor 之前只搜 to:partner@; 27 KOL 受害含 NEED 4 NINTENDO 收货却零追踪).
# 除 partner@ 主别名外, 额外监控这些手动外联收件箱. 非 partner@ 别名的回复=人工高触达关系→强制人审.
# env 覆盖: POWKONG_REPLY_ALIASES / FUNLAB_REPLY_ALIASES (逗号分隔).
REPLY_EXTRA_ALIASES = {
    "POWKONG": [a.strip() for a in env("POWKONG_REPLY_ALIASES",
        "marketing@powkong.com,frankie@powkong.com").split(",") if a.strip()],
    "FUNLAB": [a.strip() for a in env("FUNLAB_REPLY_ALIASES",
        "marketing@fireflyfunlab.com").split(",") if a.strip()],
}

# DeepSeek
DEEPSEEK_API_KEY = env("DEEPSEEK_API_KEY", required=True)

# 2026-06-04: DeepSeek 余额预警阈值(元). 余额低于此或不可用 → /deepseek/balance-check 飞书告警 Frankie.
# 根因: DeepSeek 欠费 → 整条 AI 生成链(enrich/reply_drafter/regen/talking_points)静默 402 停摆,
# 全靠运营踩到才发现(张佳烨 2026-06-04 重生 402)。这是关键依赖的 dead-man-switch。
try:
    DEEPSEEK_BALANCE_ALERT_THRESHOLD = float(env("DEEPSEEK_BALANCE_ALERT_THRESHOLD", "10") or 10)
except (ValueError, TypeError):
    DEEPSEEK_BALANCE_ALERT_THRESHOLD = 10.0

# 2026-06-04: 编辑(媒体人)邮箱域名退信率守卫. 编辑邮箱靠 {fi}{last}@域名 猜测, 特定大媒体域名
# (engadget/vox/theverge/destructoid 实测 33-50% 退信)格式系统性错→个人邮箱根本猜不到。
# 守卫: 发媒体人 cold 前算该域名历史无效率, 同域名「无效」数≥MIN 且 无效率≥RATE → 不发(猜测准是浪费),
# 标'域名高退信-需人工找邮箱/PR inbox'。数据驱动自适应, 多数中小媒体(0%退信)不受影响。
try:
    EDITOR_DOMAIN_BOUNCE_MIN = int(env("EDITOR_DOMAIN_BOUNCE_MIN", "2") or 2)
except (ValueError, TypeError):
    EDITOR_DOMAIN_BOUNCE_MIN = 2
try:
    EDITOR_DOMAIN_BOUNCE_RATE = float(env("EDITOR_DOMAIN_BOUNCE_RATE", "0.3") or 0.3)
except (ValueError, TypeError):
    EDITOR_DOMAIN_BOUNCE_RATE = 0.3

# 2026-06-10: KOL 端邮箱域名退信率守卫 (泛化 editor 守卫到 KOL). KOL cold 邮箱来自爬虫/聚合平台,
# 同样系统性退信. 阈值默认同 editor, 可独立 env 覆盖. 数据驱动: 同域名「无效」≥MIN 且 率≥RATE → 拦.
try:
    KOL_DOMAIN_BOUNCE_MIN = int(env("KOL_DOMAIN_BOUNCE_MIN", "2") or 2)
except (ValueError, TypeError):
    KOL_DOMAIN_BOUNCE_MIN = 2
try:
    KOL_DOMAIN_BOUNCE_RATE = float(env("KOL_DOMAIN_BOUNCE_RATE", "0.3") or 0.3)
except (ValueError, TypeError):
    KOL_DOMAIN_BOUNCE_RATE = 0.3

# 2026-06-12: KOL 入池全局粉丝下限门槛(env 可调). YouTube daemon 用 limit 抓搜索结果**无质量门槛**,
# 把粉丝个位数的废号全入库(审计: 3099 无邮箱号粉丝中位仅 77, 抽样 3-8 粉僵尸号)。enrich 派单筛选时,
# 任务没设「筛选-粉丝下限」(=0)则用此兜底, 防废号被派单。任务设了更高/低值则尊重任务。
try:
    KOL_MIN_FANS_FLOOR = int(env("KOL_MIN_FANS_FLOOR", "5000") or 5000)
except (ValueError, TypeError):
    KOL_MIN_FANS_FLOOR = 5000

# 2026-06-10: MCN/营销聚合域名静态黑名单 (A 类). 这些域名的地址是"频道名@代投域名"硬拼, 整域作废
# (退 1 次即拉黑, 不等退信率攒够). 与动态退信率守卫(B 类: 真实大媒体域名 engadget/vox 等, 只拦后续)互补.
# 命中即停发 cold 草稿 + 标联系人「邮箱验真状态=无效」. 可逆: 从此 env 移除即恢复.
# 数据来源: 2026-06-10 退信审计 (moreyellow.com 4次/fullscreen/apollomgmt/fluencify 等聚合平台域名).
AGGREGATOR_BLOCK_DOMAINS = {
    d.strip().lower() for d in env(
        "AGGREGATOR_BLOCK_DOMAINS",
        "moreyellow.com,fullscreen.com,apollomgmt.co,fluencify.io,"
        "ellify.com,studio71.email,intheblackmedia.com,influencerxbrand.com"
    ).split(",") if d.strip()
}

# 2026-06-04: Snov.io Email Finder — 编辑(媒体人)真邮箱解析, 治本替代 {fi}{last}@域名 猜测.
# enrich_editor 生成编辑 cold 草稿前调 finder 取真邮箱(valid 放行域名守卫; unknown 照发退信回标;
# 找不到/不可用 → 降级现状). 凭证 repo 公开只走 env 不硬编. SNOV_EDITOR_FINDER_ENABLED=0 可关.
SNOV_CLIENT_ID = env("SNOV_CLIENT_ID", required=True)
SNOV_CLIENT_SECRET = env("SNOV_CLIENT_SECRET", required=True)
SNOV_EDITOR_FINDER_ENABLED = (env("SNOV_EDITOR_FINDER_ENABLED", "1") or "1") != "0"

# 2026-06-05: KOL 上稿×任务进度 周报留档表 (按产品审计). 非 secret, 给默认值.
T_UPLOAD_REPORT = env("T_UPLOAD_REPORT", "tblHrlzTeSIhOjCY")

# 通知目标
# 2026-06-08: KOL/媒体人业务通知群 → 电商运营部(Frankie 拍板从站外运营部 oc_4ddd…26ac 迁出)。
# 告警类(退信/重复/端点失败)不进此群, 改私聊 Frankie+负责运营(见 bounce_monitor/kol_dedup/main)。
NOTIFY_CHAT_ID = env("NOTIFY_CHAT_ID", "oc_8b71a652a25ec0dd1c8af2c53e86ed93")
# 格式: "name1:open_id1,name2:open_id2,..."
NOTIFY_USERS_STR = env("NOTIFY_USERS",
    # 2026-06-08: 余琦华已离职(飞书人事 resigned=True), 从默认值移除保持与 Zeabur env 一致。
    # 注: reviewer/ship_main 角色走职务实时查(已自动过滤离职), 此默认仅 env 未设时兜底。
    "潘志聪-Frankie:ou_629ce01f4bc31de078e10fcb038dbf78,"
    "吴晓丹:ou_c65fc5c31c650790db623640b7ac74f7,"
    "张佳烨-独立站运营:ou_d850dab47bdbaea6736709d354de4b0f"
)
NOTIFY_USERS = [(p.split(":", 1)[0], p.split(":", 1)[1]) for p in NOTIFY_USERS_STR.split(",") if ":" in p]

# KOL/编辑 草稿待审通知的"主审"职务 (按飞书人事「职务」列原文, feishu-people-as-source-of-truth 铁律)
# 2026-05-15: draft_router._notify_human_review + sla_check L1 都用此职务实时查在职名单
KOL_REVIEWER_JOB_TITLE = env("KOL_REVIEWER_JOB_TITLE", "独立站运营专员")

# 服务鉴权 (n8n 调用 webhook 时 Header: Authorization: Bearer <INTERNAL_TOKEN>)
INTERNAL_TOKEN = env("INTERNAL_TOKEN", required=True)

# Phase B — Amazon Attribution per-KOL ROI (POWKONG US, 2026-06-09). 全部可选:
# 任一未配 → amazon_attribution.is_enabled()=False → sales_attribution 跳过亚马逊源,
# 现网行为完全不变 (同白牌品牌"env 配齐才挂载"模式)。凭据走 Zeabur env, 不入仓。
#   AMZ_ADS_CLIENT_ID / _SECRET   : Login with Amazon (LWA) 应用
#   AMZ_ADS_REFRESH_TOKEN         : OAuth 授权码流程换得 (/amazon/oauth/callback 一次性)
#   AMZ_ADS_PROFILE_ID            : GET /v2/profiles 里 US/POWKONG 的 profileId
#   AMZ_OAUTH_REDIRECT_URI        : LWA Allowed Return URL (与授权请求逐字一致)
AMZ_ADS_CLIENT_ID = env("AMZ_ADS_CLIENT_ID", "")
AMZ_ADS_CLIENT_SECRET = env("AMZ_ADS_CLIENT_SECRET", "")
AMZ_ADS_REFRESH_TOKEN = env("AMZ_ADS_REFRESH_TOKEN", "")
AMZ_ADS_PROFILE_ID = env("AMZ_ADS_PROFILE_ID", "")
AMZ_OAUTH_REDIRECT_URI = env("AMZ_OAUTH_REDIRECT_URI",
                             "https://kol-auto.zeabur.app/amazon/oauth/callback")


BRAND_CONFIG = {
    "FUNLAB": {
        "client_id": ZOHO_FUNLAB_CLIENT_ID,
        "client_secret": ZOHO_FUNLAB_CLIENT_SECRET,
        "refresh_token": ZOHO_FUNLAB_REFRESH_TOKEN,
        "account_id": ZOHO_FUNLAB_ACCOUNT_ID,
        "alias_from": ZOHO_FUNLAB_ALIAS,
        # 2026-06-08 配置驱动元数据(供 dispatch/reply_monitor/brand 识别派生, 行为不变):
        "domain": "fireflyfunlab.com",          # reply_monitor OUR_DOMAINS
        "match": ("funlab", "firefly"),         # 邮箱/别名子串 → 品牌识别
        "sender_label": "FUNLAB邮箱(@funlabswitch.com)",  # dispatch 发送邮箱字段值
        "naming": "auto",                       # 产品英文名: 自动拼
    },
    "POWKONG": {
        "client_id": ZOHO_POWKONG_CLIENT_ID,
        "client_secret": ZOHO_POWKONG_CLIENT_SECRET,
        "refresh_token": ZOHO_POWKONG_REFRESH_TOKEN,
        "account_id": ZOHO_POWKONG_ACCOUNT_ID,
        "alias_from": ZOHO_POWKONG_ALIAS,
        "domain": "powkong.com",
        "match": ("powkong",),
        "sender_label": "POWKONG邮箱(@powkong.com)",
        "naming": "manual",
    },
}

# 白牌(Linyuvo) — 仅在 env 配齐时挂载, 否则不存在此品牌(POWKONG/FUNLAB 完全不受影响)
if ZOHO_WHITELABEL_CLIENT_ID and ZOHO_WHITELABEL_REFRESH_TOKEN:
    BRAND_CONFIG["白牌"] = {
        "client_id": ZOHO_WHITELABEL_CLIENT_ID,
        "client_secret": ZOHO_WHITELABEL_CLIENT_SECRET,
        "refresh_token": ZOHO_WHITELABEL_REFRESH_TOKEN,
        "account_id": ZOHO_WHITELABEL_ACCOUNT_ID,
        "alias_from": ZOHO_WHITELABEL_ALIAS,
        "domain": "linyuvo.com",
        "match": ("linyuvo",),
        "sender_label": "白牌邮箱(@linyuvo.com)",
        "naming": "manual_generic",             # 纯品类名, 运营手填, 无品牌前缀
    }


def brand_from_text(text: str) -> str:
    """从邮箱地址/发送邮箱别名文本匹配品牌 key (按 BRAND_CONFIG['match'] 子串)。无匹配返回 ''。
    取代各处写死的 if 'powkong'/'funlab' 判断, 加品牌只动 BRAND_CONFIG。"""
    s = (text or "").lower()
    for _brand, _cfg in BRAND_CONFIG.items():
        for _m in _cfg.get("match", ()):
            if _m and _m in s:
                return _brand
    return ""
