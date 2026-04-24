"""配置 - 全部从环境变量读"""
import os

def env(k, default=None, required=False):
    v = os.environ.get(k, default)
    if required and not v:
        raise RuntimeError(f"Missing env: {k}")
    return v

# 飞书 App 2号 (多维表格 + 消息)
FEISHU_BITABLE_APP_ID = env("FEISHU_BITABLE_APP_ID", required=True)
FEISHU_BITABLE_APP_SECRET = env("FEISHU_BITABLE_APP_SECRET", required=True)
# 飞书 App 1号 (通知 - open_id 归属此 App)
FEISHU_NOTIFY_APP_ID = env("FEISHU_NOTIFY_APP_ID", required=True)
FEISHU_NOTIFY_APP_SECRET = env("FEISHU_NOTIFY_APP_SECRET", required=True)

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

ZOHO_REGION = env("ZOHO_REGION", ".com")  # .com / .com.cn / .eu

# DeepSeek
DEEPSEEK_API_KEY = env("DEEPSEEK_API_KEY", required=True)

# 通知目标
NOTIFY_CHAT_ID = env("NOTIFY_CHAT_ID", "oc_4ddd938ddb73201ed7354337eb2226ac")
# 格式: "name1:open_id1,name2:open_id2,..."
NOTIFY_USERS_STR = env("NOTIFY_USERS",
    "潘志聪-Frankie:ou_629ce01f4bc31de078e10fcb038dbf78,"
    "吴晓丹:ou_c65fc5c31c650790db623640b7ac74f7,"
    "余琦华-独立站运营:ou_40e677ab2d99e763e48efec7f4eb8735,"
    "张佳烨-独立站运营:ou_d850dab47bdbaea6736709d354de4b0f"
)
NOTIFY_USERS = [(p.split(":", 1)[0], p.split(":", 1)[1]) for p in NOTIFY_USERS_STR.split(",") if ":" in p]

# 服务鉴权 (n8n 调用 webhook 时 Header: Authorization: Bearer <INTERNAL_TOKEN>)
INTERNAL_TOKEN = env("INTERNAL_TOKEN", required=True)


BRAND_CONFIG = {
    "FUNLAB": {
        "client_id": ZOHO_FUNLAB_CLIENT_ID,
        "client_secret": ZOHO_FUNLAB_CLIENT_SECRET,
        "refresh_token": ZOHO_FUNLAB_REFRESH_TOKEN,
        "account_id": ZOHO_FUNLAB_ACCOUNT_ID,
        "alias_from": ZOHO_FUNLAB_ALIAS,
    },
    "POWKONG": {
        "client_id": ZOHO_POWKONG_CLIENT_ID,
        "client_secret": ZOHO_POWKONG_CLIENT_SECRET,
        "refresh_token": ZOHO_POWKONG_REFRESH_TOKEN,
        "account_id": ZOHO_POWKONG_ACCOUNT_ID,
        "alias_from": ZOHO_POWKONG_ALIAS,
    },
}
