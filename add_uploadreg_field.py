# -*- coding: utf-8 -*-
import json, urllib.request
APP_ID="cli_a9f6ae86fce8dbd8"; APP_SECRET="r0eQTiBoP1WnQCUnBanMQeu5ACT57at7"
APP_TOKEN="KINabIENjak8fRsB6AHcIDALntc"; T_KOL="tblMMhnj2hEbhF6y"
def post(u,b,h=None):
    r=urllib.request.Request(u,data=json.dumps(b).encode(),method="POST"); r.add_header("Content-Type","application/json")
    for k,v in (h or {}).items(): r.add_header(k,v)
    return json.loads(urllib.request.urlopen(r,timeout=30).read())
def get(u,h):
    r=urllib.request.Request(u,method="GET")
    for k,v in h.items(): r.add_header(k,v)
    return json.loads(urllib.request.urlopen(r,timeout=30).read())
tok=post("https://open.feishu.cn/open-apis/auth/v3/tenant_access_token/internal",{"app_id":APP_ID,"app_secret":APP_SECRET})["tenant_access_token"]
hdr={"Authorization":f"Bearer {tok}"}
base=f"https://open.feishu.cn/open-apis/bitable/v1/apps/{APP_TOKEN}/tables/{T_KOL}/fields"
names=[f["field_name"] for f in get(base+"?page_size=200",hdr)["data"]["items"]]
if "上稿登记卡发送时间" in names:
    print("已存在 上稿登记卡发送时间, 跳过")
else:
    r=post(base,{"field_name":"上稿登记卡发送时间","type":5,"ui_type":"DateTime"},hdr)
    print("field:", r.get("code"), r.get("data",{}).get("field",{}).get("field_id"))
