"""KOL ROI P1 gap scan.

Reads local UpPromote/Amazon exports and compares them with the KOL/Editor
tables in Feishu Base. By default it only writes local CSV outputs. Pass
--write-gaps to create records in the KOL attribution gap table.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import subprocess
from pathlib import Path
from xml.etree import ElementTree as ET
from zipfile import ZipFile


BASE_TOKEN = "KINabIENjak8fRsB6AHcIDALntc"
T_KOL = "tblMMhnj2hEbhF6y"
T_EDITOR = "tblinUWFZHtmXZbC"
T_MAP = "tblzxyUxNF7gWqJe"
T_GAP = "tbliU8GDl6SU9b4y"
LARK_CLI = r"C:\Users\Administrator\bin\lark-cli.exe"

XLS_NS = {
    "a": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
    "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships",
}
PKG_REL_NS = "http://schemas.openxmlformats.org/package/2006/relationships"


def norm_text(value: object) -> str:
    return str(value or "").strip()


def norm_email(value: object) -> str:
    return norm_text(value).lower()


def norm_alnum(value: object) -> str:
    return re.sub(r"[^a-z0-9]", "", norm_text(value).lower())


def slug_tokens(value: object) -> set[str]:
    return {t for t in re.findall(r"[a-z0-9]+", norm_text(value).lower()) if len(t) >= 4}


def clean_money(value: object) -> float:
    s = re.sub(r"[^0-9.\-]", "", norm_text(value))
    try:
        return float(s or 0)
    except ValueError:
        return 0.0


def col_to_idx(cell_ref: str) -> int:
    m = re.match(r"([A-Z]+)", cell_ref or "")
    if not m:
        return 0
    idx = 0
    for ch in m.group(1):
        idx = idx * 26 + ord(ch) - 64
    return idx - 1


def xlsx_rows(path: Path, sheet_index: int = 0) -> list[list[str]]:
    with ZipFile(path) as zf:
        shared = []
        if "xl/sharedStrings.xml" in zf.namelist():
            root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
            for si in root.findall("a:si", XLS_NS):
                shared.append("".join(t.text or "" for t in si.findall(".//a:t", XLS_NS)))

        wb = ET.fromstring(zf.read("xl/workbook.xml"))
        rels = ET.fromstring(zf.read("xl/_rels/workbook.xml.rels"))
        relmap = {
            r.attrib["Id"]: r.attrib["Target"]
            for r in rels.findall(f"{{{PKG_REL_NS}}}Relationship")
        }
        sheets = wb.findall("a:sheets/a:sheet", XLS_NS)
        sheet = sheets[sheet_index]
        rid = sheet.attrib.get(f"{{{XLS_NS['r']}}}id")
        target = relmap.get(rid, "")
        sheet_path = "xl/" + target.lstrip("/") if not target.startswith("xl/") else target

        out = []
        root = ET.fromstring(zf.read(sheet_path))
        for row in root.findall(".//a:sheetData/a:row", XLS_NS):
            values = []
            cursor = 0
            for cell in row.findall("a:c", XLS_NS):
                idx = col_to_idx(cell.attrib.get("r", ""))
                while cursor < idx:
                    values.append("")
                    cursor += 1
                cell_type = cell.attrib.get("t")
                if cell_type == "inlineStr":
                    value = "".join(t.text or "" for t in cell.findall(".//a:t", XLS_NS))
                else:
                    v = cell.find("a:v", XLS_NS)
                    value = "" if v is None or v.text is None else v.text
                    if cell_type == "s" and str(value).isdigit():
                        value = shared[int(value)] if int(value) < len(shared) else value
                values.append(value)
                cursor += 1
            out.append(values)
        return out


def table_from_rows(rows: list[list[str]]) -> list[dict[str, str]]:
    if not rows:
        return []
    header = [norm_text(h) for h in rows[0]]
    data = []
    for row in rows[1:]:
        if not any(norm_text(v) for v in row):
            continue
        item = {}
        for idx, name in enumerate(header):
            if name:
                item[name] = norm_text(row[idx] if idx < len(row) else "")
        data.append(item)
    return data


def read_xlsx_table(path: Path) -> list[dict[str, str]]:
    return table_from_rows(xlsx_rows(path))


def read_csv_table(path: Path) -> list[dict[str, str]]:
    for encoding in ("utf-8-sig", "gbk", "cp1252"):
        try:
            with path.open(newline="", encoding=encoding) as f:
                return list(csv.DictReader(f))
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", b"", 0, 1, f"cannot decode {path}")


def latest(paths: list[Path]) -> Path | None:
    return max(paths, key=lambda p: p.stat().st_mtime) if paths else None


def run_lark(args: list[str]) -> dict:
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    cp = subprocess.run(
        [LARK_CLI, *args],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        env=env,
    )
    if cp.returncode != 0:
        raise RuntimeError((cp.stderr or cp.stdout)[-1000:])
    data = json.loads(cp.stdout)
    if not data.get("ok"):
        raise RuntimeError(json.dumps(data.get("error"), ensure_ascii=False))
    return data


def fetch_base_contacts(table_id: str, name_field: str, object_type: str) -> list[dict]:
    fields = [name_field, "邮箱", "UTM ID", "折扣码", "亚马逊CampaignID", "主链接"]
    offset = 0
    out = []
    while True:
        args = [
            "base", "+record-list",
            "--base-token", BASE_TOKEN,
            "--table-id", table_id,
            "--offset", str(offset),
            "--limit", "200",
            "--format", "json",
        ]
        for field in fields:
            args += ["--field-id", field]
        envelope = run_lark(args)
        payload = envelope["data"]
        names = payload["fields"]
        for rid, row in zip(payload.get("record_id_list") or [], payload.get("data") or []):
            item = dict(zip(names, row))
            out.append({
                "record_id": rid,
                "object_type": object_type,
                "name": norm_text(item.get(name_field)),
                "email": norm_email(item.get("邮箱")),
                "utm_id": norm_text(item.get("UTM ID")),
                "discount_code": norm_text(item.get("折扣码")).upper(),
                "amazon_campaign_id": norm_text(item.get("亚马逊CampaignID")),
                "main_link": norm_text(item.get("主链接")),
            })
        if not payload.get("has_more"):
            break
        offset += 200
    return out


def index_contacts(contacts: list[dict]) -> dict:
    idx = {"email": {}, "name": {}, "handle": {}, "amz": {}, "code": {}}
    for c in contacts:
        if c["email"]:
            idx["email"].setdefault(c["email"], c)
        key = norm_alnum(c["name"])
        if key:
            idx["name"].setdefault(key, c)
        utm = c["utm_id"].lower()
        handle = norm_alnum(utm[4:] if utm.startswith("kol_") else utm)
        if len(handle) >= 4:
            idx["handle"].setdefault(handle, c)
        if c["amazon_campaign_id"]:
            idx["amz"].setdefault(c["amazon_campaign_id"], c)
        if c["discount_code"]:
            idx["code"].setdefault(c["discount_code"], c)
    return idx


def match_affiliate(row: dict, idx: dict) -> tuple[dict | None, str, str]:
    email = norm_email(row.get("affiliate_email") or row.get("email"))
    name = norm_text(row.get("affiliate_name") or row.get("full_name"))
    if email and email in idx["email"]:
        return idx["email"][email], "email", "强"
    key = norm_alnum(name)
    if key and key in idx["name"]:
        return idx["name"][key], "name", "中"
    return None, "", "无"


def match_amazon_campaign(campaign: str, idx: dict) -> tuple[dict | None, str, str]:
    if campaign in idx["amz"]:
        return idx["amz"][campaign], "amazon_campaign_id", "强"
    tokens = slug_tokens(campaign)
    hits = [contact for handle, contact in idx["handle"].items() if handle in tokens]
    uniq = {h["record_id"]: h for h in hits}
    if len(uniq) == 1:
        return next(iter(uniq.values())), "handle_token", "中"
    for key, contact in idx["name"].items():
        if key and key in norm_alnum(campaign):
            return contact, "name_substring", "弱"
    return None, "", "无"


def gap_id(source: str, raw_key: str) -> str:
    digest = hashlib.sha1(f"{source}:{raw_key}".encode("utf-8")).hexdigest()[:12]
    return f"{source}-{digest}"


def write_csv(path: Path, rows: list[dict], headers: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        for row in rows:
            writer.writerow({h: row.get(h, "") for h in headers})


def dedupe_gaps(rows: list[dict]) -> list[dict]:
    """Collapse order-level gaps into one operator task per affiliate/campaign."""
    deduped = {}
    for row in rows:
        key_value = row.get("raw_email") or norm_alnum(row.get("raw_name")) or row.get("source_record_id")
        key = (row.get("gap_type", ""), row.get("source", ""), key_value)
        current = deduped.get(key)
        if not current:
            deduped[key] = dict(row)
            continue
        ids = [current.get("source_record_id", ""), row.get("source_record_id", "")]
        current["source_record_id"] = ",".join(sorted(set(",".join(ids).split(","))))[:500]
        current["notes"] = (current.get("notes", "") + " | " + row.get("notes", "")).strip(" | ")[:1000]
        if row.get("actionable") == "是":
            current["actionable"] = "是"
    return list(deduped.values())


def create_gap_record(row: dict) -> None:
    payload = {
        "缺口ID": row["gap_id"],
        "缺口类型": row["gap_type"],
        "处理状态": "待发卡",
        "来源系统": row["source"],
        "来源记录ID": row.get("source_record_id", ""),
        "原始名称": row.get("raw_name", ""),
        "原始邮箱": row.get("raw_email", ""),
        "原始链接或活动": row.get("raw_campaign_or_link", ""),
        "候选KOL": row.get("candidate", ""),
        "推荐动作": row.get("recommended_action", ""),
        "备注": row.get("notes", ""),
    }
    run_lark([
        "base", "+record-upsert",
        "--base-token", BASE_TOKEN,
        "--table-id", T_GAP,
        "--json", json.dumps(payload, ensure_ascii=False),
    ])


def fetch_existing_gap_ids() -> set[str]:
    offset = 0
    existing = set()
    while True:
        envelope = run_lark([
            "base", "+record-list",
            "--base-token", BASE_TOKEN,
            "--table-id", T_GAP,
            "--field-id", "缺口ID",
            "--offset", str(offset),
            "--limit", "200",
            "--format", "json",
        ])
        payload = envelope["data"]
        for row in payload.get("data") or []:
            if row and row[0]:
                existing.add(norm_text(row[0]))
        if not payload.get("has_more"):
            break
        offset += 200
    return existing


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", default=r"D:\Users\Administrator\Desktop\ROI归因")
    ap.add_argument("--out-dir", default=r"D:\Users\Administrator\Desktop\ROI归因")
    ap.add_argument("--write-gaps", action="store_true")
    ap.add_argument("--max-write", type=int, default=50)
    ns = ap.parse_args()

    src = Path(ns.input_dir)
    out = Path(ns.out_dir)
    referrals_path = latest(list(src.glob("uppromote_referrals_*.xlsx")))
    payments_path = latest(list(src.glob("uppromote_approved_payments_*.xlsx")))
    affiliates_path = latest(list(src.glob("UpPromote-Top-Affiliates-*.xlsx")))
    amazon_path = latest(list(src.glob("*Amazon*Attribution*.csv")))

    contacts = fetch_base_contacts(T_KOL, "账号名", "KOL")
    contacts += fetch_base_contacts(T_EDITOR, "媒体人姓名", "媒体人")
    idx = index_contacts(contacts)

    matched = []
    gaps = []

    def add_up_rows(rows: list[dict], source_label: str) -> None:
        for row in rows:
            contact, via, confidence = match_affiliate(row, idx)
            raw_name = norm_text(row.get("affiliate_name") or row.get("full_name"))
            raw_email = norm_email(row.get("affiliate_email") or row.get("email"))
            source_id = norm_text(row.get("id") or row.get("order_id") or raw_email or raw_name)
            if contact:
                matched.append({
                    "source": "UpPromote",
                    "source_file": source_label,
                    "source_record_id": source_id,
                    "raw_name": raw_name,
                    "raw_email": raw_email,
                    "match_via": via,
                    "confidence": confidence,
                    "object_type": contact["object_type"],
                    "kol_record_id": contact["record_id"],
                    "kol_name": contact["name"],
                    "sales": row.get("total_sales", ""),
                    "commission": row.get("commission", row.get("total_commissions", "")),
                })
            else:
                referrals = clean_money(row.get("referrals") or row.get("total_referrals"))
                sales = clean_money(row.get("total_sales"))
                commission = clean_money(row.get("commission") or row.get("total_commissions"))
                actionable = "是"
                if source_label.startswith("UpPromote-Top-Affiliates") and referrals == 0 and sales == 0 and commission == 0:
                    actionable = "否"
                gaps.append({
                    "gap_id": gap_id("upromote", source_id),
                    "gap_type": "UpPromote未匹配",
                    "source": "UpPromote",
                    "source_file": source_label,
                    "source_record_id": source_id,
                    "raw_name": raw_name,
                    "raw_email": raw_email,
                    "raw_campaign_or_link": row.get("referral_link", ""),
                    "candidate": "",
                    "recommended_action": "发运营卡片：选择对应KOL/媒体人或填写新映射；系统写KOL归因映射表。",
                    "notes": f"免费版 UpPromote 无 API，导出明细作为校验样本。referrals={referrals}; sales={sales}; commission={commission}",
                    "actionable": actionable,
                })

    if referrals_path:
        add_up_rows(read_xlsx_table(referrals_path), referrals_path.name)
    if payments_path:
        add_up_rows(read_xlsx_table(payments_path), payments_path.name)
    if affiliates_path:
        add_up_rows(read_xlsx_table(affiliates_path), affiliates_path.name)

    if amazon_path:
        for row in read_csv_table(amazon_path):
            campaign = norm_text(row.get("广告活动") or row.get("Campaign") or row.get("campaign"))
            contact, via, confidence = match_amazon_campaign(campaign, idx)
            if contact:
                matched.append({
                    "source": "Amazon Attribution",
                    "source_file": amazon_path.name,
                    "source_record_id": campaign,
                    "raw_name": campaign,
                    "raw_email": "",
                    "match_via": via,
                    "confidence": confidence,
                    "object_type": contact["object_type"],
                    "kol_record_id": contact["record_id"],
                    "kol_name": contact["name"],
                    "sales": row.get("商品销量", row.get("Sales", "")),
                    "commission": "",
                })
            else:
                clicks = int(clean_money(row.get("点击量") or row.get("Clicks")))
                sales = clean_money(row.get("商品销量") or row.get("Sales"))
                purchases = clean_money(row.get("购买") or row.get("Purchases"))
                gaps.append({
                    "gap_id": gap_id("amazon", campaign),
                    "gap_type": "Amazon未匹配",
                    "source": "Amazon Attribution",
                    "source_file": amazon_path.name,
                    "source_record_id": campaign,
                    "raw_name": campaign,
                    "raw_email": "",
                    "raw_campaign_or_link": campaign,
                    "candidate": "",
                    "recommended_action": "发亚马逊运营卡片：补 KOL handle/campaignId 映射；后续创建连接按命名规范。",
                    "notes": f"clicks={clicks}; purchases={purchases}; sales={sales}",
                    "actionable": "是" if clicks or purchases or sales else "否",
                })

    gaps = dedupe_gaps(gaps)
    matched_headers = [
        "source", "source_file", "source_record_id", "raw_name", "raw_email",
        "match_via", "confidence", "object_type", "kol_record_id", "kol_name",
        "sales", "commission",
    ]
    gap_headers = [
        "gap_id", "gap_type", "source", "source_file", "source_record_id",
        "raw_name", "raw_email", "raw_campaign_or_link", "candidate",
        "recommended_action", "notes", "actionable",
    ]
    matched_file = out / "kol_roi_p1_matched_20260716.csv"
    gaps_file = out / "kol_roi_p1_gaps_20260716.csv"
    write_csv(matched_file, matched, matched_headers)
    write_csv(gaps_file, gaps, gap_headers)

    wrote = 0
    if ns.write_gaps:
        seen = fetch_existing_gap_ids()
        for row in gaps:
            if row.get("actionable") != "是":
                continue
            if row["gap_id"] in seen:
                continue
            create_gap_record(row)
            seen.add(row["gap_id"])
            wrote += 1
            if wrote >= ns.max_write:
                break

    print(json.dumps({
        "ok": True,
        "contacts": len(contacts),
        "matched": len(matched),
        "gaps": len(gaps),
        "wrote_gaps": wrote,
        "matched_file": str(matched_file),
        "gaps_file": str(gaps_file),
        "inputs": {
            "referrals": referrals_path.name if referrals_path else "",
            "payments": payments_path.name if payments_path else "",
            "affiliates": affiliates_path.name if affiliates_path else "",
            "amazon": amazon_path.name if amazon_path else "",
        },
    }, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
