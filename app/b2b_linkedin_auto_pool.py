"""B2B LinkedIn/Snov automatic lead-pool intake.

The job is intentionally deterministic: take a maintained seed list, enrich
contacts with Snov when possible, dedupe against CRM and the LinkedIn pool,
score with the same local rules as the original PowerShell scripts, then write
only qualified new leads into the LinkedIn lead pool.
"""
import json
import os
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import quote

import httpx

from . import config, feishu, snov

BJ = timezone(timedelta(hours=8))

B2B_APP_TOKEN = os.environ.get("B2B_CUSTOMER_APP_TOKEN", "E1kkbx1tVaJvQGsKf94cJG88nzb")
B2B_CRM_TABLE = os.environ.get("B2B_CUSTOMER_TABLE", "tbl2OoqVb7Uf1pWd")
B2B_LINKEDIN_TABLE = os.environ.get("B2B_LINKEDIN_TABLE", "tblN8XszEatuTJgP")
B2B_LINKEDIN_CANDIDATE_TABLE = os.environ.get("B2B_LINKEDIN_CANDIDATE_TABLE", "tblcfwhNPkgu0TZE")

COMPANY_TYPE_OPTIONS = {"贸易商", "分销商", "品牌商", "批发商", "混合型", "游戏IP", "电商卖家", "电商平台", "行业协会", "零售商", "待判断"}
CHANNEL_OPTIONS = {"线下连锁", "独立店", "本地电商", "海外众筹", "商超", "EBAY", "虾皮", "Amazon", "分销"}
CANDIDATE_SOURCE_OPTIONS = {"系统种子", "搜索补给", "人工导入", "现有客户相似", "展会补充"}
CANDIDATE_PENDING_STATUSES = {"", "待入池"}
CANDIDATE_FIELD_NAMES = [
    "公司名称", "公司官网", "域名", "国家/地区", "公司类型", "主力渠道", "主营类目",
    "候选状态", "优先级分", "来源", "补给批次", "入池批次", "LinkedIn线索记录ID",
    "最近补给时间", "最近入池尝试时间", "查询次数", "Snov查询状态", "Snov原始摘要",
    "去重Key", "备注",
]

KNOWN_LINKEDIN_COMPANY_PAGES = {
    "alsogroup": "https://www.linkedin.com/company/alsogroup",
    "alza": "https://www.linkedin.com/company/alza-cz",
    "alzacz": "https://www.linkedin.com/company/alza-cz",
    "centresoft": "https://www.linkedin.com/company/centresoft-group-ltd",
    "elgiganten": "https://www.linkedin.com/company/elgiganten",
    "jbhifi": "https://www.linkedin.com/company/jb-hifi",
    "mightyape": "https://www.linkedin.com/company/mightyape",
    "panvision": "https://www.linkedin.com/company/pan-vision",
    "pccomponentes": "https://www.linkedin.com/company/pccomponentes",
    "proshop": "https://www.linkedin.com/company/proshop-dk",
    "saturn": "https://www.linkedin.com/company/saturn-deutschland",
    "smythstoys": "https://www.linkedin.com/company/smyths-toys",
    "takealot": "https://www.linkedin.com/company/takealot",
    "virginmegastore": "https://www.linkedin.com/company/virgin-megastore",
    "virginmegastoremiddleeast": "https://www.linkedin.com/company/virgin-megastore",
    "xcite": "https://www.linkedin.com/company/xcitealghanim",
    "xcitealghanim": "https://www.linkedin.com/company/xcitealghanim",
}

DEFAULT_SEEDS = [
    {"company": "Game Retail Limited", "domain": "game.co.uk", "country": "United Kingdom", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games and gaming accessories retail", "notes": "UK game retailer with console and accessory category"},
    {"company": "Smyths Toys", "domain": "smythstoys.com", "country": "Ireland", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "toys, video games, Nintendo Switch and console accessories", "notes": "EU/UK retail chain carrying Nintendo and gaming products"},
    {"company": "MediaMarkt", "domain": "mediamarkt.com", "country": "Germany", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming, console accessories", "notes": "European electronics retailer with gaming category"},
    {"company": "Saturn", "domain": "saturn.de", "country": "Germany", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and console accessories", "notes": "German electronics retailer with console category"},
    {"company": "Coolblue", "domain": "coolblue.nl", "country": "Netherlands", "company_type": "电商平台", "channels": ["本地电商"], "category": "consumer electronics, gaming accessories and controllers", "notes": "Benelux ecommerce retailer"},
    {"company": "Bol.com", "domain": "bol.com", "country": "Netherlands", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, Nintendo Switch, gaming accessories", "notes": "Benelux marketplace with Switch accessories category"},
    {"company": "Fnac Darty", "domain": "fnac.com", "country": "France", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games, Nintendo Switch, console accessories", "notes": "France retail group with gaming category"},
    {"company": "LDLC", "domain": "ldlc.com", "country": "France", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computer, gaming, console and PC accessories", "notes": "French ecommerce retailer with gaming hardware"},
    {"company": "Cdiscount", "domain": "cdiscount.com", "country": "France", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, video games, console accessories", "notes": "French marketplace with gaming products"},
    {"company": "PCComponentes", "domain": "pccomponentes.com", "country": "Spain", "company_type": "电商卖家", "channels": ["本地电商"], "category": "gaming, Nintendo Switch, controllers, accessories", "notes": "Spanish ecommerce retailer with gaming category"},
    {"company": "Worten", "domain": "worten.pt", "country": "Portugal", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and console accessories", "notes": "Portugal electronics retailer"},
    {"company": "Caseking", "domain": "caseking.de", "country": "Germany", "company_type": "电商卖家", "channels": ["本地电商"], "category": "gaming hardware, controllers and accessories", "notes": "German gaming hardware ecommerce specialist"},
    {"company": "Alternate", "domain": "alternate.de", "country": "Germany", "company_type": "电商卖家", "channels": ["本地电商"], "category": "consumer electronics, gaming hardware and accessories", "notes": "German ecommerce retailer with gaming category"},
    {"company": "Proshop", "domain": "proshop.dk", "country": "Denmark", "company_type": "电商卖家", "channels": ["本地电商"], "category": "consumer electronics, gaming and Nintendo accessories", "notes": "Nordic ecommerce retailer"},
    {"company": "Elgiganten", "domain": "elgiganten.dk", "country": "Denmark", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, gaming, consoles and accessories", "notes": "Nordic electronics retailer"},
    {"company": "Webhallen", "domain": "webhallen.com", "country": "Sweden", "company_type": "电商卖家", "channels": ["本地电商"], "category": "gaming, consoles and accessories", "notes": "Swedish game and electronics ecommerce"},
    {"company": "NetOnNet", "domain": "netonnet.se", "country": "Sweden", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming products and accessories", "notes": "Nordic electronics retailer"},
    {"company": "Alza", "domain": "alza.cz", "country": "Czech Republic", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, gaming, Nintendo Switch accessories", "notes": "CEE ecommerce retailer with gaming category"},
    {"company": "Digitec Galaxus", "domain": "digitec.ch", "country": "Switzerland", "company_type": "电商平台", "channels": ["本地电商"], "category": "consumer electronics, gaming and console accessories", "notes": "Swiss ecommerce retailer"},
    {"company": "Brack", "domain": "brack.ch", "country": "Switzerland", "company_type": "电商卖家", "channels": ["本地电商"], "category": "consumer electronics, gaming, accessories", "notes": "Swiss ecommerce retailer"},
    {"company": "JB Hi-Fi", "domain": "jbhifi.com.au", "country": "Australia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, Nintendo Switch, gaming accessories", "notes": "Australia electronics retailer with gaming category"},
    {"company": "Harvey Norman", "domain": "harveynorman.com.au", "country": "Australia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics and gaming accessories", "notes": "Australia retail chain"},
    {"company": "Takealot", "domain": "takealot.com", "country": "South Africa", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, gaming accessories, Nintendo Switch", "notes": "South Africa ecommerce marketplace"},
    {"company": "Virgin Megastore Middle East", "domain": "virginmegastore.me", "country": "United Arab Emirates", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "gaming, consoles, Nintendo Switch accessories", "notes": "Middle East retailer with gaming category"},
    {"company": "Sharaf DG", "domain": "sharafdg.com", "country": "United Arab Emirates", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming accessories", "notes": "UAE electronics retailer"},
    {"company": "Jarir Bookstore", "domain": "jarir.com", "country": "Saudi Arabia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, gaming and console accessories", "notes": "Saudi retail chain with gaming products"},
    {"company": "Yodobashi Camera", "domain": "yodobashi.com", "country": "Japan", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, video games, Nintendo Switch accessories", "notes": "Japan electronics retailer"},
    {"company": "Bic Camera", "domain": "biccamera.com", "country": "Japan", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, gaming and Nintendo accessories", "notes": "Japan electronics retailer"},
    {"company": "Joshin", "domain": "joshinweb.jp", "country": "Japan", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, games, console accessories", "notes": "Japan retailer with video game category"},
    {"company": "Maxsoft", "domain": "maxsoftonline.com", "country": "Singapore", "company_type": "分销商", "channels": ["分销", "本地电商"], "category": "Nintendo and video game distribution", "notes": "Singapore game distributor adjacency"},
]

EXTRA_DEFAULT_SEEDS = [
    {"company": "GameStop", "domain": "gamestop.com", "country": "United States", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games, consoles, controllers and gaming accessories", "notes": "US game specialty retailer"},
    {"company": "Best Buy", "domain": "bestbuy.com", "country": "United States", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, console games and accessories", "notes": "US electronics retailer with gaming category"},
    {"company": "Micro Center", "domain": "microcenter.com", "country": "United States", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "gaming, computers, console and PC accessories", "notes": "US computer and gaming retailer"},
    {"company": "Newegg", "domain": "newegg.com", "country": "United States", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, gaming hardware and accessories", "notes": "US ecommerce marketplace with gaming hardware category"},
    {"company": "B&H Photo Video", "domain": "bhphotovideo.com", "country": "United States", "company_type": "电商卖家", "channels": ["本地电商"], "category": "consumer electronics and gaming accessories", "notes": "US electronics ecommerce retailer"},
    {"company": "Adorama", "domain": "adorama.com", "country": "United States", "company_type": "电商卖家", "channels": ["本地电商"], "category": "consumer electronics and gaming accessories", "notes": "US electronics retailer"},
    {"company": "Walmart", "domain": "walmart.com", "country": "United States", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "mass retail, video games, console accessories", "notes": "US mass retailer with gaming category"},
    {"company": "Target", "domain": "target.com", "country": "United States", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "mass retail, video games and accessories", "notes": "US mass retailer with gaming category"},
    {"company": "GameStop Canada", "domain": "gamestop.ca", "country": "Canada", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games and gaming accessories", "notes": "Canada game specialty retailer"},
    {"company": "Best Buy Canada", "domain": "bestbuy.ca", "country": "Canada", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and console accessories", "notes": "Canada electronics retailer"},
    {"company": "Canada Computers", "domain": "canadacomputers.com", "country": "Canada", "company_type": "电商卖家", "channels": ["本地电商", "线下连锁"], "category": "computers, gaming hardware and accessories", "notes": "Canada computer and gaming retailer"},
    {"company": "London Drugs", "domain": "londondrugs.com", "country": "Canada", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, video games and accessories", "notes": "Western Canada retailer with electronics category"},
    {"company": "Currys", "domain": "currys.co.uk", "country": "United Kingdom", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and console accessories", "notes": "UK electronics retailer"},
    {"company": "Argos", "domain": "argos.co.uk", "country": "United Kingdom", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "general retail, video games and accessories", "notes": "UK general retailer with gaming category"},
    {"company": "Very", "domain": "very.co.uk", "country": "United Kingdom", "company_type": "电商卖家", "channels": ["本地电商"], "category": "ecommerce, gaming consoles and accessories", "notes": "UK ecommerce retailer"},
    {"company": "Scan Computers", "domain": "scan.co.uk", "country": "United Kingdom", "company_type": "电商卖家", "channels": ["本地电商"], "category": "gaming hardware, PC and console accessories", "notes": "UK computer and gaming retailer"},
    {"company": "Ebuyer", "domain": "ebuyer.com", "country": "United Kingdom", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computers, gaming and accessories", "notes": "UK ecommerce retailer"},
    {"company": "Box", "domain": "box.co.uk", "country": "United Kingdom", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computing, gaming and accessories", "notes": "UK electronics ecommerce retailer"},
    {"company": "ShopTo", "domain": "shopto.net", "country": "United Kingdom", "company_type": "电商卖家", "channels": ["本地电商"], "category": "video games, consoles and accessories", "notes": "UK gaming specialist ecommerce"},
    {"company": "The Game Collection", "domain": "thegamecollection.net", "country": "United Kingdom", "company_type": "电商卖家", "channels": ["本地电商"], "category": "video games and console accessories", "notes": "UK game ecommerce retailer"},
    {"company": "Micromania Zing", "domain": "micromania.fr", "country": "France", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games, consoles and accessories", "notes": "France game specialty retailer"},
    {"company": "Boulanger", "domain": "boulanger.com", "country": "France", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and console accessories", "notes": "France electronics retailer"},
    {"company": "Materiel.net", "domain": "materiel.net", "country": "France", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computer hardware, gaming and accessories", "notes": "France computer and gaming ecommerce"},
    {"company": "Rue du Commerce", "domain": "rueducommerce.fr", "country": "France", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, electronics and gaming accessories", "notes": "France ecommerce marketplace"},
    {"company": "Carrefour France", "domain": "carrefour.fr", "country": "France", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "mass retail, electronics, games and accessories", "notes": "France mass retail chain"},
    {"company": "Otto", "domain": "otto.de", "country": "Germany", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, consumer electronics and gaming accessories", "notes": "Germany ecommerce marketplace"},
    {"company": "Conrad Electronic", "domain": "conrad.de", "country": "Germany", "company_type": "电商卖家", "channels": ["本地电商"], "category": "consumer electronics and accessories", "notes": "Germany electronics retailer"},
    {"company": "Notebooksbilliger", "domain": "notebooksbilliger.de", "country": "Germany", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computers, gaming hardware and accessories", "notes": "Germany computer ecommerce retailer"},
    {"company": "Expert", "domain": "expert.de", "country": "Germany", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics and gaming accessories", "notes": "Germany electronics retailer network"},
    {"company": "Mindfactory", "domain": "mindfactory.de", "country": "Germany", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computer hardware, gaming and accessories", "notes": "Germany computer hardware ecommerce"},
    {"company": "El Corte Ingles", "domain": "elcorteingles.es", "country": "Spain", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "department store, video games and accessories", "notes": "Spain department store with gaming category"},
    {"company": "GAME Spain", "domain": "game.es", "country": "Spain", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games and gaming accessories", "notes": "Spain game specialty retailer"},
    {"company": "Carrefour Spain", "domain": "carrefour.es", "country": "Spain", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "mass retail, games and accessories", "notes": "Spain mass retailer"},
    {"company": "Komplett", "domain": "komplett.no", "country": "Norway", "company_type": "电商卖家", "channels": ["本地电商"], "category": "consumer electronics, gaming and accessories", "notes": "Nordic electronics ecommerce"},
    {"company": "Elkjop", "domain": "elkjop.no", "country": "Norway", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, gaming and console accessories", "notes": "Norway electronics retailer"},
    {"company": "Power Norway", "domain": "power.no", "country": "Norway", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics and gaming accessories", "notes": "Nordic electronics retailer"},
    {"company": "Gigantti", "domain": "gigantti.fi", "country": "Finland", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and console accessories", "notes": "Finland electronics retailer"},
    {"company": "Verkkokauppa", "domain": "verkkokauppa.com", "country": "Finland", "company_type": "电商卖家", "channels": ["本地电商"], "category": "consumer electronics, gaming and accessories", "notes": "Finland ecommerce retailer"},
    {"company": "Power Finland", "domain": "power.fi", "country": "Finland", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics and gaming accessories", "notes": "Finland electronics retailer"},
    {"company": "Inet", "domain": "inet.se", "country": "Sweden", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computers, gaming hardware and accessories", "notes": "Sweden computer and gaming retailer"},
    {"company": "Coolshop", "domain": "coolshop.dk", "country": "Denmark", "company_type": "电商卖家", "channels": ["本地电商"], "category": "games, toys, electronics and accessories", "notes": "Nordic ecommerce retailer"},
    {"company": "Megekko", "domain": "megekko.nl", "country": "Netherlands", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computer hardware, gaming and accessories", "notes": "Netherlands computer ecommerce retailer"},
    {"company": "Bax Shop", "domain": "bax-shop.nl", "country": "Netherlands", "company_type": "电商卖家", "channels": ["本地电商"], "category": "electronics, audio and gaming adjacent accessories", "notes": "Benelux ecommerce retailer"},
    {"company": "MediaMarkt Spain", "domain": "mediamarkt.es", "country": "Spain", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and console accessories", "notes": "Spain electronics retailer"},
    {"company": "MediaMarkt Netherlands", "domain": "mediamarkt.nl", "country": "Netherlands", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and console accessories", "notes": "Netherlands electronics retailer"},
    {"company": "MediaMarkt Switzerland", "domain": "mediamarkt.ch", "country": "Switzerland", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and console accessories", "notes": "Switzerland electronics retailer"},
    {"company": "Digitec", "domain": "digitec.ch", "country": "Switzerland", "company_type": "电商平台", "channels": ["本地电商"], "category": "consumer electronics, gaming and console accessories", "notes": "Swiss ecommerce retailer"},
    {"company": "PB Tech", "domain": "pbtech.co.nz", "country": "New Zealand", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "computers, gaming and accessories", "notes": "New Zealand electronics and computer retailer"},
    {"company": "Mighty Ape", "domain": "mightyape.co.nz", "country": "New Zealand", "company_type": "电商卖家", "channels": ["本地电商"], "category": "games, toys and console accessories", "notes": "New Zealand ecommerce retailer"},
    {"company": "Noel Leeming", "domain": "noelleeming.co.nz", "country": "New Zealand", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics and gaming accessories", "notes": "New Zealand electronics retailer"},
    {"company": "The Warehouse", "domain": "thewarehouse.co.nz", "country": "New Zealand", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "mass retail, games and electronics", "notes": "New Zealand mass retailer"},
    {"company": "EB Games Australia", "domain": "ebgames.com.au", "country": "Australia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games and gaming accessories", "notes": "Australia game specialty retailer"},
    {"company": "Kogan", "domain": "kogan.com", "country": "Australia", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, electronics and gaming accessories", "notes": "Australia ecommerce marketplace"},
    {"company": "Catch", "domain": "catch.com.au", "country": "Australia", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, consumer electronics and accessories", "notes": "Australia ecommerce marketplace"},
    {"company": "Officeworks", "domain": "officeworks.com.au", "country": "Australia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics and computer accessories", "notes": "Australia office and electronics retailer"},
    {"company": "Big W", "domain": "bigw.com.au", "country": "Australia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "mass retail, video games and accessories", "notes": "Australia mass retailer"},
    {"company": "Noon", "domain": "noon.com", "country": "United Arab Emirates", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, electronics and gaming accessories", "notes": "Middle East ecommerce marketplace"},
    {"company": "Extra Stores", "domain": "extra.com", "country": "Saudi Arabia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "electronics, gaming and console accessories", "notes": "Saudi electronics retailer"},
    {"company": "Carrefour UAE", "domain": "carrefouruae.com", "country": "United Arab Emirates", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "mass retail, electronics and games", "notes": "UAE mass retailer"},
    {"company": "Lulu Hypermarket", "domain": "luluhypermarket.com", "country": "United Arab Emirates", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "mass retail, electronics and accessories", "notes": "Middle East hypermarket chain"},
    {"company": "Falabella", "domain": "falabella.com", "country": "Chile", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "department store, electronics and gaming accessories", "notes": "Latin America retailer and marketplace"},
    {"company": "Ripley", "domain": "ripley.cl", "country": "Chile", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "department store, electronics and games", "notes": "Chile retail chain"},
    {"company": "Paris", "domain": "paris.cl", "country": "Chile", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "department store, electronics and games", "notes": "Chile retail chain"},
    {"company": "PC Factory", "domain": "pcfactory.cl", "country": "Chile", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computers, gaming and accessories", "notes": "Chile computer and electronics retailer"},
    {"company": "SP Digital", "domain": "spdigital.cl", "country": "Chile", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computers, gaming and accessories", "notes": "Chile electronics ecommerce retailer"},
    {"company": "KaBuM", "domain": "kabum.com.br", "country": "Brazil", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computer hardware, gaming and accessories", "notes": "Brazil gaming and electronics ecommerce"},
    {"company": "Magazine Luiza", "domain": "magazineluiza.com.br", "country": "Brazil", "company_type": "电商平台", "channels": ["本地电商", "线下连锁"], "category": "marketplace, electronics and gaming accessories", "notes": "Brazil retailer and marketplace"},
    {"company": "Americanas", "domain": "americanas.com.br", "country": "Brazil", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, electronics, games and accessories", "notes": "Brazil ecommerce marketplace"},
    {"company": "Casas Bahia", "domain": "casasbahia.com.br", "country": "Brazil", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "retail, electronics and games", "notes": "Brazil retail chain"},
    {"company": "Liverpool", "domain": "liverpool.com.mx", "country": "Mexico", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "department store, electronics and games", "notes": "Mexico department store"},
    {"company": "Elektra", "domain": "elektra.mx", "country": "Mexico", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "retail, electronics and accessories", "notes": "Mexico electronics retailer"},
    {"company": "Sears Mexico", "domain": "sears.com.mx", "country": "Mexico", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "department store, electronics and games", "notes": "Mexico department store"},
    {"company": "Cyberpuerta", "domain": "cyberpuerta.mx", "country": "Mexico", "company_type": "电商卖家", "channels": ["本地电商"], "category": "computers, gaming hardware and accessories", "notes": "Mexico ecommerce retailer"},
    {"company": "Gameplanet", "domain": "gameplanet.com", "country": "Mexico", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games, consoles and accessories", "notes": "Mexico game specialty retailer"},
    {"company": "DataBlitz", "domain": "datablitz.com.ph", "country": "Philippines", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games, consoles and accessories", "notes": "Philippines game specialty retailer"},
    {"company": "GameXtreme", "domain": "gamextreme.ph", "country": "Philippines", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "video games and gaming accessories", "notes": "Philippines gaming retailer"},
    {"company": "Challenger", "domain": "challenger.sg", "country": "Singapore", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and accessories", "notes": "Singapore electronics retailer"},
    {"company": "Courts Singapore", "domain": "courts.com.sg", "country": "Singapore", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and accessories", "notes": "Singapore electronics retailer"},
    {"company": "Harvey Norman Singapore", "domain": "harveynorman.com.sg", "country": "Singapore", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics and gaming accessories", "notes": "Singapore electronics retailer"},
    {"company": "Senheng", "domain": "senheng.com.my", "country": "Malaysia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics and gaming accessories", "notes": "Malaysia electronics retailer"},
    {"company": "Harvey Norman Malaysia", "domain": "harveynorman.com.my", "country": "Malaysia", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics and gaming accessories", "notes": "Malaysia electronics retailer"},
    {"company": "Power Buy", "domain": "powerbuy.co.th", "country": "Thailand", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics, gaming and accessories", "notes": "Thailand electronics retailer"},
    {"company": "JIB", "domain": "jib.co.th", "country": "Thailand", "company_type": "电商卖家", "channels": ["本地电商", "线下连锁"], "category": "computers, gaming and accessories", "notes": "Thailand computer retailer"},
    {"company": "Advice", "domain": "advice.co.th", "country": "Thailand", "company_type": "电商卖家", "channels": ["本地电商", "线下连锁"], "category": "computers, gaming and accessories", "notes": "Thailand computer retailer"},
    {"company": "PChome", "domain": "pchome.com.tw", "country": "Taiwan", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, electronics and gaming accessories", "notes": "Taiwan ecommerce marketplace"},
    {"company": "Momo", "domain": "momo.com.tw", "country": "Taiwan", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, electronics and gaming accessories", "notes": "Taiwan ecommerce marketplace"},
    {"company": "Tokopedia", "domain": "tokopedia.com", "country": "Indonesia", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, gaming and electronics accessories", "notes": "Indonesia ecommerce marketplace"},
    {"company": "Blibli", "domain": "blibli.com", "country": "Indonesia", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, electronics and gaming accessories", "notes": "Indonesia ecommerce marketplace"},
    {"company": "Tiki", "domain": "tiki.vn", "country": "Vietnam", "company_type": "电商平台", "channels": ["本地电商"], "category": "marketplace, electronics and gaming accessories", "notes": "Vietnam ecommerce marketplace"},
    {"company": "CellphoneS", "domain": "cellphones.com.vn", "country": "Vietnam", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics and accessories", "notes": "Vietnam electronics retailer"},
    {"company": "Dien May Xanh", "domain": "dienmayxanh.com", "country": "Vietnam", "company_type": "零售商", "channels": ["本地电商", "线下连锁"], "category": "consumer electronics and accessories", "notes": "Vietnam electronics retailer"},
]

ROLE_TERMS = [
    "purchase", "purchasing", "buyer", "procurement", "sourcing", "achats", "acheteur",
    "compras", "zakup", "category", "product manager", "chef de produit", "product owner",
    "merchand", "business development", "partnership", "publishing", "export",
    "international sales", "bd", "ceo", "founder", "owner", "managing director",
    "general manager", "president", "director",
]

_SNOV_TOKEN = {"value": "", "ts": 0.0}
_LAST_RUN: dict = {}


def get_last_run() -> dict:
    return dict(_LAST_RUN)


def _now_bj() -> datetime:
    return datetime.now(BJ)


def _text(value) -> str:
    return str(feishu.ext(value) or "").strip()


def _url(value) -> str:
    return str(feishu.ext_url(value) or "").strip()


def _normalize_url(value: str) -> str:
    value = (value or "").strip()
    if value and not re.match(r"^https?://", value, flags=re.I):
        value = "https://" + value
    return value


def _domain_of(value: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        return ""
    m = re.search(r"@([a-z0-9.-]+\.[a-z]{2,})", raw)
    if m:
        return re.sub(r"^www\.", "", m.group(1))
    raw = _normalize_url(raw)
    m = re.match(r"^https?://([^/]+)", raw)
    if not m:
        return ""
    return re.sub(r"^www\.", "", m.group(1).split(":")[0])


def _text_key(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (value or "").lower())


def _linkedin_company_key(value: str) -> str:
    value = _domain_of(value) or (value or "")
    value = re.sub(r"\.(com|co|net|org|io|dk|de|fr|es|pt|nl|cz|co\.uk|com\.au|co\.nz)$", "", value.lower())
    return _text_key(value)


def _linkedin_company_overrides() -> dict[str, str]:
    raw = os.environ.get("B2B_LINKEDIN_COMPANY_URLS_JSON", "").strip()
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except Exception as exc:
        print(f"[b2b_linkedin_auto_pool] bad B2B_LINKEDIN_COMPANY_URLS_JSON: {exc}")
        return {}
    if not isinstance(parsed, dict):
        return {}
    out = {}
    for key, value in parsed.items():
        url = _normalize_url(str(value or ""))
        if "linkedin.com/company/" not in url.lower():
            continue
        out[_linkedin_company_key(str(key))] = url
    return out


def _resolve_linkedin_company(seed: dict, *, domain: str, company: str) -> dict:
    explicit = _normalize_url(str(seed.get("linkedin_company") or ""))
    if explicit:
        return {
            "url": explicit,
            "status": "已确认",
            "source": "seed",
            "note": "LinkedIn公司页来自 seed 显式字段",
        }

    keys = [
        _linkedin_company_key(domain),
        _linkedin_company_key(company),
        _text_key(company),
    ]
    overrides = _linkedin_company_overrides()
    for key in keys:
        if key and key in overrides:
            return {
                "url": overrides[key],
                "status": "已确认",
                "source": "env",
                "note": "LinkedIn公司页来自 env 高置信映射",
            }
    for key in keys:
        if key and key in KNOWN_LINKEDIN_COMPANY_PAGES:
            return {
                "url": KNOWN_LINKEDIN_COMPANY_PAGES[key],
                "status": "已确认",
                "source": "known_map",
                "note": "LinkedIn公司页来自已核验高置信映射",
            }

    return {
        "url": "",
        "status": "待人工确认",
        "source": "missing",
        "note": "LinkedIn公司页待人工确认：未匹配到高置信企业主页，系统未自动写入",
    }


def _append_note(notes: str, extra: str) -> str:
    notes = (notes or "").strip()
    extra = (extra or "").strip()
    if not extra or extra in notes:
        return notes
    return f"{notes}；{extra}" if notes else extra


def _first_name(name: str) -> str:
    parts = [p for p in (name or "").strip().split() if p]
    return parts[0] if parts else "there"


def _has_any(text: str, terms: list[str]) -> bool:
    text = (text or "").lower()
    return any(term.lower() in text for term in terms)


def _score_lead(lead: dict) -> dict:
    score = 0
    reasons = []
    role = f"{lead.get('title', '')} {lead.get('contact', '')}".lower()
    context = " ".join([
        str(lead.get("company_type") or ""),
        " ".join(lead.get("channels") or []),
        str(lead.get("category") or ""),
        str(lead.get("competitors") or ""),
        str(lead.get("notes") or ""),
        str(lead.get("company") or ""),
    ]).lower()
    country = (lead.get("country") or "").upper()

    if _has_any(role, ["owner", "founder", "ceo", "president", "director", "general manager"]):
        score += 20
        reasons.append("联系人是老板或高层")
    elif _has_any(role, ["purchasing", "procurement", "buyer", "category", "sourcing", "business development", "product manager", "sales manager"]):
        score += 18
        reasons.append("联系人接近采购、品类或BD角色")
    elif lead.get("title"):
        score += 8
        reasons.append("联系人职位已知")

    if _has_any(context, ["distributor", "wholesale", "retail", "reseller", "importer", "trading", "分销", "批发", "零售", "贸易"]):
        score += 22
        reasons.append("公司类型接近分销、批发或零售")
    elif _has_any(context, ["ecommerce", "marketplace", "amazon", "shopify", "电商"]):
        score += 14
        reasons.append("公司有电商渠道信号")

    if _has_any(context, ["gaming", "game", "console", "nintendo", "switch", "playstation", "xbox", "controller", "accessories", "游戏", "手柄", "配件"]):
        score += 22
        reasons.append("主营或描述含游戏、主机或配件信号")

    if _has_any(context, ["8bitdo", "gamesir", "hori", "powera", "nacon", "turtle beach", "skull", "nyxi", "dobe", "iine", "gulikit"]):
        score += 12
        reasons.append("出现游戏配件竞品或相邻品牌")

    priority = {"US", "USA", "UNITED STATES", "UK", "UNITED KINGDOM", "DE", "GERMANY", "FR", "FRANCE", "ES", "SPAIN", "IT", "ITALY", "NL", "NETHERLANDS", "PL", "POLAND", "AE", "UAE", "SA", "SAUDI", "KW", "KUWAIT", "AU", "AUSTRALIA", "NZ", "JAPAN", "JP", "KOREA", "KR", "SG", "SINGAPORE"}
    if country in priority or any(x in country for x in priority):
        score += 10
        reasons.append("国家属于优先开发市场")
    elif lead.get("country"):
        score += 5
        reasons.append("国家信息完整")

    complete = 0
    if lead.get("website"):
        complete += 4
    if lead.get("linkedin_company") or lead.get("linkedin_profile"):
        complete += 4
    if lead.get("contact"):
        complete += 3
    if lead.get("title"):
        complete += 3
    if lead.get("country"):
        complete += 2
    score += min(complete, 10)
    if complete >= 8:
        reasons.append("线索关键字段较完整")

    score = min(score, 100)
    if score >= 75:
        return {"score": score, "icp": "是", "grade": "A-优先开发", "reasons": reasons}
    if score >= 55:
        return {"score": score, "icp": "待判断", "grade": "B-可开发", "reasons": reasons}
    return {"score": score, "icp": "否", "grade": "C-低优先", "reasons": reasons}


def _copy_for_lead(lead: dict, score: dict) -> dict:
    first = _first_name(lead.get("contact") or "")
    company = lead.get("company") or "your company"
    market = lead.get("country") or "your market"
    category = lead.get("category") or "gaming accessories"
    reason = "；".join(score.get("reasons") or []) or "字段不足，需要人工复核"

    connect = f"Hi {first}, I noticed {company} works around {category}. We make Nintendo Switch gaming accessories for distributors and retailers. Open to connect?"
    if len(connect) > 280:
        connect = f"Hi {first}, I saw {company} in gaming accessories/distribution. We make Switch accessories for retailers and distributors. Open to connect?"
    message = f"Thanks for connecting, {first}. Quick question: are you currently sourcing Switch or Switch 2 accessories for {market}? If yes, I can send a short line sheet and distributor pricing."
    email = (
        f"Subject: Switch accessories for {company}\r\n\r\n"
        f"Hi {first},\r\n\r\n"
        f"I found {company} while researching gaming accessories distributors and retailers in {market}.\r\n\r\n"
        "We make Nintendo Switch accessories such as controllers, docks, carrying cases, and related add-ons under FUNLAB and POWKONG. "
        "If this category fits your current sourcing plan, I can send a short line sheet with MOQ, pricing logic, and available samples.\r\n\r\n"
        "Would it be useful to compare a few SKUs for your channel?\r\n\r\n"
        "Best regards,\r\nFrankie"
    )
    cn_reason = f"{company} 符合现有 B2B 相似客户开发逻辑：{reason}。建议先 LinkedIn 连接，接受后用低压问题确认是否采购 Switch/游戏配件；未接受则转 email 或官网表单。"
    return {"connect": connect, "message": message, "email": email, "reason": cn_reason}


def _url_cell(url: str) -> dict | None:
    url = _normalize_url(url)
    return {"link": url, "text": url} if url else None


def _clean_channels(values) -> list[str]:
    if isinstance(values, str):
        values = re.split(r"[,;；、/]", values)
    out = []
    for value in values or []:
        value = str(value).strip()
        if value in CHANNEL_OPTIONS and value not in out:
            out.append(value)
    return out


def _field_list(value) -> list[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [x.strip() for x in re.split(r"[,;；、/]", value) if x.strip()]
    if isinstance(value, list):
        out = []
        for item in value:
            if isinstance(item, dict):
                text = str(item.get("text") or item.get("name") or item.get("link") or "").strip()
            else:
                text = str(item or "").strip()
            if text and text not in out:
                out.append(text)
        return out
    return [str(value).strip()] if str(value).strip() else []


def _field_int(value, default: int = 0) -> int:
    try:
        return int(float(_text(value) or value or default))
    except Exception:
        return default


def _timestamp_ms() -> int:
    return int(_now_bj().timestamp() * 1000)


def _seed_key(seed: dict) -> str:
    return _domain_of(str(seed.get("domain") or seed.get("website") or "")) or _text_key(str(seed.get("company") or ""))


def _candidate_source(seed: dict) -> str:
    raw = str(seed.get("candidate_source") or seed.get("source") or "").strip()
    if raw in CANDIDATE_SOURCE_OPTIONS:
        return raw
    if "展会" in raw:
        return "展会补充"
    if "相似" in raw:
        return "现有客户相似"
    if "搜索" in raw or "Google" in raw or "Snov" in raw:
        return "搜索补给"
    if "人工" in raw:
        return "人工导入"
    return "系统种子"


def _candidate_key_from_fields(fields: dict) -> str:
    return (
        _domain_of(_text(fields.get("去重Key")))
        or _domain_of(_text(fields.get("域名")))
        or _domain_of(_url(fields.get("公司官网")))
        or _text_key(_text(fields.get("公司名称")))
    )


def _candidate_record_to_seed(rec: dict) -> dict:
    fields = rec.get("fields") or {}
    domain = (
        _domain_of(_text(fields.get("域名")))
        or _domain_of(_text(fields.get("去重Key")))
        or _domain_of(_url(fields.get("公司官网")))
    )
    website = _url(fields.get("公司官网")) or domain
    company_type = _text(fields.get("公司类型")) or "待判断"
    if company_type not in COMPANY_TYPE_OPTIONS:
        company_type = "待判断"
    return {
        "company": _text(fields.get("公司名称")),
        "domain": domain,
        "website": website,
        "country": _text(fields.get("国家/地区")),
        "company_type": company_type,
        "channels": _clean_channels(_field_list(fields.get("主力渠道"))),
        "category": _text(fields.get("主营类目")),
        "source": "LinkedIn-现有客户相似",
        "notes": _text(fields.get("备注")),
        "_candidate_record_id": rec.get("record_id") or rec.get("id") or "",
        "_candidate_query_count": _field_int(fields.get("查询次数"), 0),
        "_priority_score": _field_int(fields.get("优先级分"), 0),
    }


def _candidate_fields_for_seed(seed: dict, lead: dict, score: dict, *, batch: str) -> dict:
    domain = lead.get("domain") or _domain_of(lead.get("website") or "")
    fields = {
        "公司名称": lead.get("company"),
        "域名": domain,
        "国家/地区": lead.get("country"),
        "公司类型": lead.get("company_type") or "待判断",
        "主力渠道": lead.get("channels") or [],
        "主营类目": lead.get("category"),
        "候选状态": "待入池",
        "优先级分": int(score.get("score") or 0),
        "来源": _candidate_source(seed),
        "补给批次": batch,
        "最近补给时间": _timestamp_ms(),
        "查询次数": 0,
        "Snov查询状态": "未查询",
        "去重Key": domain or _text_key(lead.get("company") or ""),
        "备注": lead.get("notes"),
    }
    website = _url_cell(lead.get("website") or domain or "")
    if website:
        fields["公司官网"] = website
    return {k: v for k, v in fields.items() if v not in (None, "", [])}


def _dedupe_seeds(seeds: list[dict]) -> list[dict]:
    out = []
    seen = set()
    for seed in seeds:
        if not isinstance(seed, dict):
            continue
        key = _seed_key(seed)
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(seed)
    return out


def _load_seeds() -> list[dict]:
    raw = os.environ.get("B2B_LINKEDIN_AUTO_SEEDS_JSON", "").strip()
    if raw:
        try:
            parsed = json.loads(raw)
            if isinstance(parsed, list):
                return _dedupe_seeds([x for x in parsed if isinstance(x, dict)])
        except Exception as exc:
            print(f"[b2b_linkedin_auto_pool] bad B2B_LINKEDIN_AUTO_SEEDS_JSON: {exc}")
    return _dedupe_seeds(DEFAULT_SEEDS + EXTRA_DEFAULT_SEEDS)


async def _list_records(table_id: str, *, field_names: list[str]) -> list[dict]:
    items = []
    page_token = ""
    encoded_fields = "&field_names=" + quote(json.dumps(field_names, ensure_ascii=False), safe="")
    while True:
        path = f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records?page_size=500{encoded_fields}"
        if page_token:
            path += "&page_token=" + quote(page_token, safe="")
        resp = await feishu.api("GET", path, which="bitable")
        data = resp.get("data") or {}
        items.extend(data.get("items") or [])
        if not data.get("has_more"):
            break
        page_token = data.get("page_token") or ""
        if not page_token:
            break
    return items


async def _create_table_record(table_id: str, fields: dict) -> str:
    resp = await feishu.api("POST", f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records", {"fields": fields}, which="bitable")
    return (((resp.get("data") or {}).get("record") or {}).get("record_id")) or ""


async def _update_table_record(table_id: str, record_id: str, fields: dict) -> None:
    clean = {k: v for k, v in fields.items() if v not in (None, "", [])}
    if not record_id or not clean:
        return
    await feishu.api("PUT", f"/bitable/v1/apps/{B2B_APP_TOKEN}/tables/{table_id}/records/{record_id}", {"fields": clean}, which="bitable")


async def _list_candidate_records() -> list[dict]:
    return await _list_records(B2B_LINKEDIN_CANDIDATE_TABLE, field_names=CANDIDATE_FIELD_NAMES)


async def _load_pending_candidate_seeds() -> tuple[list[dict], Counter]:
    seeds = []
    status_counts = Counter()
    for rec in await _list_candidate_records():
        fields = rec.get("fields") or {}
        status = _text(fields.get("候选状态"))
        status_counts[status or "空"] += 1
        if status not in CANDIDATE_PENDING_STATUSES:
            continue
        seed = _candidate_record_to_seed(rec)
        if _seed_key(seed):
            seeds.append(seed)
        else:
            status_counts["missing_domain_company"] += 1
    seeds.sort(key=lambda x: (-_field_int(x.get("_priority_score"), 0), x.get("company") or ""))
    return seeds, status_counts


async def _load_existing_keys() -> tuple[set[str], set[str], set[str], set[str]]:
    lead_domains = set()
    lead_company_keys = set()
    crm_domains = set()
    crm_company_keys = set()

    lead_fields = ["公司名称", "公司官网", "去重Key"]
    for rec in await _list_records(B2B_LINKEDIN_TABLE, field_names=lead_fields):
        fields = rec.get("fields") or {}
        domain = _domain_of(_text(fields.get("去重Key"))) or _domain_of(_url(fields.get("公司官网")))
        company_key = _text_key(_text(fields.get("公司名称")))
        if domain:
            lead_domains.add(domain)
        if company_key:
            lead_company_keys.add(company_key)

    crm_fields = ["公司名称", "公司官网", "邮箱", "LinkedIn"]
    for rec in await _list_records(B2B_CRM_TABLE, field_names=crm_fields):
        fields = rec.get("fields") or {}
        for value in [_url(fields.get("公司官网")), _text(fields.get("邮箱"))]:
            domain = _domain_of(value)
            if domain:
                crm_domains.add(domain)
        company_key = _text_key(_text(fields.get("公司名称")))
        if company_key:
            crm_company_keys.add(company_key)

    return lead_domains, lead_company_keys, crm_domains | lead_domains, crm_company_keys | lead_company_keys


async def refill_candidates(*, commit: bool = False, limit: int = 200) -> dict:
    """Top up the candidate-company pool from maintained seed sources.

    This is intentionally separate from Snov enrichment. Refill builds inventory;
    the daily auto-pool job consumes that inventory and writes qualified leads.
    """
    limit = max(1, min(int(limit or 200), 1000))
    batch = "candidate-refill-" + _now_bj().strftime("%Y%m%d-%H%M")
    started_at = _now_bj().strftime("%Y-%m-%d %H:%M:%S")
    seeds = _load_seeds()
    lead_domains, lead_company_keys, all_domains, all_company_keys = await _load_existing_keys()

    candidate_domains = set()
    candidate_company_keys = set()
    candidate_status_counts = Counter()
    for rec in await _list_candidate_records():
        fields = rec.get("fields") or {}
        status = _text(fields.get("候选状态")) or "空"
        candidate_status_counts[status] += 1
        key = _candidate_key_from_fields(fields)
        domain = _domain_of(key)
        if domain:
            candidate_domains.add(domain)
        company_key = _text_key(_text(fields.get("公司名称")))
        if company_key:
            candidate_company_keys.add(company_key)
        if key and not domain:
            candidate_company_keys.add(key)

    skip_reasons = Counter()
    planned = []
    created = []
    seen_this_run = set()

    for seed in seeds:
        if len(planned) >= limit:
            break
        lead = _seed_to_lead(seed)
        domain = lead.get("domain")
        company_key = _text_key(lead.get("company") or "")
        key = domain or company_key
        if not key:
            skip_reasons["missing_domain_company"] += 1
            continue
        if key in seen_this_run:
            skip_reasons["duplicate_seed_this_run"] += 1
            continue
        seen_this_run.add(key)
        if domain and domain in candidate_domains:
            skip_reasons["duplicate_candidate_domain"] += 1
            continue
        if company_key and company_key in candidate_company_keys:
            skip_reasons["duplicate_candidate_company"] += 1
            continue
        if domain and domain in lead_domains:
            skip_reasons["duplicate_lead_pool_domain"] += 1
            continue
        if company_key and company_key in lead_company_keys:
            skip_reasons["duplicate_lead_pool_company"] += 1
            continue
        if domain and domain in all_domains:
            skip_reasons["duplicate_crm_domain"] += 1
            continue
        if company_key and company_key in all_company_keys:
            skip_reasons["duplicate_crm_company"] += 1
            continue

        score = _score_lead(lead)
        fields = _candidate_fields_for_seed(seed, lead, score, batch=batch)
        planned.append({
            "company": lead.get("company"),
            "domain": domain,
            "score": score["score"],
            "source": fields.get("来源"),
            "fields": fields,
        })

    if commit:
        for row in planned:
            record_id = await _create_table_record(B2B_LINKEDIN_CANDIDATE_TABLE, row["fields"])
            created.append({
                "record_id": record_id,
                "company": row["company"],
                "domain": row["domain"],
                "score": row["score"],
            })

    return {
        "commit": commit,
        "started_at_bj": started_at,
        "batch": batch,
        "seed_total": len(seeds),
        "candidate_table": B2B_LINKEDIN_CANDIDATE_TABLE,
        "existing_candidate_status_counts": dict(candidate_status_counts),
        "limit": limit,
        "planned_candidates": len(planned),
        "created_candidates": len(created),
        "created": created,
        "planned_preview": [
            {k: row[k] for k in ["company", "domain", "score", "source"]}
            for row in planned[:30]
        ],
        "skip_reasons": dict(skip_reasons),
    }


async def _snov_token() -> str:
    if _SNOV_TOKEN["value"] and time.time() - _SNOV_TOKEN["ts"] < 3000:
        return _SNOV_TOKEN["value"]
    if not config.SNOV_CLIENT_ID or not config.SNOV_CLIENT_SECRET:
        raise RuntimeError("SNOV_CLIENT_ID/SECRET 未配置")
    async with httpx.AsyncClient(timeout=30.0) as cli:
        r = await cli.post("https://api.snov.io/v1/oauth/access_token", data={
            "grant_type": "client_credentials",
            "client_id": config.SNOV_CLIENT_ID,
            "client_secret": config.SNOV_CLIENT_SECRET,
        })
        r.raise_for_status()
        data = r.json()
    token = data.get("access_token") or ""
    if not token:
        raise RuntimeError("Snov OAuth returned no access_token")
    _SNOV_TOKEN["value"] = token
    _SNOV_TOKEN["ts"] = time.time()
    return token


async def _snov_json(method: str, url: str, token: str, body: dict | None = None) -> dict:
    headers = {"Authorization": f"Bearer {token}"}
    async with httpx.AsyncClient(timeout=40.0) as cli:
        if body is None:
            r = await cli.request(method, url, headers=headers)
        else:
            r = await cli.request(method, url, headers=headers, json=body)
        r.raise_for_status()
        return r.json()


async def _poll_result(start_resp: dict, token: str, *, poll_seconds: int = 3, max_polls: int = 8) -> dict:
    result_url = (
        start_resp.get("result_url")
        or ((start_resp.get("links") or {}).get("result"))
        or ((start_resp.get("data") or {}).get("result_url") if isinstance(start_resp.get("data"), dict) else "")
    )
    if not result_url:
        return start_resp
    import asyncio
    result = start_resp
    for _ in range(max_polls):
        await asyncio.sleep(poll_seconds)
        result = await _snov_json("GET", result_url, token)
        status = str(result.get("status") or ((result.get("data") or {}).get("status") if isinstance(result.get("data"), dict) else "")).lower()
        if status and not re.search(r"progress|pending|processing|queued", status):
            return result
        if not status:
            return result
    return {"status": "timeout", "result_url": result_url}


def _collect_prospects(obj) -> list[dict]:
    if not isinstance(obj, dict):
        return []
    data = obj.get("data")
    if isinstance(data, dict) and isinstance(data.get("prospects"), list):
        return [x for x in data["prospects"] if isinstance(x, dict)]
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(obj.get("prospects"), list):
        return [x for x in obj["prospects"] if isinstance(x, dict)]
    return []


def _role_score(prospect: dict) -> int:
    position = str(prospect.get("position") or prospect.get("job_title") or prospect.get("title") or "").lower()
    if _has_any(position, ["purchase", "purchasing", "buyer", "procurement", "sourcing", "achats", "acheteur", "compras", "zakup"]):
        return 100
    if _has_any(position, ["category", "product manager", "chef de produit", "product owner", "merchand"]):
        return 90
    if _has_any(position, ["business development", "partnership", "publishing", "export", "international sales", "bd"]):
        return 80
    if _has_any(position, ["ceo", "founder", "owner", "managing director", "general manager", "president", "director"]):
        return 70
    if _has_any(position, ["sales", "key account", "account manager", "retail"]):
        return 55
    return 0


async def _snov_prospects(domain: str, *, max_prospects: int) -> tuple[list[dict], str]:
    token = await _snov_token()
    query = "domain=" + quote(domain, safe="") + "&page=1"
    url = "https://api.snov.io/v2/domain-search/prospects/start?" + query
    start = await _snov_json("POST", url, token)
    result = await _poll_result(start, token)
    prospects = _collect_prospects(result)
    prospects = sorted(prospects, key=lambda p: -_role_score(p))
    summary = json.dumps({
        "domain": domain,
        "prospects": len(prospects),
        "status": result.get("status") or ((result.get("data") or {}).get("status") if isinstance(result.get("data"), dict) else ""),
    }, ensure_ascii=False)
    return prospects[:max_prospects], summary[:1200]


def _prospect_to_lead(seed: dict, prospect: dict, *, email: str = "", email_status: str = "") -> dict:
    first = str(prospect.get("first_name") or prospect.get("firstName") or "").strip()
    last = str(prospect.get("last_name") or prospect.get("lastName") or "").strip()
    contact = " ".join(x for x in [first, last] if x).strip() or str(seed.get("contact") or "").strip()
    title = str(prospect.get("position") or prospect.get("job_title") or prospect.get("title") or seed.get("title") or "").strip()
    linkedin = ""
    for key in ["linkedin_url", "linkedin", "source_page", "url"]:
        value = str(prospect.get(key) or "")
        if "linkedin.com" in value:
            linkedin = _normalize_url(value)
            break
    return _seed_to_lead(seed) | {
        "contact": contact,
        "title": title,
        "linkedin_profile": linkedin or str(seed.get("linkedin_profile") or ""),
        "email": email,
        "email_status": email_status,
    }


def _seed_to_lead(seed: dict) -> dict:
    domain = str(seed.get("domain") or _domain_of(seed.get("website") or "")).strip().lower()
    website = _normalize_url(str(seed.get("website") or domain or ""))
    company = str(seed.get("company") or "").strip()
    linkedin_company = _resolve_linkedin_company(seed, domain=domain, company=company)
    company_type = str(seed.get("company_type") or "待判断").strip()
    if company_type not in COMPANY_TYPE_OPTIONS:
        company_type = "待判断"
    notes = str(seed.get("notes") or "").strip()
    if linkedin_company["status"] != "已确认":
        notes = _append_note(notes, linkedin_company["note"])
    return {
        "company": company,
        "contact": str(seed.get("contact") or "").strip(),
        "title": str(seed.get("title") or "").strip(),
        "website": website,
        "domain": domain,
        "linkedin_company": linkedin_company["url"],
        "linkedin_company_status": linkedin_company["status"],
        "linkedin_company_source": linkedin_company["source"],
        "linkedin_company_note": linkedin_company["note"],
        "linkedin_profile": _normalize_url(str(seed.get("linkedin_profile") or "")),
        "country": str(seed.get("country") or "").strip(),
        "company_type": company_type,
        "channels": _clean_channels(seed.get("channels") or []),
        "competitors": str(seed.get("competitors") or "").strip(),
        "category": str(seed.get("category") or "").strip(),
        "owner": str(seed.get("owner") or "").strip(),
        "source": str(seed.get("source") or "LinkedIn-现有客户相似").strip(),
        "notes": notes,
        "email": str(seed.get("email") or "").strip(),
        "email_status": str(seed.get("email_status") or "").strip(),
    }


async def _create_record(fields: dict) -> str:
    return await _create_table_record(B2B_LINKEDIN_TABLE, fields)


def _lead_fields(lead: dict, score: dict, copy: dict, *, batch: str, snov_status: str, snov_source: str, snov_summary: str) -> dict:
    name = lead.get("company") or lead.get("linkedin_profile") or lead.get("domain")
    if lead.get("contact"):
        name = f"{lead.get('company')} - {lead.get('contact')}"
    next_action = "业务员手动核对 LinkedIn profile；合格则手动加人并发送推荐连接语"
    if lead.get("linkedin_company_status") != "已确认":
        next_action = "先人工确认企业LinkedIn公司页，再核对联系人 profile；合格则手动加人并发送推荐连接语"
    fields = {
        "线索名称": name,
        "公司名称": lead.get("company"),
        "线索来源": lead.get("source") or "LinkedIn-现有客户相似",
        "开发状态": "待开发",
        "触达状态": "待触达",
        "联系人姓名": lead.get("contact"),
        "职位": lead.get("title"),
        "国家/地区": lead.get("country"),
        "公司类型": lead.get("company_type") or "待判断",
        "主力渠道": lead.get("channels") or [],
        "代理竞品": lead.get("competitors"),
        "主营类目": lead.get("category"),
        "AI开发评分": int(score["score"]),
        "ICP匹配": score["icp"],
        "AI建议等级": score["grade"],
        "AI开发理由": copy["reason"],
        "推荐连接语": copy["connect"],
        "推荐私信": copy["message"],
        "推荐开发信": copy["email"],
        "跟进人": lead.get("owner"),
        "邮箱": lead.get("email"),
        "邮箱验真状态": lead.get("email_status"),
        "Snov查询状态": snov_status,
        "Snov来源": snov_source,
        "Snov原始摘要": snov_summary,
        "Snov最后查询时间": int(_now_bj().timestamp() * 1000),
        "下一步行动": next_action,
        "CRM匹配状态": "新线索",
        "去重Key": lead.get("domain") or _text_key(lead.get("company") or ""),
        "创建批次": batch,
        "备注": lead.get("notes"),
    }
    for key, value in [("公司官网", _url_cell(lead.get("website") or "")), ("LinkedIn公司页", _url_cell(lead.get("linkedin_company") or "")), ("LinkedIn联系人页", _url_cell(lead.get("linkedin_profile") or ""))]:
        if value:
            fields[key] = value
    return {k: v for k, v in fields.items() if v not in (None, "", [])}


async def run(
    *,
    commit: bool = False,
    domain_limit: int = 10,
    record_limit: int = 10,
    max_prospects_per_domain: int = 3,
    min_score: int = 55,
    allow_company_fallback: bool = True,
) -> dict:
    domain_limit = max(1, min(int(domain_limit or 10), 50))
    record_limit = max(1, min(int(record_limit or 10), 50))
    max_prospects_per_domain = max(1, min(int(max_prospects_per_domain or 3), 5))
    batch = "auto-linkedin-" + _now_bj().strftime("%Y%m%d-%H%M")
    started_at = _now_bj().strftime("%Y-%m-%d %H:%M:%S")

    seed_bank = _load_seeds()
    candidate_seeds, candidate_status_counts = await _load_pending_candidate_seeds()
    if candidate_seeds:
        seeds = candidate_seeds
        candidate_source = "candidate_pool"
    else:
        seeds = seed_bank
        candidate_source = "seed_fallback"
    lead_domains, lead_company_keys, all_domains, all_company_keys = await _load_existing_keys()
    skip_reasons = Counter()
    created = []
    planned = []
    selected_domains = 0
    snov_errors = []

    async def mark_candidate(seed: dict, status: str, extra: dict | None = None):
        record_id = seed.get("_candidate_record_id") or ""
        if not commit or not record_id:
            return
        fields = {
            "候选状态": status,
            "最近入池尝试时间": _timestamp_ms(),
            "入池批次": batch,
        }
        if extra:
            fields.update(extra)
        await _update_table_record(B2B_LINKEDIN_CANDIDATE_TABLE, record_id, fields)

    for seed in seeds:
        if selected_domains >= domain_limit or len(planned) >= record_limit:
            break
        base_lead = _seed_to_lead(seed)
        domain = base_lead.get("domain")
        company_key = _text_key(base_lead.get("company") or "")
        if not domain and not company_key:
            skip_reasons["missing_domain_company"] += 1
            await mark_candidate(seed, "查询失败", {"备注": _append_note(seed.get("notes") or "", "入池跳过：缺公司域名和公司名")})
            continue
        if domain and domain in lead_domains:
            skip_reasons["duplicate_lead_pool_domain"] += 1
            await mark_candidate(seed, "跳过-重复", {"备注": _append_note(seed.get("notes") or "", "入池跳过：LinkedIn线索池域名重复")})
            continue
        if company_key and company_key in lead_company_keys:
            skip_reasons["duplicate_lead_pool_company"] += 1
            await mark_candidate(seed, "跳过-重复", {"备注": _append_note(seed.get("notes") or "", "入池跳过：LinkedIn线索池公司名重复")})
            continue
        if domain and domain in all_domains:
            skip_reasons["duplicate_crm_domain"] += 1
            await mark_candidate(seed, "跳过-重复", {"备注": _append_note(seed.get("notes") or "", "入池跳过：CRM域名重复")})
            continue
        if company_key and company_key in all_company_keys:
            skip_reasons["duplicate_crm_company"] += 1
            await mark_candidate(seed, "跳过-重复", {"备注": _append_note(seed.get("notes") or "", "入池跳过：CRM公司名重复")})
            continue

        selected_domains += 1
        query_count = int(seed.get("_candidate_query_count") or 0) + 1
        prospects = []
        snov_summary = ""
        snov_status = "未查询"
        if domain:
            try:
                prospects, snov_summary = await _snov_prospects(domain, max_prospects=max_prospects_per_domain)
                snov_status = "查询成功" if prospects else "无结果"
            except Exception as exc:
                snov_status = "查询失败"
                snov_summary = f"{type(exc).__name__}: {str(exc)[:300]}"
                snov_errors.append({"domain": domain, "error": snov_summary})

        candidate_leads = []
        for prospect in prospects:
            lead = _prospect_to_lead(seed, prospect)
            if lead.get("contact") and domain:
                try:
                    found = await snov.find_email(lead["contact"], domain)
                    lead["email"] = found.get("email") or ""
                    lead["email_status"] = found.get("status") or ""
                except Exception:
                    lead["email_status"] = "unavailable"
            candidate_leads.append((lead, snov_status, "Domain Search", snov_summary))

        if not candidate_leads and allow_company_fallback:
            candidate_leads.append((base_lead, "无结果", "Company seed fallback", snov_summary or f"{domain or base_lead.get('company')} 无联系人，保留公司级线索待人工核对"))

        seed_planned = 0
        best_score = 0
        for lead, status, source, summary in candidate_leads:
            if len(planned) >= record_limit:
                break
            score = _score_lead(lead)
            best_score = max(best_score, int(score["score"]))
            if score["score"] < min_score or score["icp"] == "否":
                skip_reasons["low_icp"] += 1
                continue
            copy = _copy_for_lead(lead, score)
            fields = _lead_fields(lead, score, copy, batch=batch, snov_status=status, snov_source=source, snov_summary=summary)
            seed_planned += 1
            planned.append({
                "company": lead.get("company"),
                "domain": lead.get("domain"),
                "contact": lead.get("contact"),
                "score": score["score"],
                "grade": score["grade"],
                "linkedin_company": lead.get("linkedin_company"),
                "linkedin_company_status": lead.get("linkedin_company_status"),
                "linkedin_company_source": lead.get("linkedin_company_source"),
                "candidate_record_id": seed.get("_candidate_record_id") or "",
                "candidate_query_count": query_count,
                "snov_status": status,
                "snov_summary": summary,
                "fields": fields,
            })

        if seed.get("_candidate_record_id") and seed_planned == 0:
            if snov_status == "查询失败":
                await mark_candidate(seed, "查询失败", {
                    "查询次数": query_count,
                    "Snov查询状态": snov_status,
                    "Snov原始摘要": snov_summary,
                    "优先级分": best_score,
                })
            else:
                await mark_candidate(seed, "跳过-低评分", {
                    "查询次数": query_count,
                    "Snov查询状态": snov_status,
                    "Snov原始摘要": snov_summary,
                    "优先级分": best_score,
                })

    if commit:
        created_by_candidate: dict[str, list[dict]] = {}
        for row in planned:
            record_id = await _create_record(row["fields"])
            item = {
                "record_id": record_id,
                "company": row["company"],
                "domain": row["domain"],
                "contact": row["contact"],
                "score": row["score"],
                "grade": row["grade"],
            }
            created.append(item)
            if row.get("candidate_record_id"):
                created_by_candidate.setdefault(row["candidate_record_id"], []).append({**item, **row})
        for candidate_record_id, rows in created_by_candidate.items():
            await _update_table_record(B2B_LINKEDIN_CANDIDATE_TABLE, candidate_record_id, {
                "候选状态": "已入池",
                "入池批次": batch,
                "LinkedIn线索记录ID": ",".join([x["record_id"] for x in rows if x.get("record_id")]),
                "最近入池尝试时间": _timestamp_ms(),
                "查询次数": max(int(x.get("candidate_query_count") or 0) for x in rows),
                "Snov查询状态": rows[0].get("snov_status") or "",
                "Snov原始摘要": rows[0].get("snov_summary") or "",
                "优先级分": max(int(x.get("score") or 0) for x in rows),
            })

    result = {
        "commit": commit,
        "started_at_bj": started_at,
        "batch": batch,
        "seed_total": len(seed_bank),
        "candidate_source": candidate_source,
        "candidate_pending_total": len(candidate_seeds),
        "candidate_status_counts": dict(candidate_status_counts),
        "candidate_table": B2B_LINKEDIN_CANDIDATE_TABLE,
        "domain_limit": domain_limit,
        "record_limit": record_limit,
        "selected_domains": selected_domains,
        "planned_records": len(planned),
        "created_records": len(created),
        "created": created,
        "planned_preview": [
            {k: row[k] for k in ["company", "domain", "contact", "score", "grade", "linkedin_company_status"]}
            for row in planned[:20]
        ],
        "linkedin_company_resolved": sum(1 for row in planned if row.get("linkedin_company")),
        "linkedin_company_pending": sum(1 for row in planned if not row.get("linkedin_company")),
        "linkedin_company_pending_preview": [
            {k: row[k] for k in ["company", "domain", "contact", "linkedin_company_status"]}
            for row in planned
            if not row.get("linkedin_company")
        ][:20],
        "skip_reasons": dict(skip_reasons),
        "snov_errors": snov_errors[:10],
    }
    _LAST_RUN.clear()
    _LAST_RUN.update(result)
    return result
