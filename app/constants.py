BASE_TOKEN = "KINabIENjak8fRsB6AHcIDALntc"

TABLES = {
    "keyword_config": "tblgWfvdPgbkq541",
    "competitor_posts": "tblCDbvLtnLzdxEp",
    "marketing_events": "tblpZaWYEWy54Sll",
}

KEYWORD = "nyxi"
BRAND = "NYXI"
PLATFORM = "YouTube"
CONFIG_RECORD_ID = "recvrM7WDZ0ZV9"

POST_SINGLE_SELECT_FIELDS = {
    "平台",
    "相关性",
    "内容类型",
    "合作信号",
    "营销阶段",
    "AI分析状态",
    "人工复核状态",
}

DATETIME_FIELDS = {
    "发布时间",
    "抓取时间",
    "最近成功采集时间",
    "最近采集水位",
}
RELATION_FIELDS = {"关联监控任务", "关联营销事件", "关联KOL"}
URL_FIELDS = {"帖子URL", "缩略图URL", "KOL主页URL", "官方来源链接"}

POST_READ_FIELDS = [
    "唯一键",
    "竞品品牌",
    "平台",
    "帖子ID",
    "帖子标题",
    "帖子内容",
    "发布时间",
    "帖子URL",
    "缩略图URL",
    "附件类型",
    "KOL平台ID",
    "KOL账号名",
    "KOL账号Handle",
    "KOL主页URL",
    "粉丝数快照",
    "帖子数快照",
    "曝光量",
    "点赞数",
    "评论数",
    "抓取时间",
    "采集批次ID",
    "原始数据哈希",
    "采集来源",
    "YouTube命中查询词",
    "YouTube查询时间窗",
    "视频时长秒",
    "字幕可用",
    "视频标签",
    "相关性",
    "内容类型",
    "合作信号",
    "营销阶段",
    "AI分析状态",
    "人工复核状态",
]

CONFIG_READ_FIELDS = [
    "监控任务",
    "竞品品牌",
    "关键词",
    "关键词别名",
    "排除词",
    "平台",
    "启用",
    "最近成功采集时间",
    "产品系列词",
    "产品型号词",
]

EVENT_READ_FIELDS = [
    "事件名称",
    "竞品品牌",
    "产品系列",
    "正式开售日期",
    "来源类型",
    "人工确认状态",
]
