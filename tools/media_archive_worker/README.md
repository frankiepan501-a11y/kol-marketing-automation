# KOL 素材归档本地终端

## 结论

这台终端只负责五件事：格式检查 → 高画质下载 → 清晰度检查 → 飞书上传 → 表格回填。云端负责派发、YouTube 数据回填和终端存活检查。

- 下载规则：平台当前能拿到的最高画质，不设最低分辨率，不放大，不做有损转码。
- 一个平台链接一行；相同内容用“同源作品组”归并，只下载一份，其他平台行共用同一个飞书文件直链。
- 文件名：`平台简称-KOL-产品-序号.mp4`，例如 `YT-Amrie47-YS11-5-戴夫-01.mp4`。
- 云盘：`红人素材/视频/品牌/产品/文件`；表格回填 `/file/<token>` 文件直链，不回填文件夹链接。
- 任一步失败会登记具体环节和原因，可只重跑一条记录。
- 下载会锁定格式检查阶段选出的最高视频/音频轨；同分辨率还会核对帧率。上传成功但表格回填超时时，会用本地 SHA256 上传回执继续回填，不会盲认云盘同名文件。

## 灰度顺序

1. 旧终端先装 `yt-dlp`、FFmpeg、`lark-cli`，并使用聪哥分身 2 号的 bot 配置。
2. 复制 `.env.worker.example` 为 `.env.worker`，只在本机填写凭据和 cookies 路径。
3. 云端保持 `MEDIA_ARCHIVE_ENABLED=0`，先跑工具与权限检查。
4. 选一条 YouTube 小样本，在作品表只勾这一行“允许自动归档”。
5. 云端开 `MEDIA_ARCHIVE_ENABLED=1` 后只扫描该条，终端运行一次；核对画质、云盘路径、文件名、文件直链和数据回填。
6. 再分别用一条 TikTok、一条 Instagram 验证 cookies 与美国代理。
7. 旧终端稳定后，把同一目录和 `.env.worker` 迁到陈翔宇电脑，再安装开机任务；不要在 Frankie 开发机安装长期任务。

云端有三条 n8n 定时任务：每10分钟扫描队列、每天09:15刷新 YouTube 数据、每10分钟检查终端心跳。开关关闭时前两条只返回“已跳过”，不会改表。

## 运行

```powershell
# 安装 Python 侧依赖；FFmpeg 和 lark-cli 仍按各自安装方式放入 PATH。
python.exe -m pip install -r ./tools/media_archive_worker/requirements-worker.txt

# 只读检查工具版本、飞书视频目录权限和云端健康状态
pwsh -File ./tools/media_archive_worker/probe.ps1

# 单次灰度（最多领取一条）
pwsh -File ./tools/media_archive_worker/start-worker.ps1 -Once

# 指定一条重跑
pwsh -File ./tools/media_archive_worker/start-worker.ps1 -RecordId recxxxxxxxx

# 灰度通过后，安装开机静默任务；会弹出 Windows 凭据框
$credential = Get-Credential
pwsh -File ./tools/media_archive_worker/install-task.ps1 -Credential $credential
```

计划任务使用有密码的专用 Windows 用户运行，做到“不登录也能跑”，同时能读取该用户自己的 `lark-cli` bot 配置和 cookies 文件。不要改为依赖当前 Chrome 登录态。

## 检查证据

- 本地日志：`MEDIA_ARCHIVE_WORK_ROOT/logs/worker-YYYYMMDD.jsonl`
- 每条任务目录：`MEDIA_ARCHIVE_WORK_ROOT/<归档任务ID>/`
- 格式选择证据：`format-audit.json`
- 视频信息：`qa/ffprobe.json`
- 画面抽检：`qa/start.jpg`、`qa/middle.jpg`、`qa/end.jpg`
- 云端终端表：最后心跳、版本、主机、成功/失败、最后错误、最后记录 ID。

## 依赖

- [yt-dlp](https://github.com/yt-dlp/yt-dlp)：YouTube / TikTok / Instagram 格式识别与最高画质下载。
- [yt-dlp FFmpeg builds](https://github.com/yt-dlp/FFmpeg-Builds)：合并音视频、解码检查、抽质检帧。
- `lark-cli drive +upload`：文件超过 20 MB 时自动走飞书分片上传。
