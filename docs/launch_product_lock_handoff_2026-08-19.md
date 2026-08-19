# KOL 新品活动专用锁：P0 实施记录

## 问题与影响

- 现有每日派单只看“主推 + 五项就绪”，不知道产品是否已经进入定点集中上稿活动。
- POWKONG 食人花二代在产品库有两条同时就绪的记录，常规派单每天会各建一批任务。
- 已合作 KOL 的 `secondary_outreach` 也会从主推产品池选产品；只改每日派单会留下另一个绕过入口。

## 处理规则

- 产品库新增 `派单模式`：空值与“常规派单”保持旧行为；“活动专用”“暂停”及任何未知非空值都不进入常规派单（填错时先拦截）。
- 新增 `活动归并键`、`活动主记录`、`活动主记录ID`，只做逻辑归并，不删除产品记录或历史关联。
- 食人花主记录为 `recvhAqrCyCPgl`；别名记录 `recvqD87uSM1Fh` 指向该主记录。
- Dave 记录 `recvkJOoCsNb1s` 也标记为活动专用，避免未来价格闸恢复后误入常规派单。
- 三道锁：每日调度不建新任务；`enrich/enrich_editor` 取消已排队的旧任务；`auto_send` 扫描和发送前再次拦截已生成的首次 cold 草稿。
- 历史空来源按 cold 处理；reply、follow-up、寄样、物流、退信和上稿闭环不受影响。
- 主动开发信缺关联产品、产品回读为空或读取失败时一律停止发送，并单列 `lock_validation_failures`，避免被伪装成“没有待发邮件”。

## 改动文件

- `app/product_dispatch_mode.py`：派单模式与归并组验证的单一规则源。
- `app/dispatch.py`：每日 KOL/媒体人常规派单排除活动专用与暂停产品。
- `app/secondary_outreach.py`：已合作 KOL 二次维护产品池使用同一把锁。
- `app/enrich.py` / `app/enrich_editor.py`：拦住写锁前已经排队的常规任务。
- `app/auto_send.py`：拦住写锁前已经生成的首次开发信，并在真正发送前二次回读产品锁。
- `scripts/apply_launch_product_lock.py`：默认 dry-run、显式 `--commit` 才写；写前校验表结构、产品品牌/SKU/名称，部分失败自动回滚已写记录。
- `tests/test_product_dispatch_mode.py` / `tests/test_launch_product_lock_migration.py`：覆盖旧产品兼容、活动/暂停/未知值拦截、cold 与正常跟进分流、食人花归并、入口过滤及迁移前置校验。

## 验证

- 专项测试：14/14 通过。
- 修改文件语法检查：通过。
- 全仓测试：218 个中 217 通过；`test_zeabur_watchdog` 有 1 个与本次无关的既有失败，本次未改该模块。
- 生产验证必须按顺序：先部署代码并核实 commit，再写活动锁，再回读三条记录并确认常规候选池不包含它们。

## 回滚

- 写入过程中若中途失败，迁移脚本会自动恢复已经改过的记录；新建字段保留但为空。
- 上线后人工回滚：把三条记录的 `派单模式` 改回“常规派单”或清空；不需要删除字段。
- 代码回滚：恢复 `dispatch.py` 与 `secondary_outreach.py` 对活动锁的过滤。
- 不删除食人花别名记录，避免丢失历史任务、草稿和联系人关系。
