# 进度

> 每完成一步更新一次。最上面是"现在在哪、下一步做什么"，下面是按时间顺序的日志。
> 最后更新：2026-11-11

---

## 现在在哪

- **数据源：** Bloomberg
- **数据集：** `equityNamrV2`（Bloomberg 共 6 个数据集中的第 1 个）
- **阶段：** 第一阶段。类型推断、全量校验、规模摸底、重复诊断已完成，还没有入库
- **下一步（只做这一件）：** 她把下面的问题发给 xdata 维护者，拿到答复截图：
  1. （已知 `daily` 按年存、按年读）append 时是不是要重写所在的整个年块？我在同一年里每次 append 约 150 万行 × 160 列，耗时从 108s 涨到 184s。
  2. 如果改成 `second`（按天存）：一次 write / append 传入多天的数据，会自动按天拆开存吗？索引是 (date, ID) 两级、date 是零点的日期，可以吗？
  3. 按天存的表，多个进程同时往同一张表 append 不同日期的数据，安全吗？是不是要先 register 好再并行？
  4. 按天存以后，append 的耗时还会随表变大而变长吗？读的时候是不是只读涉及的那几天？读几年的数据，会不会因为分块太多而变慢？
  5. 怎么删除一个已经注册的 path？（要清理测试表）
  6. `register(path, data_type, 'xx', is_public, desc)` 的第三个参数是什么意思？
  7. （已知是列式存储）读的时候只读指定的几列吗？在一个分块里能按日期只读一部分行，还是要读整个分块？文件是不是按"分块 × 列"存的？
- **下游怎么读：** 她目前还不确定（2026-11-11）。Claude 的默认建议：用 `minute`（按月分块），写入和读取都居中；除非维护者的答复显示按天读多年也很快。试跑后实测"读一天"和"读一年"的耗时来验证。第一阶段的表能从原始 parquet 重建，以后读法明确了可以换粒度重建（前提是知道怎么删除 xdata 路径）。
- **之后：** 开新对话（Opus 5.5 + High），按维护者答复重新设计写库方式（很可能改成 `second` 按天存、worker 并行写），改 `bbg_clean.py`，再试跑。正式 build 前她要先读代码（README 要求 15）。
- **diagnose 之后、build 之前要做的代码修改**（一起改，只需替换一次内网文件）：
  1. 按 diagnose 结果确定主键冲突时的选行规则；
  2. ✅ 已改（还没替换到内网）：`bbg_clean.py` 加进度日志 + 每个任务的内存峰值；`bbg_common.py` 新增 `setup_logger` / `run_parallel`。`bbg_schema.py` 以后需要重跑时再加；
  3. ✅ 已改（还没替换到内网）：分组只解析去重日期；`build` 边算边按日期顺序写库，去掉 `--batch`；默认并发 40。
  内网环境：joblib 1.6.0 / pandas 2.2.3 / pyarrow 10.0.1（2026-11-11 查），本地用同版本 + numpy 1.26.4 测过。

## 总体路线

| # | 事项 | 状态 |
|---|---|---|
| 1 | xdata 支持哪些 dtype（实测） | ✅ 完成 |
| 2 | Bloomberg `equityNamrV2`：类型推断 → 人工审阅 → 全量校验 | ✅ 完成 |
| 3 | Bloomberg `equityNamrV2`：数据规模摸底（`bbg_inspect`） | ✅ 完成 |
| 4 | Bloomberg `equityNamrV2`：重复诊断（`diagnose`）→ 主键完整的行无冲突；主键为空的 17 行单独存 `__nokey`（她确认方案 A） | ✅ 完成 |
| 5 | Bloomberg `equityNamrV2`：试跑（`build --trial 3`，✅ 读回通过；写库太慢）→ 确定写库方式 → 全量入库（`build`），读回检查 | ⏭ 进行中 |
| 6 | Bloomberg 其余 5 个数据集：`equityNamrCins`、`equityNamrPricing`、3 个 CorporateActions，各走一遍 2~5 | 待做 |
| 7 | 用 GLM 从 Bloomberg 数据说明中摘取字段定义，解决 `bbg/PROCESSING.md` 中的未确认问题 | 待做 |
| 8 | Refinitiv 第一阶段 | 待做 |
| 9 | Bloomberg ↔ Refinitiv symbol 对应 | 待做 |
| 10 | 第二阶段：筛选与数据内容异常处理 | 待做 |

## 工作日志

### 2026-11-11
- 重写 Bloomberg 第一阶段代码：去掉所有筛选和 univ；类型由历史数据自动推断；新数据入库前做校验。
- xdata dtype 实测两轮，结论见 `common/README.md`。据此确定了存储格式：int/bool 存成 float64，读回时还原。
- `equityNamrV2` 试跑推断（前 30 个文件 → 等间隔抽 30 个文件），逐段审阅了 `types` / `na` / `strings` / `typed` / 单列：
  - `COLUMN0` 确认是 Bloomberg 请求标识，不是行号 → 不再在去重时忽略它。
  - `DVD_FREQ` 的 `'None'` 是取值 → 不当作缺失。
  - 只有空白的字符串、超范围日期 → 定为规则 R2、R3（已确认）。
- `equityNamrV2` 全量推断：235 个文件、160 列，约 16 分钟。
- `equityNamrV2` 全量校验通过：160 列在 235 个文件上都能按 schema 转换，约 36 分钟（`--n-jobs 10`）。
- 建立文档体系：README / PROGRESS / 各目录 PROCESSING。
- 新增 `SOURCES.md` / `references/` 用于登记外部信息来源；完善 `.gitignore`。
- 仓库定名 `vendor_data`（数据里还有权证等非股票证券，以后可能接入其它数据商，所以不叫 `us_equity_data`），位置 `/nfs/ssd0_73/gaosangzi/conda_envs/gsz_code/vendor_data`；xdata 根目录改为 `stock_1d/xdata_test/vendor_data`（Bloomberg 在 `.../bbg/`）。到这时为止还没有任何正式数据写入 xdata，不需要迁移。
- README 要求 13 修改：换模型只在开新对话时做；对话中途只调 effort；每步给出具体档位建议。
- `equityNamrV2` 规模摸底（`bbg_inspect`，实测）：235 个文件，总行数 214,041,973；每个文件行数 min 35,609 / 中位数 817,495 / max 1,616,880；磁盘 min 10 / 中位数 202 / max 276 MB，合计 45.2 GB；机器 CPU 256 核、内存 2003 GB（当时可用 1684 GB）；最大文件 `equityNamrV2.20081201-20081231.parquet` 读成 pandas 后 7.01 GB（1,616,880 行 × 160 列），读取 35s。
  - `diagnose` / `build` 的并发取 10。依据：每个进程除原始数据外，还要放类型转换后的副本和去重的中间结果，**估计**峰值是原始大小的 3~4 倍，即最大文件约 20~30 GB；10 个进程最坏约 300 GB，远低于可用内存。这是估计值，不是实测；另外日期重叠的文件会合并成一组处理，合并后一组有多大，要等 `plan_groups` 运行时才知道。
- `diagnose --n-jobs 10` 运行中实测（`ps`，主进程已运行 10:52 时的一次快照）：
  - 10 个 worker 每个占 1.5~6.0 GB（只是快照，不是峰值），比上面估计的 20~30 GB 低得多；主进程 6.0 GB；
  - worker 已运行 03:10，主进程 10:52 → 开始前的 `plan_groups`（只读 235 个文件的 `DL_SNAPSHOT_DATE` 并解析）用了约 7.5 分钟。原因**推测**是逐行解析 2 亿个日期字符串、且用线程并行受 GIL 限制，未实测确认；
  - 服务器是共享的（她确认）。
- `equityNamrV2` 重复诊断（旧版 `diagnose --n-jobs 10`，共 25.5 分钟，其中分组约 7.5 分钟）：
  - 235 个文件 → 235 组，**没有日期重叠的文件**；共 214,041,973 行；
  - **完全重复行 0**；
  - **主键冲突只有 3 个主键、7 行**（分布在 3 组文件里），差异列组合只有一种：只有 `COLUMN0` 不同，其余列全部相同；
  - 看样例（`reports/equityNamrV2_dup_samples.csv`）：这 7 行的 `ID_BB_UNIQUE` **都是空的**，`COLUMN0` 是不同的证券（如 `LIMC US Equity`、`915106Z CN Equity`），只是因为主键为空被归到一组。主键非空的行没有冲突；
  - 全量扫描 `ID_BB_UNIQUE` 为空的行（一次性脚本，约 4 分钟；CPU 平均只有 26%，主要在等读盘）：17 行、9 个文件、2006-12 ~ 2008-01；每行只有 `DL_DATASET`、`DL_SNAPSHOT_DATE`、`DL_SNAPSHOT_START_TIME`、`DL_SNAPSHOT_END_TIME`、`COLUMN0`、`RC` 6 列有值；
  - 她确认方案 A：主键为空的行原样存进 `equityNamrV2__nokey`。代码已改（含 diagnose），新增 `build --trial N`；本地模拟数据测过（同一天多行空主键、单行空主键、读回）；
  - 运行中 loky 报过一次警告 "A worker stopped while some jobs were given to the executor"（worker 超时或内存增长时会被重启），任务最终全部完成（235/235），没有组被跳过。
- `build --trial 3`（新版代码，内网实测，总 7m34s）：
  - 分组 45s（旧版约 7.5 分钟；仍主要在读盘）；
  - 计算：858,378 行 59s / 峰值 5.0 GB；1,489,887 行 103s / 8.6 GB；1,492,550 行 108s / 8.6 GB；
  - **写库：第 1 组（register + write）11s，第 2 组（append）108s，第 3 组（append）184s** —— 写库远慢于计算，是瓶颈；两组 append 行数相近但第 3 组慢很多，**怀疑** append 耗时随表已有数据量增长，只有两个点，未确认；
  - 主进程内存最高 11.7 GB；汇总 n_raw 3,840,815，n_nokey 2，完全重复 0，冲突 0；
  - 日志里"计算完成"那一行的时间戳会被写库耽搁（主进程写库时不收结果），但"用时"是 worker 端实测，准确。
- 试跑读回检查通过（详见 `bbg/PROCESSING.md` 第 6 节）：主表 3,840,813 行 × 159 列，主键唯一、无空值、升序，dtype 与 schema 一致；`__nokey` 2 行、`RC` = 10；读回整张表用了 62s。
- 她找到之前（2026-10-20）与 xdata 维护者的聊天：`second` 按天存储、可并行写；`minute` 适合按月写。记进 `common/README.md`，截图放 `common/references/`。写库方式待问清楚后重新设计。
- 她告知：xdata 存储地址是同一个，分块在底层硬盘；`daily` 按年存、按年读。试跑的 3 组都在 2006 年同一个年块里，追加变慢与此一致（推测追加要重写年块，待维护者确认）。分块粒度要结合下游读法来定。
- 她告知：xdata 是列式存储（2026-11-12）。给维护者的问题加了第 7 条。
