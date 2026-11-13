# Bloomberg 数据处理说明

本文件：Bloomberg 数据的处理规则、流程、代码、实证结论、未确认问题。
第一阶段保留所有证券类型（数据中除股票外还有权证等），不做任何筛选。
通用要求见根目录 README.md；当前进度见 PROGRESS.md；xdata 存储约定见 common/README.md。
最后更新：2026-11-13

## 1. 数据集

原始数据路径：/mnt/win-g12x/Research/BBG/{数据集名}/*.parquet
入库路径：stock_1d/xdata_test/vendor_data/bbg/{数据集名}（主键冲突行表 {数据集名}__dup，主键为空的行表 {数据集名}__nokey；试跑写到 {数据集名}_trial 及其 __dup / __nokey）

数据集 | 主键（第一个列入库后改名为 date） | 主键冲突时的选行规则（暂定，待 diagnose 验证） | 存储粒度(data_type)
---|---|---|---
equityNamrV2 | DL_SNAPSHOT_DATE, ID_BB_UNIQUE | n_nonnull → row_order | second
equityNamrCins | DL_SNAPSHOT_DATE, ID_BB_UNIQUE | n_nonnull → row_order | second
equityNamrPricing | DL_SNAPSHOT_DATE, ID_BB_UNIQUE, EXCH_CODE | has_px_last → n_nonnull → row_order | second
equityNamrCorporateActionsV2 | DL_SNAPSHOT_DATE, ID_BB_UNIQUE, ACTION_ID, CP_AMENDMENT_DATE | comp_match → n_nonnull → row_order | second
effDtEquityNamrCorporateActionsCorporateActions | 同上 | 同上 | second
premarketEquityNamrCorporateActionsV2CorporateActions | 同上 | 同上 | second

选行规则的含义：按顺序比较，值越大越优先。

- n_nonnull：非空字段数；
- has_px_last：PX_LAST 是否非空；
- comp_match：CP_ID_BB_COMP == ID_BB_COMPANY；
- row_order：在原文件中越靠后越优先。

主键沿用原脚本的定义，尚未用数据验证。

### ⚠️ 核心前提假设：第一列必须是日期

目前第一阶段的所有代码（包括 bbg_clean.py、bbg_common.py 等），都基于一个极强的默认前提：**数据的业务主键中，第一个必须是日期（或时间戳）列。**

在配置 DATASETS 的 key 时，必须遵循以下硬性规则：
1. key 列表的第一个元素，必须是代表日期的列（对于 BBG 数据，就是 DL_SNAPSHOT_DATE）。
2. 在落库时，代码会自动把这个第一列重命名为 date，并作为 xdata 的 MultiIndex 的第一层级（例如 ['date', 'ID_BB_UNIQUE']）。

代码中大量依赖此假设的地方：
- bbg_clean.py 的 split_nokey()：使用 key[1:] 来排除日期列，检查其他业务主键是否为空。
- bbg_clean.py 的 to_xdata_frame()：使用 index_cols[1:] 来配合日期列生成多级索引。
- bbg_schema.py 等推断脚本：默认按照日期进行文件分组（plan_groups）。
- 下游 bbg_read.py：默认第一层索引是日期（date），方便按时间范围切片读取。

未来扩展的注意事项：
如果未来接入的新数据商（或新数据集）的数据结构不是“按日期切片”的（例如：按证券 ID 切分，或者没有日期列），不能直接复用当前的代码。必须重写 plan_groups（分组逻辑）和 to_xdata_frame（索引构建逻辑）。

## 2. 代码

文件 | 作用
---|---
bbg_common.py | 配置（路径、数据集、缺失值占位符等）+ 类型系统：推断规则、cast_series 强制转换、check_values 取值校验、to_storage / from_storage 存储格式转换 + 日志与并行：setup_logger、run_parallel
bbg_schema.py | infer：用历史数据推断每列类型 → schema/inferred/{name}.json + reports/{name}_infer.csv；verify：用 schema 在全量历史上逐列强制转换 → reports/{name}_verify.csv
bbg_report.py | 把推断报告压缩成适合截图的几段：types / strings / na / typed / col 列名
bbg_inspect.py | 数据规模与机器资源摸底
bbg_clean.py | diagnose：重复数据诊断（不写库）→ reports/{name}_dup_*.csv；build：全量入库；update --start 日期：增量入库
bbg_read.py | 下游读取入口 read_bbg(name, table='', **cli.read参数)，按 schema 还原 dtype

- 人工修正某列类型：写 schema/overrides/{name}.json，格式 {"columns": {"列名": {"type": "string"}}}。重跑推断不会覆盖它；修正后需要重跑 verify。
- 入库日志：logs/{name}_{build|update}_{时间}.jsonl，每组文件一行，包含错误、提示、重复统计。
- 运行日志：logs/{name}_{diagnose|build|update}_{时间}.log，与终端输出相同，带时间戳：分组用时、每完成一组的用时 / 内存峰值 / 预计剩余、每次写库的用时和主进程内存、汇总。内存峰值标 (task) 表示该任务自己的峰值，标 (process) 表示 worker 进程启动以来的峰值。

## 3. 第一阶段处理流程

### 3.1 类型推断（bbg_schema.py infer）

- 每个任务只处理一小批列，逐个文件只读这些列。判定只基于每个文件里的去重取值，内存占用小。
- 先扫描所有文件的列名取并集。
- 候选顺序：bool → int → float → 日期 → ISO 时间 → string。全部历史值都能转的最严格类型胜出。
- 防误判：带前导零、超长数字、超过 2^53 的整数不转数字。字段名含 DT/DATE 才优先判为日期。
- bool 只认 Y/N/TRUE/FALSE。
- 枚举记录：不同取值 ≤ 500 种的 string/int 列，记录完整取值集合；数值、日期列记录最小、最大值。
- 嵌套结构（列表 / 结构体）存成 JSON 字符串。

### 3.2 全量校验（bbg_schema.py verify）

用 schema（推断结果 + 人工修正）在全部历史文件上逐列跑真正的转换函数，统计转不过去的值。

### 3.2.1 增量校验与 Schema 的历史底线（2026-11-13）

随着后续增量数据（`update`）的引入，必须明确 **Schema（`schema/inferred/*.json`）是历史全量数据（如 2006~2026）的客观记录，是“历史底线”**。

1. **增量的校验方式**：
   日常跑增量前，使用 `python bbg_schema.py verify equityNamrV2 --start YYYY-MM-DD` 仅校验新文件。若新数据出现新的极值（如价格突破历史最高价），`check_values` 会在日志中打出 `above_hist_max` 或 `unseen_value` 等 Warning。这属于**正常业务变化**，仅做提醒，绝不阻断 `clean.py update` 的入库。

2. **绝不轻易覆盖底线**：
   **严禁**为了消除上述 Warning，直接把增量数据的新极值覆盖写回 `schema/inferred/*.json`。如果新数据的最小值变大、最大值变小，强行覆盖会导致历史极值（如 2006 年的极低价）丢失，未来一旦回溯历史数据，将引发大规模的误报或精度灾难。

3. **底线的更新时机**：
   只有当积累了大量新数据（如跨越数年后），或者 Bloomberg 更改了底层字段定义时，才通过重新执行全量 `python bbg_schema.py infer equityNamrV2`（基于所有新旧文件）来重新计算一版新的 Schema。此时生成的 JSON 才是包含最新时间跨度的真正新底线。

### 3.2.2 推断与校验结果的检查规则（2026-11-13）

正式入库前，必须按以下规则检查 `infer` 和 `verify` 的输出，确认无误后方可执行 `build`。

**（1）检查 `infer` 结果（类型推断）**
- **位置**：终端日志、`reports/{name}_infer.csv`
- **规则**：
  - 确认终端打印的“需要人工确认的列”，尤其注意 `decided_by: name_rule`（因字段名含 DT/DATE 被强制判为日期）的列是否符合业务常识。
  - 检查 `type` 分布，确保核心数值列未被错误降级为 `string`。若被降级，在 `_infer.csv` 中查看 `why_not_float` 或 `nonnum_values`，通常是因为混入了极少数占位符（如 `N.A.`）。
  - 检查 `note` 列，确认 `oob_examples`（超范围年份）符合 R3 规则的预期；确认 `nested` 结构体按 `json` 或 `string` 处理。

**（2）检查 `verify` 结果（全量校验）**
- **位置**：终端日志、`reports/{name}_verify.csv`
- **规则**：
  - **一票否决项**：终端提示“校验未通过 X 列”或报告中出现 `NOT_IN_SCHEMA`（新列）。若 `n_bad > 0`，说明存在按 schema 转不过去的值，必须排查原始数据或调整 schema，**严禁继续执行 `build`**。
  - **警告放行项**：报告 `warnings` 列中的 `above_hist_max`、`unseen_value` 等，属于增量数据自然突破历史范围或产生新枚举值，仅作为提醒，**允许通过**。
  - **放行标准**：终端必须明确打印“全部 N 列在 M 个文件上均可按 schema 转换”，方可进入 `build`。

**（3）增量更新时的检查**
- 使用 `verify --start YYYY-MM-DD` 仅校验增量文件。此时只需确保没有 `n_bad` 致命错误，警告类信息可以忽略。检查通过后，执行 `clean.py update` 进行增量入库。

### 3.3 入库（bbg_clean.py build / update）

- 文件分组：先只读各文件的 DL_SNAPSHOT_DATE，按日期排序。日期范围有重叠的文件合成一组一起处理。只解析去重后的日期。
- 字段校验：
  - 出现 schema 里没有的新字段 → 报错，不入库。
  - schema 里有、文件里缺的字段 → 补成空列。
- 强制类型转换：按 schema 转换；转不过去的值 → 报错，不入库。
- 取值校验：超出历史范围/新类别 → 打印提示，照常入库。
- 主键为空的行：单独存进 {name}__nokey，索引 (date, NOKEY_SEQ)。
- 重复处理：完全重复只留一行；主键冲突存 {name}__dup，主表按规则选一行。
- 写入：并行只做读取和转换，由主进程按日期顺序写入（n_jobs + max_buffer 控制内存）。
- 存储粒度：注册为 data_type='second'（按天分块，并行写）。代码中已抽离至 DATASETS 字典配置。
- 安全措施：build 遇已存在拒绝；update 遇库中该日期段已有数据拒绝。
- 严格校验：任何不符合 schema 的列/类型，直接 raise KeyError，绝不静默放行。

## 4. 改动原始值的规则（已确认）

以后遇到类似情况，先问我，确认后再加进来（R4、R5……）。

# | 规则 | 适用范围 | 理由 / 依据 | 留痕方式 | 确认日期
---|---|---|---|---|---
R1 | 字符串等于 ''、N.A.、N.S. 时视为缺失 | 所有列 | N.S. 在数据中实测出现；N.A. 我记得有 | 推断报告 na_token_counts 记录次数 | 2026-11-11
R2 | 只由空白字符组成的字符串视为缺失 | 所有列 | 固定宽度补齐留下的空白，不含信息。末尾空格原样保留 | 推断报告 na_token_counts 记为 '<空白>' | 2026-11-11
R3 | 超出 pandas 日期范围（1677-09-21 ~ 2262-04-11）的日期置为 NaT | 所有判为 date/datetime 的列 | pandas 无法表示；实测值如 2313 年，不可能是真实日期。格式正确但年份超范围才置空 | 推断报告样例；verify 记录；入库日志记录 | 2026-11-11

明确不做的改动：
- 'None' 不视为缺失（可能是真实取值）。
- nan、NULL、null 等不视为缺失。
- 不去掉文本前后的空格。
- 不做任何行筛选。

## 5. 不改动原始值的处理决定

决定 | 理由
---|---
COLUMN0 参与去重比较 | 实测它是请求标识，属于原始信息
主键（除日期外）任一列为空的行，单独存 __nokey 表（方案 A） | 这 17 行是没取到数据的请求记录。放主表会主键不唯一；放 __dup 含义不对；不删除
主键冲突时主表的选行规则 | 沿用原脚本思路

## 6. 实证结论

### equityNamrV2（2026-11-11）

- 235 个文件，每个文件覆盖一段日期；160 列。
- 规模：总行数 214,041,973；磁盘合计 45.2 GB；最大文件读成 pandas 后 7.01 GB。
- 推断结果：string 83、empty 21、float 19、int 18、date 10、bool 7、datetime 2。
- 全量校验：160 列在 235 个文件上都能按 schema 转换。
- 占位符：N.S. 只出现在 SEDOL 相关 4 列，整列都是。N.A. 未观测到。
- COLUMN0 取值如 'AAPL    US Equity'，string 类型。
- 带前导零的代码列判为 string。
- 21 个全空列；最早 30 个文件有 57 个全空列，全量剩 21 个。
- 重复诊断：0 完全重复；3 个主键、7 行冲突（全是空主键导致，方案 A 处理）。
- 试跑入库（build --trial 3）读回检查通过：主表 3,840,813 行 × 159 列，dtype 一致；__nokey 2 行。
- 用 cli.read 读 int/bool 显示为 float；用 bbg_read.read_bbg 还原成 Int64/boolean。

## 7. 未确认的问题

问题 | 去哪里确认 | 何时需要
---|---|---
N.A.、N.S.、DVD_FREQ 里 'None' 的确切含义 | 数据说明；终端 HELP HELP | 写清规则依据时
COLUMN0 的确切定义 | 数据说明 | 同上
RC 的含义 | 数据说明；终端 HELP HELP | 写清规则依据时 / 第二阶段
EQY_SH_OUT_TOT_MULT_SH 最小值 -999991 是否为特殊标记值 | 终端 FLDS <GO> | 第二阶段
日期字段中其它不合理的日期 | 第二阶段处理 | 第二阶段

## 8. 变更记录

日期 | 内容
---|---
2026-11-11 | 建立；R1~R3 确认；方案 A 确认；新增 build --trial N
2026-11-11 | 加运行日志；去掉 --batch，边算边按顺序写库；默认并发 10 → 40
2026-11-12 | 试跑改用 second 存储，耗时从 184s 降至 23s。补充“第一列必须是日期”前提。data_type 抽离为可配置项。
2026-11-13 | bbg_schema.py 增加 verify 的 --start 增量校验；新增 3.2.1 历史底线规则、3.2.2 检查规则说明。增加严格校验（Fail Fast）原则。