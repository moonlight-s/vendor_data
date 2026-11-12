# Bloomberg 数据处理说明

本文件：Bloomberg 数据的处理规则、流程、代码、实证结论、未确认问题。
第一阶段保留所有证券类型（数据中除股票外还有权证等），不做任何筛选。
通用要求见根目录 README.md；当前进度见 PROGRESS.md；xdata 存储约定见 common/README.md。
最后更新：2026-11-12

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
bbg_common.py | 配置（路径、数据集、缺失值占位符等）+ 类型系统：推断规则、cast_series 强制转换、check_values 取值校验、to_storage / from_storage 存储格式转换 + 日志与并行：setup_logger、run_parallel（进度按已完成计，记录每个任务的用时和内存峰值）
bbg_schema.py | infer：用历史数据推断每列类型 → schema/inferred/{name}.json + reports/{name}_infer.csv；verify：用 schema 在全量历史上逐列强制转换 → reports/{name}_verify.csv
bbg_report.py | 把推断报告压缩成适合截图的几段：types / strings / na / typed / col 列名
bbg_inspect.py | 数据规模与机器资源摸底
bbg_clean.py | diagnose：重复数据诊断（不写库）→ reports/{name}_dup_*.csv；build：全量入库；update --start 日期：增量入库
bbg_read.py | 下游读取入口 read_bbg(name, table='', **cli.read参数)，按 schema 还原 dtype

- 人工修正某列类型：写 schema/overrides/{name}.json，格式 {"columns": {"列名": {"type": "string"}}}。重跑推断不会覆盖它；修正后需要重跑 verify。
- 入库日志：logs/{name}_{build|update}_{时间}.jsonl，每组文件一行，包含错误、提示、重复统计。
- 运行日志（2026-11-11 起，bbg_clean.py 的三个命令）：logs/{name}_{diagnose|build|update}_{时间}.log，与终端输出相同，带时间戳：分组用时、每完成一组的用时 / 内存峰值 / 预计剩余、每次写库的用时和主进程内存、汇总。内存峰值标 (task) 表示该任务自己的峰值，标 (process) 表示 worker 进程启动以来的峰值（系统不支持重置峰值时）。bbg_schema.py 还没加，以后需要重跑时再加。

## 3. 第一阶段处理流程

### 3.1 类型推断（bbg_schema.py infer）

- 每个任务只处理一小批列，逐个文件只读这些列。判定只基于每个文件里的去重取值，内存占用小。
- 先扫描所有文件的列名取并集（原脚本只看第一个文件，中途新增的列会被漏掉）。
- 候选顺序：bool → int → float → 日期（%Y%m%d、%Y-%m-%d、%m/%d/%Y、%Y/%m/%d）→ ISO 时间 → string。全部历史值都能转的最严格类型胜出。
- 防误判：
  - 带前导零的值（如 0174）不转数字；
  - 超过 15 位的数字不转 float；
  - 整数和 %Y%m%d 日期都说得通时，字段名含 DT/DATE 才判为日期，并标注"请确认"；
  - 整数绝对值超过 2^53 判为 string。
- bool 只认 Y/N/TRUE/FALSE（不区分大小写）；0/1 判为 int。
- 不同取值 ≤ 500 种的 string/int 列，记录完整取值集合；数值、日期列记录最小、最大值。
- 原始 parquet 里是列表 / 结构体的字段，存成 JSON 字符串（原脚本用 k=v|... 格式，值本身含 , | = 时会解析错）。

### 3.2 全量校验（bbg_schema.py verify）

用 schema（推断结果 + 人工修正）在全部历史文件上逐列跑真正的转换函数，统计转不过去的值。

### 3.3 入库（bbg_clean.py build / update）

- 文件分组：先只读各文件的 DL_SNAPSHOT_DATE，按日期排序。日期范围有重叠的文件合成一组一起处理（否则跨文件的重复查不到）。只解析每个文件里去重后的日期（2026-11-11 改；原来逐行解析，内网实测分组用了约 7.5 分钟；本地用 161 万行的模拟文件测，单个文件从 1.68s 降到 0.04s，结果相同）。
- 字段校验：
  - 出现 schema 里没有的新字段 → 报错，不入库（xdata 已有表能否追加新列，未确认）；
  - schema 里有、文件里缺的字段 → 补成空列，保证每次入库的列一致。
- 强制类型转换：按 schema 转换；转不过去的值 → 报错，不入库（不静默丢弃）。
- 取值校验：超出历史最小/最大值、出现历史上没有的类别、全空列出现了值 → 打印提示并记入日志，照常入库。
- 主键为空的行：主键里除日期外任一列为空的行，原样存进 {name}__nokey（不去重、不参与冲突判断），索引 (date, NOKEY_SEQ)，NOKEY_SEQ 为同一天内的序号（按原始行顺序）。见第 5 节。
- 重复处理（只对主键完整的行）：
  - 所有列都相同的行只留一行（无信息损失）；
  - 主键冲突的行全部原样存入 {name}__dup 表，并加 DUP_CHOSEN、DUP_SEQ 两列；
  - 主表按第 1 节的规则选一行，并加 DUP_N 列（该主键在原始数据中的行数，1 表示无冲突）。
- 写入：并行只做读取和转换，由主进程按日期顺序写入（原脚本在并行进程里各自 append，可能重复 register，顺序也不对）。2026-11-11 起：哪组先算完先收下，轮到它时立即写库，worker 不必等整批算完（原来每批 --batch 组一起跑，并发最多只有 batch 个，而且要等整批最慢的一组）；"正在算的 + 已算完等着写的"合计不超过 n_jobs + max_buffer 组，以控制主进程内存。默认 --n-jobs 40、--max-buffer 10。
- 存储粒度（2026-11-12 起）：根据 xdata 维护者答复，注册为 data_type='second'，按天分块，可多进程并行写。代码中已将此参数抽离至 DATASETS 字典的 data_type 字段，方便未来按表定制。
- 安全措施：
  - build 时如果目标路径（含 __dup、__nokey）已存在，拒绝执行；
  - 正式 build 前先用 --trial N 把最早 N 组写到 {name}_trial 试跑真实 xdata（读回用 read_bbg(name, table='_trial')）；
  - update 时如果库里该日期段已有数据，拒绝写入。

## 4. 改动原始值的规则（已确认）

以后遇到类似情况，先问我，确认后再加进来（R4、R5……）。

# | 规则 | 适用范围 | 理由 / 依据 | 留痕方式 | 确认日期
---|---|---|---|---|---
R1 | 字符串等于 ''、N.A.、N.S. 时视为缺失 | 所有列 | N.S. 在数据中实测出现（SEDOL 相关 4 列，整列都是）；N.A. 我记得有；'' 为空串 | 推断报告 na_token_counts 列记录每列各占位符的次数 | 2026-11-11
R2 | 只由空白字符组成的字符串（如 '  '）视为缺失 | 所有列 | 固定宽度补齐留下的空白，不含信息（实测见 SEDOL1_COUNTRY_ISO）。文本末尾的空格不受影响，原样保留 | 推断报告 na_token_counts 中记为 '<空白>' | 2026-11-11
R3 | 超出 pandas 日期范围（1677-09-21 ~ 2262-04-11）的日期置为 NaT | 所有判为 date/datetime 的列 | pandas 无法表示这些日期，否则整列只能存成 string；实测值如 2313-12-23、3026-03-31，不可能是真实日期。只针对格式正确但年份超范围的值，其它解析失败的值仍然报错、不入库 | 推断报告"需要人工确认"中列出样例；verify 报告记录个数；入库日志（kind=date_out_of_range_to_NaT）记录原始值 | 2026-11-11

明确不做的改动：

- 'None' 不视为缺失：在 DVD_FREQ 中出现约 3100 万次，很可能是真实取值（确切含义待确认）。
- nan、NULL、null 等字符串不视为缺失：实测没有出现。以后若出现，会在推断 / 校验报告中显示出来，届时再问我。
- 不去掉文本中的前后空格（如 COLUMN0 的固定宽度补齐）。
- 不做任何行筛选：原来按 RC、证券类型、国家、交易日的过滤全部去掉，放到第二阶段。

## 5. 不改动原始值的处理决定

决定 | 理由
---|---
COLUMN0 参与去重比较（META_COLS = []） | 实测它是 'AAPL    US Equity' 这类请求标识，属于原始信息；只有 COLUMN0 不同的行作为主键冲突进入 __dup 表
主键（除日期外）任一列为空的行，单独存 {name}__nokey 表（2026-11-11 她确认，方案 A） | equityNamrV2 有 17 行 ID_BB_UNIQUE 为空，是不同证券的、没取到数据的请求记录（见第 6 节）。放主表会让主键不唯一（xdata 索引能否含空值未确认）；放 __dup 含义不对；删除违反第一阶段保留全部原始数据的原则。不改动任何值
主键冲突时主表的选行规则（第 1 节） | 沿用原脚本的思路。equityNamrV2 主键完整的行没有冲突，用不上；其余数据集待各自 diagnose 验证

## 6. 实证结论

### equityNamrV2（2026-11-11）

- 235 个文件（equityNamrV2.20061015-20061031.parquet ~ equityNamrV2.20260401.parquet），每个文件覆盖一段日期；160 列。
- 规模（bbg_inspect）：总行数 214,041,973；每个文件 35,609 ~ 1,616,880 行（中位数 817,495）；磁盘合计 45.2 GB；最大文件 equityNamrV2.20081201-20081231.parquet 读成 pandas 后 7.01 GB。
- 推断结果：string 83、empty 21、float 19、int 18、date 10、bool 7、datetime 2。
- 全量校验：160 列在 235 个文件上都能按 schema 转换。
- 占位符实际出现情况：N.S. 只出现在 SEDOL 相关 4 列，而且整列都是；N.A. 在数据中暂未观测到；''、nan、NULL 等都没有出现。
- COLUMN0 不是行号，取值是 'AAPL    US Equity' 这类请求标识（固定宽度，用空格补齐）；各文件中都是 string 类型。
- 带前导零的代码列判为 string：EQY_SIC_CODE、ID_DUTCH、ID_VALOREN、ID_FRENCH、ID_BELGIUM、ID_COMMON、ID_NAICS_CODE（出现了 072203）。
- 21 个全空列：ID_SEDOL1、EQY_DVD_PCT_FRANKED、REDENOM_ROUND_METHOD、IS_STK_MARGINABLE、WHICH_JAPANESE_SECTION、ID_SEDOL2、SEDOL1_COUNTRY_ISO、SEDOL2_COUNTRY_ISO、ID_MIC_LOCAL_EXCH、LSE_SECTOR、LSE_SEGMENT、MTF_ASSC_MKT_COMP_ID_BB_GLOBAL、MTF_ASSC_MKT_PRIM_ID_BB_GLOBAL、TRADING_SYSTEM_IDENTIFIER、TRADING_SYSTEM_IDENTIFIER_DES、SSE_SEHK_CONN_ASSC_ID_BB_GBL、SSE_SEHK_CONN_COMP_ID_BB_GBL、SZSE_SEHK_CONN_ASSC_FIGI、SZSE_SEHK_CONN_ASSC_COMP_FIGI、A_OVER_H_SHARE_CONVERSION、DOMAIN_SETTLEMENT_SECURITY_ID。
- 只看最早 30 个文件时有 57 个全空列，全量时只剩 21 个，说明很多字段是历史后段才开始有值的。
- 重复诊断（diagnose，2026-11-11）：没有日期重叠的文件（235 个文件各自一组）；完全重复行 0；主键 (DL_SNAPSHOT_DATE, ID_BB_UNIQUE) 冲突只有 3 个主键、7 行，冲突行之间只有 COLUMN0 不同，其余 159 列全部相同。但看样例发现这 7 行的 ID_BB_UNIQUE 都是空的：COLUMN0 分别是 LIMC US Equity、915106Z CN Equity、925165Z CN Equity 等不同证券，是因为主键为空被归到同一组（groupby(dropna=False)），不是同一证券的重复。主键非空的行没有任何冲突。
- ID_BB_UNIQUE 为空的行（全量扫描，空值含 R1/R2 占位符）：17 行，分布在 9 个文件，日期 2006-12-04 ~ 2008-01-28。每行只有 6 列有值：DL_DATASET、DL_SNAPSHOT_DATE、DL_SNAPSHOT_START_TIME、DL_SNAPSHOT_END_TIME、COLUMN0、RC，其余字段全空。COLUMN0 如 'INXD    US Equity'、'831672Z CN Equity'。看起来是请求了但没有取到数据的记录（RC 含义未确认，见第 7 节）。
- 试跑入库（build --trial 3，2006-10-15 ~ 2006-12-31，67 个日期）读回检查通过：主表 3,840,813 行 × 159 列，主键唯一、无空值、日期升序；dtype 为 string 103 / Int64 19 / float64 19 / datetime64 11 / boolean 7，与 schema 一致（160 列 − 2 个索引列 + DUP_N）；__dup 未生成（无冲突）；__nokey 2 行（2006-12-04 853194Z US Equity、2006-12-15 831672Z CN Equity），RC 都是 10。她查看 2006-12-31 的正常行，RC 为 0。RC 含义仍未确认。
- 用 cli.read 直接读时，int / bool 列显示为 float（如 RC 显示 0.0），这是存储约定；用 bbg_read.read_bbg 读会还原成 Int64 / boolean。
- DVD_PAY_DT、DVD_RECORD_DT 在原始 parquet 里本身是 date32 类型，含超范围日期（按 R3 处理）：2313-12-23、3026-03-31、3026-03-30、3009-09-30、3013-03-31。

## 7. 未确认的问题

从外部来源确认后：原始材料放进 references/，在 SOURCES.md 登记，再把结论连同来源编号写进本文件。

问题 | 去哪里确认 | 何时需要
---|---|---
N.A.、N.S.、DVD_FREQ 里 'None' 的确切含义 | 内网 Bloomberg 数据说明；data.bloomberg.com 上 Data License 的文档；终端 HELP HELP | 写清规则依据时
COLUMN0 的确切定义 | 数据说明 | 同上
RC 的含义（17 行 ID_BB_UNIQUE 为空的记录都有 RC，取值待查） | 内网 Bloomberg 数据说明；Data License 文档；终端 HELP HELP | 写清规则依据时 / 第二阶段
EQY_SH_OUT_TOT_MULT_SH 最小值 -999991 是否为特殊标记值 | 终端 FLDS <GO> 查字段说明；数据说明 | 第二阶段
DVD_SH_LAST、LAST_DPS_GROSS 最大值约 1.13e14；EQY_SPLIT_ADJ_INIT_PO_PX 最大值 6.25e17 | 先查是哪几只股票、哪几天 | 第二阶段
DL_SNAPSHOT_START_TIME / END_TIME 最晚到 2025-08-31，而 DL_SNAPSHOT_DATE 最晚到 2026-04-01 | 查近几个月的文件中这两列是否为空 | 第二阶段前
日期字段中其它不合理的日期（如 DVD_RECORD_DT 到 2099 年、EQY_SH_OUT_DT 到 2108 年） | 第二阶段处理 | 第二阶段

## 8. 变更记录

日期 | 内容
---|---
2026-11-11 | 建立；equityNamrV2 完成推断与全量校验；R1~R3 确认
2026-11-11 | 主键为空的行单独存 __nokey 表（方案 A）；diagnose 同样把这些行排除在冲突判断外；新增 build --trial N 试跑
2026-11-11 | bbg_clean.py：加运行日志（进度、用时、内存峰值）；分组只解析去重日期；去掉 --batch，改为边算边按顺序写库（--max-buffer）；默认并发 10 → 40。用模拟数据 + 假 xdata 测过 infer / diagnose / build / 重复 build 拒绝 / update 拒绝 / 读回 / 出错中止，日期列为字符串和 date32 两种情况都测了
2026-11-12 | 第 7 节：删去「ID_BB_UNIQUE 为空的 17 行待定」一行，改为已确认方案 A（见第 5 节）
2026-11-12 | 试跑（--trial 3）改用 second 存储，耗时从 184s 降至 23s。补充“第一列必须是日期”的核心前提假设。将 data_type 抽离为数据集可配置项。