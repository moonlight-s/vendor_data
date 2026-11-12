# common：与数据源无关的工具与结论

> 最后更新：2026-11-11

## xdata 支持的 dtype（实测）

脚本：`xdata_dtype_test.py`。只写测试路径；2026-11-11 的两轮实测在旧路径 `stock_1d/xdata_test/bbg/_dtype_test/` 下跑，现在脚本里已改为 `stock_1d/xdata_test/vendor_data/_dtype_test/`。旧路径下的测试数据可以删除。

### 第 1 轮（2026-11-11）：各类型能否原样写入、读回

| dtype | 结果 |
|---|---|
| `float64` | ✅ |
| `datetime64[ns]` | ✅ |
| object（纯 str + NaN） | ✅ |
| `int64`（无缺失） | ✅ |
| `bool`（无缺失） | ✅ |
| `Int64`（可空整数） | ❌ `cannot convert NA to integer`；xdata 提示应改用 int64 或 float64 |
| `boolean`（可空布尔） | ❌ `'BooleanDtype' object has no attribute 'str'` |
| pandas `string` | ❌ `decoded error, may be mixed object type, it should be pure string type` |
| object（int + NaN） | ❌ `cannot convert float NaN to integer`（**原脚本的 int/bool 存法就是这种**） |

### 第 2 轮（2026-11-11）：全空列与 append

| 用例 | 结果 |
|---|---|
| 全空的 object 列写入 | ✅ |
| object 列：先有值，再 append 全空 | ✅ |
| object 列：先全空，再 append 有值 | ✅ |
| `2^53` 存成 float64 读回 | ✅ 精确 |
| float64：先有值，再 append 全 NaN | ✅ |
| datetime64[ns]：先有值，再 append 全 NaT | ✅ |

### 由此确定的存储约定（各数据源通用）

| 逻辑类型 | 内存中（下游读出） | 存进 xdata |
|---|---|---|
| int | `Int64` | `float64`（绝对值须 ≤ 2^53，超过则该列定为 string） |
| bool | `boolean` | `float64`（1 / 0 / NaN） |
| float | `float64` | `float64` |
| date / datetime | `datetime64[ns]`（带时区的转为 UTC 后去掉时区） | `datetime64[ns]` |
| string / json / 全空 | `string` | object（纯 str + NaN） |

## data_type 与存储粒度（xdata 维护者答复）

来源：她与 xdata 维护者的聊天，2026-10-20，截图 `common/references/xdata_maintainer_chat_20261020.png`。原话要点：
- 数据要足够大时，可以注册成 `second`，**会按天存储**，可以并行写（一个任务写一天，没有等待问题）；
- `tick` 是通过第一次写入的数据自动判断的；
- 按月并行、一个任务写一个月时，选 `minute` 也没有等待问题（维护者答"是的"）。
- **由此可知**（推论，未确认）：`second` ≈ 按天分块，`minute` ≈ 按月分块。
- 她告知（2026-11-11）：存储地址是同一个，分块好像是在底层硬盘上做的；**`daily` 是按年存，读的时候也是按年读**。
- 她告知（2026-11-12）：**xdata 是列式存储**。

## 写入 / 读取耗时（实测）

2026-11-11，`equityNamrV2` 试跑（`bbg_clean.py build --trial 3`），data_type=`daily`，160 列：

| 操作 | 行数 | 用时 |
|---|---|---|
| register + write（第 1 组） | 858,378 | 11s |
| append（第 2 组） | 1,489,887 | 108s |
| append（第 3 组） | 1,492,550 | 184s |
| 读回整张表（`read_bbg`） | 3,840,813 | 62s |

两次 append 行数相近，耗时差很多。3 组都在 2006 年，即同一个年块：第 3 组追加时年块里已有 235 万行，第 2 组时只有 86 万行。**怀疑**追加时要重写所在的年块，所以耗时随年块变大而变长；与"按年存"一致，但仍是推测。读回 62s 读的也是 2006 整个年块。

### 尚未确认（2026-11-11 拟问 xdata 维护者，问题清单见 PROGRESS.md）
- ~~`daily` 的存储方式~~ → 按年存、按年读（她告知）。仍待确认：append 是否重写所在的年块。
- 读取时是否只读涉及的分块（如 `second` 下读一天只读那一天）；读多年时，大量小分块有没有额外开销。
- 列式存储下：只读几列时，是否只读这几列的文件（`read_bbg` 读全部 159 列用了 62s，下游通常只读几列）；在一个分块内能否按日期只读一部分行，还是要读整个分块的这几列；文件是不是按"分块 × 列"存（若是，按天存 160 列、20 年会有约 80 万个文件）。
- `second`（按天）下：一次 write/append 多天数据是否自动按天拆分；两级索引 (date, ID)、date 为零点日期是否可以；多个进程同时 append 同一张表的不同日期是否安全；append 耗时是否还随表变大；读一段日期的速度。
- xdata 的 append 遇到已存在的日期时怎么处理（覆盖？重复？报错？）。目前代码在已有数据时直接拒绝写入。
- 如何删除一个 xdata 路径（要清理 `_trial` 测试表）。
- `register(path, data_type, 'xx', is_public, desc)` 第三个参数的含义。
