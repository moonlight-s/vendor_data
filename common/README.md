# common：与数据源无关的工具与结论

最后更新：2026-11-12

## xdata 支持的 dtype（实测）

脚本：xdata_dtype_test.py。只写测试路径；2026-11-11 的两轮实测在旧路径 stock_1d/xdata_test/bbg/_dtype_test/ 下跑，现在脚本里已改为 stock_1d/xdata_test/vendor_data/_dtype_test/。旧路径下的测试数据可以删除。

### 第 1 轮（2026-11-11）：各类型能否原样写入、读回

dtype | 结果
---|---
float64 | ✅
datetime64[ns] | ✅
object（纯 str + NaN） | ✅
int64（无缺失） | ✅
bool（无缺失） | ✅
Int64（可空整数） | ❌ cannot convert NA to integer；xdata 提示应改用 int64 或 float64
boolean（可空布尔） | ❌ 'BooleanDtype' object has no attribute 'str'
pandas string | ❌ decoded error, may be mixed object type, it should be pure string type
object（int + NaN） | ❌ cannot convert float NaN to integer（原脚本的 int/bool 存法就是这种）

### 第 2 轮（2026-11-11）：全空列与 append

用例 | 结果
---|---
全空的 object 列写入 | ✅
object 列：先有值，再 append 全空 | ✅
object 列：先全空，再 append 有值 | ✅
2^53 存成 float64 读回 | ✅ 精确
float64：先有值，再 append 全 NaN | ✅
datetime64[ns]：先有值，再 append 全 NaT | ✅

### 由此确定的存储约定（各数据源通用）

逻辑类型 | 内存中（下游读出） | 存进 xdata
---|---|---
int | Int64 | float64（绝对值须 ≤ 2^53，超过则该列定为 string）
bool | boolean | float64（1 / 0 / NaN）
float | float64 | float64
date / datetime | datetime64[ns]（带时区的转为 UTC 后去掉时区） | datetime64[ns]
string / json / 全空 | string | object（纯 str + NaN）

## data_type 与存储粒度（xdata 维护者答复，2026-11-12 更新）

来源：她与 xdata 维护者的聊天记录（2026-10-20 及后续）。维护者对写库问题的明确答复：

1. **append 会重写所在的整个年块**（证实了 daily 追加慢的推测，因此放弃 daily）。
2. **改用 second 会一天一个分块存**。
3. **多个进程同时往同一张表 append 不同日期的数据，是安全的**。
4. **已经存在的表不需要重新 register**（代码里已改为先 `exists` 判断，不存在才 register，否则直接 append）。
5. **不允许代码直接删除 xdata 路径**；需要清理测试表时，由她手动删除。
6. **register 的第三个参数是 desc 备注**（说明数据），可以不写。
7. **列式存储可以按列读**（时间分块 × 列）。

- 由此确定（2026-11-12 起）：BBG 第一阶段入库的 `data_type` 改为 `second`。一次 write/append 传入多天数据会自动按天拆开；索引仍为 (date, ID) 两级，date 为日期（零点）。
- 待试跑验证：`second` 下按天拆分后，读回、append 的耗时是否如预期（不再随表变大而线性变慢）。

## 写入 / 读取耗时（实测）

2026-11-11，equityNamrV2 试跑（bbg_clean.py build --trial 3），data_type=daily，160 列：

操作 | 行数 | 用时
---|---|---
register + write（第 1 组） | 858,378 | 11s
append（第 2 组） | 1,489,887 | 108s
append（第 3 组） | 1,492,550 | 184s
读回整张表（read_bbg） | 3,840,813 | 62s

两次 append 行数相近，耗时差很多。3 组都在 2006 年，即同一个年块：第 3 组追加时年块里已有 235 万行，第 2 组时只有 86 万行。**维护者已确认 daily 追加要重写整个年块**，所以耗时随年块变大而变长；改用 `second` 后预期不再有这个问题（待新试跑验证）。

## 尚未确认

- `second` 下读多年数据时，大量按天的小分块有没有额外读取开销。
- 在一个分块内能否按日期只读一部分行，还是要读整个分块的这几列（目前已知“按列读”是支持的）。
- xdata 的 append 遇到**已存在的日期**时怎么处理（覆盖？重复？报错？）。目前代码在 update 前会先查重并拒绝写入。