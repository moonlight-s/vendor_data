# 进度

每完成一步更新一次。最上面是"现在在哪、下一步做什么"，下面是按时间顺序的日志。
最后更新：2026-11-12

## 现在在哪

- 数据源： Bloomberg
- 数据集：equityNamrV2（Bloomberg 共 6 个数据集中的第 1 个）
- 阶段： 第一阶段。类型推断、全量校验、规模摸底、重复诊断、试跑读回全部完成；写库方式已改为 `second` 按天分块；代码已审查完毕。
- 下一步（只做这一件）： 全量执行 `build`，将 235 个文件正式写入 xdata。命令：`python bbg_clean.py build equityNamrV2 --n-jobs 40`。
- 之后： 写回检查全量数据；接着做其余 5 个数据集（equityNamrCins、equityNamrPricing、3 个 CorporateActions）。

## 总体路线

# | 事项 | 状态
---|---|---
1 | xdata 支持哪些 dtype（实测） | ✅ 完成
2 | Bloomberg equityNamrV2：类型推断 → 人工审阅 → 全量校验 | ✅ 完成
3 | Bloomberg equityNamrV2：数据规模摸底（bbg_inspect） | ✅ 完成
4 | Bloomberg equityNamrV2：重复诊断（diagnose）→ 主键完整的行无冲突；主键为空的 17 行单独存 __nokey（方案 A） | ✅ 完成
5 | Bloomberg equityNamrV2：试跑（build --trial 3，✅ 读回通过；写库耗时从 184s 降到 23s）→ **全量入库（build）**，读回检查 | ⏭ 进行中
6 | Bloomberg 其余 5 个数据集：equityNamrCins、equityNamrPricing、3 个 CorporateActions，各走一遍 2~5 | 待做
7 | 用 GLM 从 Bloomberg 数据说明中摘取字段定义，解决 bbg/PROCESSING.md 中的未确认问题 | 待做
8 | Refinitiv 第一阶段 | 待做
9 | Bloomberg ↔ Refinitiv symbol 对应 | 待做
10 | 第二阶段：筛选与数据内容异常处理 | 待做

## 工作日志

### 2026-11-12

- 拿到 xdata 维护者对 7 个问题的答复（记录在 common/README.md）。
- 试跑（build --trial 3）改用 `second` 存储（按天分块），写库耗时大幅下降（第 2 组从 108s 降至 17s，第 3 组从 184s 降至 23s），彻底解决写库瓶颈。
- 试跑数据读回检查通过：索引正确（`date` 和 `ID_BB_UNIQUE`），主键唯一且无空值；`__nokey` 表 2 行数据正确。
- 为未来不同数据集（或不同数据商）使用不同存储粒度预留扩展性，将 `data_type` 抽离至 `DATASETS` 配置中，当前 6 个 BBG 数据集均默认 `second`。
- 更新 `bbg/PROCESSING.md`：补充“核心前提假设：第一列必须是日期”；补充存储粒度可配置说明。
- 完成全量 build 前的最终代码审查。

### 2026-11-11

- 重写 Bloomberg 第一阶段代码：去掉所有筛选和 univ；类型由历史数据自动推断；新数据入库前做校验。
- xdata dtype 实测两轮，结论见 common/README.md。据此确定了存储格式：int/bool 存成 float64，读回时还原。
- equityNamrV2 试跑推断（前 30 个文件 → 等间隔抽 30 个文件），逐段审阅了 types / na / strings / typed / 单列：
  - COLUMN0 确认是 Bloomberg 请求标识，不是行号 → 不再在去重时忽略它。
  - DVD_FREQ 的 'None' 是取值 → 不当作缺失。
  - 只有空白的字符串、超范围日期 → 定为规则 R2、R3（已确认）。
- equityNamrV2 全量推断：235 个文件、160 列，约 16 分钟。
- equityNamrV2 全量校验通过：160 列在 235 个文件上都能按 schema 转换，约 36 分钟（--n-jobs 10）。
- 建立文档体系：README / PROGRESS / 各目录 PROCESSING。
- 新增 SOURCES.md / references/ 用于登记外部信息来源；完善 .gitignore。
- 仓库定名 vendor_data，位置 /nfs/ssd0_73/gaosangzi/conda_envs/gsz_code/vendor_data；xdata 根目录改为 stock_1d/xdata_test/vendor_data（Bloomberg 在 .../bbg/）。
- equityNamrV2 规模摸底（bbg_inspect，实测）：235 个文件，总行数 214,041,973；每个文件行数 min 35,609 / 中位数 817,495 / max 1,616,880；磁盘合计 45.2 GB；机器 CPU 256 核、内存 2003 GB。
- equityNamrV2 重复诊断（旧版 diagnose --n-jobs 10，共 25.5 分钟）：
  - 235 个文件 → 235 组，没有日期重叠的文件；共 214,041,973 行；
  - 完全重复行 0；
  - 主键冲突只有 3 个主键、7 行，差异列只有 COLUMN0；
  - 但这 7 行的 ID_BB_UNIQUE 都是空的 → 她确认方案 A：主键为空的行原样存进 equityNamrV2__nokey。
- build --trial 3（新版代码，内网实测，总 7m34s）：
  - 计算：858,378 行 59s；1,489,887 行 103s；1,492,550 行 108s；
  - 写库：第 1 组 11s，第 2 组 108s，第 3 组 184s —— 写库远慢于计算，是瓶颈；
  - 读回整张表 62s。
- 试跑读回检查通过：主表 3,840,813 行 × 159 列，主键唯一、无空值、升序，dtype 与 schema 一致；__nokey 2 行。