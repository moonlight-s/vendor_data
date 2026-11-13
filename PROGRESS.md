# 进度

每完成一步更新一次。最上面是"现在在哪、下一步做什么"，下面是按时间顺序的日志。
最后更新：2026-11-13

## 现在在哪

- 数据源： Bloomberg
- 数据集：equityNamrV2（Bloomberg 共 6 个数据集中的第 1 个）
- 阶段： 第一阶段。代码已全面审查并加入严格校验（Fail Fast）原则；准备从源头开始重跑。
- 下一步（只做这一件）： 执行全量推断 `python bbg_schema.py infer equityNamrV2 --n-jobs 40`，生成最新 schema 后跑 `verify`，最后跑全量 `build`。
- 之后： 写回检查全量数据；接着做其余 5 个数据集（equityNamrCins、equityNamrPricing、3 个 CorporateActions）。

## 总体路线

# | 事项 | 状态
---|---|---
1 | xdata 支持哪些 dtype（实测） | ✅ 完成
2 | Bloomberg equityNamrV2：类型推断 → 人工审阅 → 全量校验 | ✅ 完成
3 | Bloomberg equityNamrV2：数据规模摸底（bbg_inspect） | ✅ 完成
4 | Bloomberg equityNamrV2：重复诊断（diagnose） | ✅ 完成
5 | Bloomberg equityNamrV2：试跑通过 → **代码全面审查与加固（Fail Fast）** → **从源头重跑全量** | ⏭ 进行中
6 | Bloomberg 其余 5 个数据集 | 待做
7 | 用 GLM 从 Bloomberg 数据说明中摘取字段定义 | 待做
8 | Refinitiv 第一阶段 | 待做
9 | Bloomberg ↔ Refinitiv symbol 对应 | 待做
10 | 第二阶段：筛选与数据内容异常处理 | 待做

## 工作日志

### 2026-11-13

- 完成全部核心 `.py` 文件的审查，明确了 `bbg_clean.py`、`bbg_schema.py`、`bbg_read.py` 的内部数据流转。
- 修复了 `bbg_read.py` 和 `bbg_clean.py` 中可能悄悄放过野生列的逻辑，严格加入 `raise KeyError`。确立了“严格校验，拒绝静默失败（Fail Fast）”的第一原则。
- 给 `bbg_schema.py` 的 `verify` 增加了 `--start` 增量校验功能。
- 更新了 `README.md`、`bbg/PROCESSING.md`，记录了历史底线维护规则、检查规则和 Fail Fast 原则。
- 清理了旧的 `schema` 目录和测试数据，准备从源头重跑 `infer`。

### 2026-11-12

- 拿到 xdata 维护者对 7 个问题的答复。
- 试跑改用 `second` 存储（按天分块），写库耗时大幅下降。
- 试跑数据读回检查通过。
- 将 `data_type` 抽离至 `DATASETS` 配置中。

### 2026-11-11

- 重写 Bloomberg 第一阶段代码。
- xdata dtype 实测两轮，确定存储格式。
- equityNamrV2 全量推断与校验通过。
- 建立文档体系。
- 重复诊断与方案 A 确认。