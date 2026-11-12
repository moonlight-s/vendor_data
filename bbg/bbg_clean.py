"""
第一阶段：原始 BBG parquet -> 公司内部 xdata。不做任何筛选、不做 univ，只做：
  字段校验 -> 按 schema 强制转换类型 -> 处理重复 -> 入库

用法:
  python bbg_clean.py diagnose equityNamrV2                 # 先看重复是怎么来的（不写库）
  python bbg_clean.py build    equityNamrV2                 # 全量历史入库（目标路径已存在会拒绝）
  python bbg_clean.py build    equityNamrV2 --trial 3       # 试跑：最早 3 组写到 equityNamrV2_trial
  python bbg_clean.py update   equityNamrV2 --start 2026-11-10   # 增量：只处理 DL_SNAPSHOT_DATE >= start 的数据
  （数据集名都可以用 all；--n-jobs 默认 40，--max-buffer 默认 10）

入库内容：
  {PATH_XDATA}/{file_name}        主表，主键唯一；多一列 DUP_N = 原始数据里这个主键有几行（1 = 无冲突）
  {PATH_XDATA}/{file_name}__dup   主键冲突的全部原始行（含被选中的那行，DUP_CHOSEN 标记），没有冲突就不建
  {PATH_XDATA}/{file_name}__nokey 主键（除日期外）任一列为空的行，原样存放，不去重；索引 (date, NOKEY_SEQ)，没有就不建

日志（2026-11-11 起）：logs/{数据集}_{命令}_{时间}.log（进度、用时、内存，人看）+ 同名 .jsonl（build/update 每组明细）。

存储粒度（2026-11-12 更新）：
  根据 xdata 维护者答复（见 common/README.md），改用 data_type='second'，按天分块、可并行写。
  一次 write/append 传入多天的数据，xdata 会自动按天拆开存；多个进程同时 append 不同日期是安全的。
"""

import argparse
import json
import time
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from joblib import Parallel, delayed

from bbg_common import (
    DATASETS,
    DATE_COL,
    EXAMPLES_MAX,
    EXTRA_SPECS,
    get_cli,
    LOG_DIR,
    META_COLS,
    PATH_XDATA,
    REPORT_DIR,
    _fmt_sec,
    _types_mapper,
    cast_series,
    check_values,
    list_files,
    load_schema,
    read_parquet,
    rss_gb,
    run_parallel,
    setup_logger,
    to_storage,
)

N_JOBS = 40  # 2026-11-11 diagnose --n-jobs 10 实测每个 worker 1.5~6 GB（快照），她可用至少 40 个 CPU
MAX_BUFFER = 10  # build/update：正在算的组 + 已算完等着按日期顺序写库的组，合计最多 n_jobs + MAX_BUFFER 个


def save_to_xdata(remote_path, data, desc=""):
    # 只在主进程里写库（原脚本在并行 worker 里各自 append，多个 worker 可能同时看到"不存在"而重复 register，
    # append 顺序也不按日期）。
    # 2026-11-12：改用 second（按天分块），可并行写；维护者确认已存在的表不需要重新 register。
    cli = get_cli()
    if not cli.exists(remote_path):
        cli.register(remote_path, "second", "xx", is_public=False, desc=desc)
        cli.write(remote_path, data)
    else:
        cli.append(remote_path, data)


# ============================== 重复处理 ==============================
def _n_nonnull(d):
    return (
        d.drop(columns=[c for c in META_COLS + ["DUP_N"] if c in d.columns])
        .notna()
        .sum(axis=1)
    )


def _has_px_last(d):
    return (
        d["PX_LAST"].notna().astype(int)
        if "PX_LAST" in d.columns
        else pd.Series(0, index=d.index)
    )


def _comp_match(d):
    if not {"CP_ID_BB_COMP", "ID_BB_COMPANY"} <= set(d.columns):
        return pd.Series(0, index=d.index)
    a = pd.to_numeric(d["CP_ID_BB_COMP"], errors="coerce").astype("float64")
    b = pd.to_numeric(d["ID_BB_COMPANY"], errors="coerce").astype("float64")
    return (a == b).astype(int)


# 越大越优先；row_order 放最后作为兜底（= 原来的 keep='last'，文件里靠后的行优先）
SCORERS = {
    "n_nonnull": _n_nonnull,
    "has_px_last": _has_px_last,
    "comp_match": _comp_match,
    "row_order": lambda d: pd.Series(np.arange(len(d)), index=d.index),
}


def _drop_exact_dups(d):
    cmp_cols = [c for c in d.columns if c not in META_COLS]
    exact = d.duplicated(subset=cmp_cols, keep="first")
    return d[~exact], int(exact.sum())


def resolve_dups(d, key, priority):
    """
    1. 完全相同的行（忽略 META_COLS）只留一行 —— 无信息损失
    2. 主键冲突：所有冲突行原样进 dup 表；主表按 priority 选一行，并标 DUP_N
    不再删列（原脚本一旦有重复就把 COLUMN0 / ID_BB_SEC_NUM_DES 整列删掉，
    导致"有重复的文件"和"没重复的文件"列不一样）。
    """
    rep = {}
    d, rep["n_exact_dup"] = _drop_exact_dups(d)
    g = d.groupby(key, dropna=False, sort=False).ngroup()
    d = d.assign(DUP_N=g.map(g.value_counts()).astype("int64"))
    conf = d["DUP_N"] > 1
    rep["n_conflict_rows"] = int(conf.sum())
    rep["n_conflict_keys"] = int(g[conf].nunique())
    if not conf.any():
        return d, None, rep

    scores = pd.DataFrame({p: SCORERS[p](d) for p in priority}, index=d.index)
    scores["_g"] = g
    best = (
        scores.sort_values(["_g"] + priority, kind="mergesort")
        .groupby("_g")
        .tail(1)
        .index
    )
    chosen = d.index.isin(best)
    dup = d[conf].assign(
        DUP_CHOSEN=chosen[conf.to_numpy()], DUP_SEQ=g[conf].groupby(g[conf]).cumcount()
    )
    return d[chosen], dup, rep


# ============================== 单组文件处理 ==============================
def load_and_cast(file_name, files, schema, start_date=None, allow_new_cols=False):
    """读一组文件 -> 字段检查 -> 强制类型转换 -> 取值范围检查。不改动任何行。"""
    specs, na_tokens = schema["columns"], schema["na_tokens"]
    raw = pd.concat([read_parquet(f) for f in files], ignore_index=True)
    rep = dict(
        files=[Path(f).name for f in files], n_raw=len(raw), errors={}, warnings=[]
    )

    new_cols = [c for c in raw.columns if c not in specs]
    missing = [c for c in specs if c not in raw.columns]
    if new_cols and not allow_new_cols:
        # 新字段：xdata 已有表能否追加新列我无法确认，所以一律拦下，人工决定
        rep["errors"]["new_cols"] = new_cols
    if missing:
        rep["missing_cols"] = missing  # 补成空列，保证每次入库列一致

    out, cast_err = {}, {}
    for c, spec in specs.items():
        s = (
            raw[c]
            if c in raw.columns
            else pd.Series(np.nan, index=raw.index, dtype=object)
        )
        v, bad, oob = cast_series(s, spec, na_tokens)
        if bad.any():
            cast_err[c] = dict(
                type=spec["type"],
                fmt=spec.get("fmt"),
                n=int(bad.sum()),
                examples=[str(x) for x in s[bad].unique()[:EXAMPLES_MAX]],
            )
        if oob.any():  # 超出 pandas 日期范围 -> NaT，原值记进日志
            rep["warnings"].append(
                dict(
                    col=c,
                    kind="date_out_of_range_to_NaT",
                    n=int(oob.sum()),
                    examples=[
                        str(x) for x in s[oob].astype(str).unique()[:EXAMPLES_MAX]
                    ],
                )
            )
        out[c] = v
    if cast_err:
        rep["errors"]["cast"] = cast_err
    d = pd.DataFrame(out, index=raw.index)

    if d[DATE_COL].isna().any():
        rep["errors"]["null_snapshot_date"] = int(d[DATE_COL].isna().sum())
    if start_date is not None:
        d = d[d[DATE_COL] >= start_date]
    rep["n_rows"] = len(d)

    for c, spec in specs.items():
        if c != DATE_COL:  # 快照日期增量时必然超出历史最大值，不提示
            rep["warnings"] += check_values(d[c], spec, c)
    return d, rep


def to_xdata_frame(d, specs, index_cols):
    d = d.copy()
    for c in d.columns:
        spec = specs.get(c) or EXTRA_SPECS.get(c)
        if spec:
            d[c] = to_storage(d[c], spec, c)
    d = d.set_index(index_cols).sort_index()
    d.index = d.index.set_names(["date"] + index_cols[1:])
    return d


def split_nokey(d, key):
    """
    主键（除日期外）任一列为空的行单独拿出来，原样存进 __nokey 表（2026-11-11 她确认，方案 A）。
    依据：equityNamrV2 全量有 17 行 ID_BB_UNIQUE 为空，只有 DL_DATASET / 日期 / START_TIME / END_TIME /
    COLUMN0 / RC 有值，是不同证券的请求记录；若参与去重会被误当成主键冲突。日期为空的行在 load_and_cast 里已报错。
    索引 (date, NOKEY_SEQ)：NOKEY_SEQ = 同一天内的序号（按原始行顺序）。
    """
    m = d[key[1:]].isna().any(axis=1)
    if not m.any():
        return d, None
    nk = d[m]
    nk = nk.assign(NOKEY_SEQ=nk.groupby(DATE_COL, sort=False).cumcount())
    return d[~m], nk


def process_group(file_name, files, schema, start_date=None):
    cfg = DATASETS[file_name]
    d, rep = load_and_cast(file_name, files, schema, start_date)
    if rep["errors"] or d.empty:
        return None, None, None, rep
    d, nokey = split_nokey(d, cfg["key"])
    rep["n_nokey"] = 0 if nokey is None else len(nokey)
    d, dup, dup_rep = resolve_dups(d, cfg["key"], cfg["dup_priority"])
    rep.update(dup_rep)
    specs = schema["columns"]
    d = to_xdata_frame(d, specs, cfg["key"])
    if dup is not None:
        dup = to_xdata_frame(dup, specs, cfg["key"] + ["DUP_SEQ"])
    if nokey is not None:
        nokey = to_xdata_frame(nokey, specs, [DATE_COL, "NOKEY_SEQ"])
    return d, dup, nokey, rep


# ============================== 文件分组 ==============================
def _date_range(f, spec, na_tokens):
    """
    一个文件的 DL_SNAPSHOT_DATE 范围。只解析去重后的取值（每个文件只有几十个不同日期）。
    2026-11-11：原来逐行解析，diagnose --n-jobs 10 实测这一步用了约 7.5 分钟（235 个文件、2 亿多行）。
    """
    col = pq.ParquetFile(f).read(columns=[DATE_COL]).column(0)
    u = pc.unique(col)
    s = pa.table({DATE_COL: u}).to_pandas(types_mapper=_types_mapper)[DATE_COL]
    v, bad, _ = cast_series(s, spec, na_tokens)
    return f, v.min(), v.max(), int(bad.sum())


def plan_groups(file_name, files, schema, n_jobs, log):
    """
    先只读 DL_SNAPSHOT_DATE，得到每个文件的日期范围，按日期排序；
    日期范围有重叠的文件合成一组一起处理（否则跨文件的重复永远查不到 —— 原脚本是逐文件去重的）。
    """
    t0 = time.time()
    spec, na_tokens = schema["columns"][DATE_COL], schema["na_tokens"]
    log.info(f"[分组] 读取 {len(files)} 个文件的 {DATE_COL} ...")
    rngs = Parallel(n_jobs=n_jobs, prefer="threads")(
        delayed(_date_range)(f, spec, na_tokens) for f in files
    )
    bad = [(Path(f).name, nb) for f, _, _, nb in rngs if nb]
    if bad:  # 入库时同样的值会在类型转换里报错、不入库；这里先提示
        log.warning(
            f"[分组] 这些文件的 {DATE_COL} 有无法解析的取值（不同取值个数）：{bad[:EXAMPLES_MAX]}"
        )
    empty = [Path(f).name for f, lo, _, _ in rngs if pd.isna(lo)]
    if empty:
        raise RuntimeError(
            f"这些文件 {DATE_COL} 全部为空或无法解析，请先检查：{empty[:EXAMPLES_MAX]}"
        )
    rngs = [r[:3] for r in rngs]
    rngs.sort(key=lambda x: (x[1], x[2], x[0]))
    groups, cur, cur_hi = [], [], None
    for f, lo, hi in rngs:
        if cur and lo <= cur_hi:
            cur.append(f)
            cur_hi = max(cur_hi, hi)
        else:
            if cur:
                groups.append((cur, cur_hi))
            cur, cur_hi = [f], hi
    groups.append((cur, cur_hi))
    multi = [g for g, _ in groups if len(g) > 1]
    log.info(
        f"[分组] {len(files)} 个文件 -> {len(groups)} 组，用时 {_fmt_sec(time.time() - t0)}"
    )
    if multi:
        log.info(
            f"[分组] 注意：{len(multi)} 组文件日期有重叠，已合并处理（最多 {max(map(len, multi))} 个文件一组），"
            f"例如 {[Path(f).name for f in multi[0]]}"
        )
    return groups


# ============================== 运行 ==============================
def _group_desc(res):
    rep = res[-1]
    fs = rep["files"]
    name = fs[0] if len(fs) == 1 else f"{fs[0]} 等 {len(fs)} 个文件"
    return f'{name}，{rep.get("n_rows", 0):,} 行' + (
        "，有错误" if rep["errors"] else ""
    )


def _run(
    file_name,
    groups,
    schema,
    n_jobs,
    max_buffer,
    log,
    ts,
    start_date=None,
    mode="build",
    path=None,
):
    path = path or f"{PATH_XDATA}/{file_name}"
    jsonl_path = LOG_DIR / f"{file_name}_{mode}_{ts}.jsonl"
    totals, warn_cnt = Counter(), Counter()
    n, t0, t_write = len(groups), time.time(), 0.0
    args = [(file_name, g, schema, start_date) for g in groups]
    with open(jsonl_path, "w", encoding="utf-8") as jl:
        # 并行只做读取+转换+去重；写库在主进程里按日期顺序做（run_parallel ordered=True 保证顺序）
        for i, (d, dup, nokey, rep) in run_parallel(
            process_group,
            args,
            n_jobs,
            log,
            f"{mode} 计算",
            ordered=True,
            max_buffer=max_buffer,
            describe=_group_desc,
        ):
            jl.write(json.dumps(rep, ensure_ascii=False, default=str) + "\n")
            jl.flush()
            if rep["errors"]:
                log.error(
                    f'第 {i + 1}/{n} 组 {rep["files"]} 校验失败，未写入，停止。'
                    f"前 {i} 组已写入 xdata。完整明细 {jsonl_path}"
                )
                raise RuntimeError(
                    f'{rep["files"]} 校验失败：'
                    f'{json.dumps(rep["errors"], ensure_ascii=False, default=str)[:2000]}'
                )
            for w in rep["warnings"]:
                warn_cnt[(w["col"], w["kind"])] += w["n"]
            for k in (
                "n_raw",
                "n_rows",
                "n_nokey",
                "n_exact_dup",
                "n_conflict_keys",
                "n_conflict_rows",
            ):
                totals[k] += rep.get(k, 0)
            tw = time.time()
            if d is not None and not d.empty:
                save_to_xdata(path, d)
            if dup is not None and not dup.empty:
                save_to_xdata(path + "__dup", dup)
            if nokey is not None and not nokey.empty:
                save_to_xdata(path + "__nokey", nokey)
            tw = time.time() - tw
            t_write += tw
            el = time.time() - t0
            log.info(
                f'[写入 {i + 1}/{n}] {_group_desc((rep,))}，主键为空 {rep.get("n_nokey", 0):,}，'
                f'冲突主键 {rep.get("n_conflict_keys", 0):,} | '
                f"写库 {tw:.0f}s（累计 {_fmt_sec(t_write)}）| 主进程内存 {rss_gb():.1f} GB | "
                f"已用 {_fmt_sec(el)}，预计剩余 {_fmt_sec(el / (i + 1) * (n - i - 1))}"
            )
    log.info(f"[{file_name}] 汇总 {dict(totals)}")
    if warn_cnt:
        log.warning(f"[{file_name}] 取值超出历史范围的提示（不阻止入库，请检查）：")
        for (c, k), cnt in warn_cnt.most_common(50):
            log.warning(f"  {c:40s} {k:24s} {cnt}")
    log.info(f"每组明细 -> {jsonl_path}")


def build(file_name, n_jobs=N_JOBS, max_buffer=MAX_BUFFER, trial=None):
    """trial=N：只处理最早的 N 组，写到 {file_name}_trial（试跑真实 xdata 用，正式表不受影响）。"""
    t0 = time.time()
    log, ts = setup_logger(file_name, "build_trial" if trial else "build")
    log.info(
        f"build {file_name}：n_jobs={n_jobs}, max_buffer={max_buffer}, trial={trial}"
    )
    cli = get_cli()
    path = f"{PATH_XDATA}/{file_name}" + ("_trial" if trial else "")
    for p in (path, path + "__dup", path + "__nokey"):
        if cli.exists(p):
            raise RuntimeError(
                f"{p} 已存在。全量重建会和已有数据叠加，请先确认并手动删除/改名，或用 update"
            )
    schema = load_schema(file_name)
    groups = [
        g for g, _ in plan_groups(file_name, list_files(file_name), schema, n_jobs, log)
    ]
    if trial:
        groups = groups[:trial]
        log.info(f"试跑：只处理最早的 {len(groups)} 组，写到 {path}")
    _run(
        file_name,
        groups,
        schema,
        n_jobs,
        max_buffer,
        log,
        ts,
        mode="build_trial" if trial else "build",
        path=path,
    )
    log.info(f"build {file_name} 完成，总用时 {_fmt_sec(time.time() - t0)}")


def update(file_name, start_date, n_jobs=N_JOBS, max_buffer=MAX_BUFFER):
    """
    增量。start_date 应为库里最后一天之后的日期；这里会先查库里 >= start_date 是否已有数据，有就拒绝，
    因为 xdata 的 append 遇到重复日期怎么处理我无法确认。
    新数据会：按历史 schema 转换（转不过去 -> 报错不入库）；出现新字段 -> 报错不入库；
    取值超出历史范围/出现新类别 -> 打印提示，照常入库。
    """
    t0 = time.time()
    log, ts = setup_logger(file_name, "update")
    log.info(
        f"update {file_name}：start={start_date}, n_jobs={n_jobs}, max_buffer={max_buffer}"
    )
    start = pd.Timestamp(start_date)
    schema = load_schema(file_name)
    cfg = DATASETS[file_name]
    groups = [
        (g, hi)
        for g, hi in plan_groups(file_name, list_files(file_name), schema, n_jobs, log)
        if hi >= start
    ]
    if not groups:
        log.info(f"[{file_name}] 没有 {DATE_COL} >= {start.date()} 的文件")
        return
    end = max(hi for _, hi in groups)
    path = f"{PATH_XDATA}/{file_name}"
    probe_col = next(c for c in schema["columns"] if c not in cfg["key"])
    cli = get_cli()
    if cli.exists(path):
        existing = cli.read(path, columns=[probe_col], start=start, end=end)
        if len(existing):
            raise RuntimeError(
                f"{path} 在 {start.date()} ~ {end.date()} 已有 {len(existing)} 行，拒绝重复写入"
            )
    _run(
        file_name,
        [g for g, _ in groups],
        schema,
        n_jobs,
        max_buffer,
        log,
        ts,
        start,
        mode="update",
    )
    log.info(f"update {file_name} 完成，总用时 {_fmt_sec(time.time() - t0)}")


# ============================== 重复诊断 ==============================
def _diagnose_group(file_name, files, schema):
    key = DATASETS[file_name]["key"]
    d, rep = load_and_cast(file_name, files, schema, allow_new_cols=True)
    if "cast" in rep["errors"]:
        return None
    d, nokey = split_nokey(d, key)  # 与入库一致：主键为空的行不参与去重
    n_nokey = 0 if nokey is None else len(nokey)
    d, n_exact = _drop_exact_dups(d)
    conf = d.duplicated(subset=key, keep=False)
    res = dict(
        n_rows=len(d) + n_nokey,
        n_nokey=n_nokey,
        n_exact=n_exact,
        n_conf_rows=int(conf.sum()),
        diff_cols=Counter(),
        patterns=Counter(),
        sample=None,
    )
    if not conf.any():
        return res
    sub = d[conf]
    g = sub.groupby(key, dropna=False, sort=False).ngroup()
    differ = (
        sub.drop(columns=key).groupby(g).nunique(dropna=False) > 1
    )  # 每个冲突主键 × 每列：取值是否不同
    res["n_conf_keys"] = len(differ)
    res["diff_cols"] = Counter(differ.sum().loc[lambda x: x > 0].to_dict())
    res["patterns"] = Counter(differ.apply(lambda r: "|".join(r.index[r]), axis=1))
    first_keys = g.drop_duplicates().iloc[:5].to_numpy()
    show_cols = key + list(differ.columns[differ.any()])
    res["sample"] = sub.loc[g.isin(first_keys), show_cols].assign(
        _files=",".join(rep["files"])
    )
    return res


def _diag_desc(r):
    if r is None:
        return "类型转换失败，跳过"
    return f'{r["n_rows"]:,} 行，主键为空 {r["n_nokey"]:,}，完全重复 {r["n_exact"]:,}，冲突行 {r["n_conf_rows"]:,}'


def diagnose(file_name, n_jobs=N_JOBS, limit_groups=None):
    """
    不写库。统计：完全重复行有多少；主键冲突的行，是哪些列不一样、最常见的"差异列组合"是什么。
    看完再决定 DATASETS 里的 dup_priority（或者某些列要不要并入主键）。
    """
    t0 = time.time()
    log, _ = setup_logger(file_name, "diagnose")
    log.info(f"diagnose {file_name}：n_jobs={n_jobs}, limit_groups={limit_groups}")
    schema = load_schema(file_name)
    groups = [
        g for g, _ in plan_groups(file_name, list_files(file_name), schema, n_jobs, log)
    ]
    if limit_groups:
        groups = groups[:limit_groups]
    args = [(file_name, g, schema) for g in groups]
    results = [
        r
        for _, r in run_parallel(
            _diagnose_group, args, n_jobs, log, "diagnose", describe=_diag_desc
        )
    ]
    skipped = sum(r is None for r in results)
    results = [r for r in results if r is not None]
    tot = Counter()
    diff_cols, patterns, samples = Counter(), Counter(), []
    for r in results:
        for k in ("n_rows", "n_nokey", "n_exact", "n_conf_rows", "n_conf_keys"):
            tot[k] += r.get(k, 0)
        tot["groups_with_conflict"] += r["n_conf_rows"] > 0
        diff_cols.update(r["diff_cols"])
        patterns.update(r["patterns"])
        if r["sample"] is not None and len(samples) < 20:
            samples.append(r["sample"])
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    base = REPORT_DIR / f"{file_name}_dup"
    pd.Series(diff_cols, name="n_conflict_keys", dtype="int64").sort_values(
        ascending=False
    ).rename_axis("col").to_csv(f"{base}_cols.csv", encoding="utf-8-sig")
    pd.Series(patterns, name="n_conflict_keys", dtype="int64").sort_values(
        ascending=False
    ).rename_axis("differing_cols").to_csv(f"{base}_patterns.csv", encoding="utf-8-sig")
    if samples:
        pd.concat(samples).to_csv(
            f"{base}_samples.csv", index=False, encoding="utf-8-sig"
        )
    log.info(
        f"[{file_name}] {len(groups)} 组文件，{dict(tot)}"
        + (f"，{skipped} 组因类型转换失败跳过" if skipped else "")
    )
    if patterns:
        log.info("最常见的差异列组合：")
        for p, n in patterns.most_common(15):
            log.info(f"  {n:8d}  {p}")
    log.info(f"report -> {base}_cols.csv / _patterns.csv / _samples.csv")
    log.info(f"diagnose {file_name} 完成，总用时 {_fmt_sec(time.time() - t0)}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["build", "update", "diagnose"])
    ap.add_argument("file_name", help="数据集名，或 'all'")
    ap.add_argument(
        "--start", help="update 用：只处理 DL_SNAPSHOT_DATE >= start 的数据"
    )
    ap.add_argument("--n-jobs", type=int, default=N_JOBS)
    ap.add_argument(
        "--max-buffer",
        type=int,
        default=MAX_BUFFER,
        help="build/update 用：正在算的组 + 已算完等着写库的组，合计最多 n_jobs + max_buffer 个（越大越不容易让 worker 空等，越占主进程内存）",
    )
    ap.add_argument(
        "--limit-groups", type=int, default=None, help="diagnose 用：只看前 N 组"
    )
    ap.add_argument(
        "--trial",
        type=int,
        default=None,
        help="build 用：只处理最早的 N 组，写到 {数据集}_trial",
    )
    a = ap.parse_args()
    names = list(DATASETS) if a.file_name == "all" else [a.file_name]
    for name in names:
        if a.cmd == "build":
            build(name, a.n_jobs, a.max_buffer, a.trial)
        elif a.cmd == "update":
            assert a.start, "update 需要 --start"
            update(name, a.start, a.n_jobs, a.max_buffer)
        else:
            diagnose(name, a.n_jobs, a.limit_groups)
