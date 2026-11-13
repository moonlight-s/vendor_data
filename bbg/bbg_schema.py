"""
用历史数据推断每个字段的类型，并在全量历史上校验能否强制转换。

用法:
  python bbg_schema.py infer  equityNamrV2            # 推断 -> schema/inferred/equityNamrV2.json + reports/*_infer.csv
  python bbg_schema.py infer  equityNamrV2 --limit-files 30   # 先用少量文件试跑
  python bbg_schema.py verify equityNamrV2            # 用 inferred + overrides 在全部历史文件上逐列强制转换，报告失败值
  python bbg_schema.py verify equityNamrV2 --start 2026-11-01 # 增量校验：只校验日期 >= 2026-11-01 的文件
  python bbg_schema.py infer  all  /  verify all

内存：每个任务只负责一小批列（--cols-per-task），逐个文件只读这几列；
推断时只对每个文件里这几列的"去重后取值"做判定，不会把整列历史拼到内存里。
"""

import argparse
import re
import time
from collections import Counter

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from joblib import Parallel, delayed
from tqdm import tqdm

NONNUM_MAX = 20  # 非数字取值超过这么多种，就不再记录（肯定是文本列）

from bbg_common import (
    CANDIDATE_ORDER,
    BOOL_MAP,
    DATASETS,
    DATE_FMTS,
    DATE_NAME_RE,
    ENUM_MAX,
    EXAMPLES_MAX,
    INT_RE,
    ISO_DT_RE,
    LEAD0_RE,
    LONG_DIGITS_RE,
    NA_TOKENS,
    EXACT_INT_MAX,
    REPORT_DIR,
    TZ_SUFFIX_RE,
    DATE_COL,
    cast_series,
    check_values,
    date_oob,
    list_files,
    to_ns_range,
    load_schema,
    read_parquet,
    save_json,
    schema_path,
    _types_mapper,
)


# ============================== 扫描文件结构 ==============================
def _subset(files, limit_files=None, sample_files=None):
    """limit_files: 只取最早的 N 个；sample_files: 在整段历史上等间隔抽 N 个（试跑时更有代表性）"""
    if sample_files and sample_files < len(files):
        idx = sorted(
            set(int(round(i)) for i in np.linspace(0, len(files) - 1, sample_files))
        )
        files = [files[i] for i in idx]
    elif limit_files:
        files = files[:limit_files]
    return files


def _filter_by_start(files, schema, start_date):
    """增量校验专用：只保留日期范围包含 start_date 之后数据的文件。"""
    start = pd.Timestamp(start_date)
    spec, na_tokens = schema["columns"][DATE_COL], schema["na_tokens"]

    def _check(f):
        col = pq.ParquetFile(f).read(columns=[DATE_COL]).column(0)
        u = pc.unique(col)
        s = pa.table({DATE_COL: u}).to_pandas(types_mapper=_types_mapper)[DATE_COL]
        v, _, _ = cast_series(s, spec, na_tokens)
        return v.max() >= start

    keep = Parallel(n_jobs=10, prefer="threads")(
        delayed(_check)(f) for f in tqdm(files, desc="筛选增量文件")
    )
    return [f for f, k in zip(files, keep) if k]


def scan_columns(files, n_jobs):
    """读所有文件的 schema（只读 footer），得到列的并集（按首次出现顺序）和每个文件有哪些列。"""
    schemas = Parallel(n_jobs=n_jobs, prefer="threads")(
        delayed(pq.read_schema)(f) for f in files
    )
    col_order, seen, file_cols = [], set(), []
    for sch in schemas:
        names = sch.names
        file_cols.append(set(names))
        for c in names:
            if c not in seen:
                seen.add(c)
                col_order.append(c)
    return col_order, file_cols


def _plans(col_order, files, file_cols, cols_per_task):
    """每个任务：一小批列 + [(文件, 该文件里存在的这批列)]"""
    plans = []
    for i in range(0, len(col_order), cols_per_task):
        batch = col_order[i : i + cols_per_task]
        plan = [(f, [c for c in batch if c in fc]) for f, fc in zip(files, file_cols)]
        plans.append((batch, [(f, cs) for f, cs in plan if cs]))
    return plans


# ============================== 推断 ==============================
def _kind(t):
    if pa.types.is_null(t):
        return "null"
    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return "string"
    if pa.types.is_boolean(t):
        return "bool"
    if pa.types.is_integer(t):
        return "int"
    if pa.types.is_floating(t):
        return "float"
    if pa.types.is_timestamp(t) or pa.types.is_date(t):
        return "temporal"
    if pa.types.is_dictionary(t):
        return "dict"
    if pa.types.is_nested(t):  # list / struct / map
        return "nested"
    return "other"  # decimal / time / binary ...，下面尝试转字符串


def _new_stats():
    return dict(
        n_rows=0,
        n_null=0,
        n_na_token=0,
        n_files=0,
        kinds=set(),
        arrow_types=set(),
        alive=list(CANDIDATE_ORDER),
        eliminated={},
        uniques=set(),
        na_counts=Counter(),
        nonnum=set(),
        n_num_uniq=0,
        int_rng=None,
        float_rng=None,
        dt_rng={},
        dt_oob={},
        len_rng=None,
        tz=set(),
    )


def _upd_rng(old, lo, hi):
    return (lo, hi) if old is None else (min(old[0], lo), max(old[1], hi))


def _unique_strings(arr, kind, st, na_set):
    """把一个文件里某列的去重非空值转成字符串 Series，统一走字符串判定规则。"""
    if kind == "string":
        if pa.types.is_large_string(arr.type):
            arr = arr.cast(pa.string())
        is_tok = pc.or_kleene(
            pc.is_in(arr, value_set=na_set), pc.utf8_is_space(arr)
        )  # 纯空白也算缺失
        n_tok = int(pc.sum(is_tok).as_py() or 0)
        if n_tok:
            st["n_na_token"] += n_tok
            for x in pc.value_counts(arr.filter(is_tok)).to_pylist():
                k = x["values"] if x["values"] in NA_TOKENS else "<空白>"
                st["na_counts"][k] += x["counts"]
        u = pc.unique(arr)
        tok_u = pc.or_kleene(pc.is_in(u, value_set=na_set), pc.utf8_is_space(u))
        u = u.filter(pc.and_kleene(pc.is_valid(u), pc.invert(tok_u)))
        return pd.Series(u.to_pylist(), dtype=object)
    if kind == "float":
        st["n_null"] += int(pc.sum(pc.is_nan(arr)).as_py() or 0)
        u = pc.unique(arr)
        u = u.filter(pc.and_(pc.is_valid(u), pc.invert(pc.is_nan(u))))
        return pd.Series([str(x) for x in u.to_pylist()], dtype=object)
    if kind in ("int", "bool"):
        u = pc.drop_null(pc.unique(arr))
        return pd.Series([str(x) for x in u.to_pylist()], dtype=object)
    if kind == "temporal":
        u = pd.Series(pc.drop_null(pc.unique(arr)).to_pandas())
        return pd.Series([x.isoformat() for x in u], dtype=object)
    raise ValueError(kind)


def _date_cand(cand, s, parsed, oob, st, kill):
    """日期候选：超出 pandas 范围的值不淘汰该格式，只记录；其它解析失败才淘汰。返回是否仍存活。"""
    bad = parsed.isna() & ~oob
    if bad.any():
        kill(cand, s[bad])
        return False
    if oob.any():
        ex = st["dt_oob"].setdefault(cand, [])
        ex += [x for x in s[oob].tolist() if x not in ex][: EXAMPLES_MAX - len(ex)]
    ok = parsed.dropna()
    if len(ok):
        st["dt_rng"][cand] = _upd_rng(st["dt_rng"].get(cand), ok.min(), ok.max())
    return True


def _update_candidates(s, st):
    if s.empty:
        return
    alive = st["alive"]

    def kill(cand, bad):
        alive.remove(cand)
        st["eliminated"][cand] = [str(x) for x in bad.iloc[:EXAMPLES_MAX]]

    if st["uniques"] is not None:
        st["uniques"].update(s.tolist())
        if len(st["uniques"]) > ENUM_MAX:
            st["uniques"] = None
    L = s.str.len()
    st["len_rng"] = _upd_rng(st["len_rng"], int(L.min()), int(L.max()))

    if st["nonnum"] is not None:
        isnum = pd.to_numeric(s, errors="coerce").notna()
        st["n_num_uniq"] += int(isnum.sum())
        st["nonnum"].update(s[~isnum].tolist())
        if len(st["nonnum"]) > NONNUM_MAX:
            st["nonnum"] = None
    lead0 = s.str.match(LEAD0_RE, na=False)
    if "bool" in alive:
        ok = s.str.upper().isin(list(BOOL_MAP))
        if not ok.all():
            kill("bool", s[~ok])
    if "int" in alive:
        ok = s.str.fullmatch(INT_RE, na=False) & ~lead0
        if not ok.all():
            kill("int", s[~ok])
        else:
            v = s.map(int)
            st["int_rng"] = _upd_rng(st["int_rng"], int(v.min()), int(v.max()))
    if "float" in alive:
        num = pd.to_numeric(s, errors="coerce")
        ok = num.notna() & ~lead0 & ~s.str.fullmatch(LONG_DIGITS_RE, na=False)
        if not ok.all():
            kill("float", s[~ok])
        else:
            st["float_rng"] = _upd_rng(
                st["float_rng"], float(num.min()), float(num.max())
            )
    for fmt, rx in DATE_FMTS.items():
        cand = f"date|{fmt}"
        if cand in alive:
            m = s.str.fullmatch(rx, na=False)
            parsed = to_ns_range(
                pd.to_datetime(s.where(m), format=fmt, errors="coerce")
            )
            _date_cand(cand, s, parsed, date_oob(s, fmt, parsed), st, kill)
    cand = "datetime|ISO8601"
    if cand in alive:
        m = s.str.fullmatch(ISO_DT_RE, na=False)
        parsed = to_ns_range(
            pd.to_datetime(
                s.where(m), format="ISO8601", errors="coerce", utc=True
            ).dt.tz_localize(None)
        )
        if _date_cand(cand, s, parsed, date_oob(s, "ISO8601", parsed), st, kill):
            st["tz"].update(s.str.fullmatch(TZ_SUFFIX_RE, na=False).unique().tolist())


def _finalize(col, st):
    non_null = st["n_rows"] - st["n_null"] - st["n_na_token"]
    kinds = st["kinds"] - {"null"}
    spec = dict(
        type=None,
        fmt=None,
        enum=None,
        min=None,
        max=None,
        decided_by="auto",
        note="",
        n_files=st["n_files"],
        n_rows=st["n_rows"],
        null_ratio=round(1 - non_null / st["n_rows"], 6) if st["n_rows"] else 1.0,
        n_na_token=st["n_na_token"],
        na_token_counts=dict(st["na_counts"]),
        n_num_uniq=st["n_num_uniq"],
        nonnum_values=(
            sorted(st["nonnum"]) if st["nonnum"] is not None else f">{NONNUM_MAX}"
        ),
        n_unique=len(st["uniques"]) if st["uniques"] is not None else f">{ENUM_MAX}",
        arrow_types=sorted(st["arrow_types"]),
        eliminated=st["eliminated"],
    )

    if "nested" in kinds:
        spec["type"] = "json" if kinds == {"nested"} else "string"
        if kinds != {"nested"}:
            spec["note"] = (
                f"不同文件里 arrow 类型不一致: {sorted(kinds)}，按 string 处理，请检查"
            )
        return spec
    if non_null <= 0:
        spec["type"] = "empty"
        spec["note"] = "历史全部为空"
        return spec

    alive = st["alive"]
    pick = next((c for c in CANDIDATE_ORDER if c in alive), None)
    if pick == "int" and DATE_NAME_RE.search(col):
        d = next(
            (c for c in CANDIDATE_ORDER if c.startswith("date|") and c in alive), None
        )
        if d:
            pick = d
            spec["decided_by"] = "name_rule"
            spec["note"] = (
                f'取值既是整数也都能按 {d.split("|")[1]} 解析成日期，因字段名含 DT/DATE 判为日期，请确认'
            )

    if pick is None:
        spec["type"] = "string"
        if st["len_rng"]:
            spec["min_len"], spec["max_len"] = st["len_rng"]
    elif pick in ("bool", "int", "float"):
        spec["type"] = pick
        rng = (
            st["int_rng"]
            if pick == "int"
            else st["float_rng"] if pick == "float" else None
        )
        if rng:
            spec["min"], spec["max"] = rng
        if pick == "int" and rng and max(abs(rng[0]), abs(rng[1])) > EXACT_INT_MAX:
            spec["type"], spec["min"], spec["max"] = "string", None, None
            spec["note"] = "整数超过 2^53，xdata 只能存 float64 会丢精度，判为 string"
    else:
        spec["type"], spec["fmt"] = pick.split("|")
        if pick in st["dt_rng"]:
            lo, hi = st["dt_rng"][pick]
            spec["min"], spec["max"] = lo.isoformat(), hi.isoformat()
        if st["dt_oob"].get(pick):
            spec["oob_examples"] = st["dt_oob"][pick]
            spec["note"] = (
                f"有超出 pandas 日期范围(1677~2262)的值，入库时置为 NaT 并记入日志，"
                f'例如 {st["dt_oob"][pick][:3]}'
            )
        if spec["type"] == "datetime":
            spec["tz_aware"] = (
                True
                if st["tz"] == {True}
                else False if st["tz"] == {False} else "mixed"
            )
            if spec["tz_aware"] == "mixed":
                spec["note"] = "有的值带时区有的不带；不带时区的会被当作 UTC，请确认"

    u = st["uniques"]
    if u is not None and spec["type"] in ("string", "int"):
        spec["enum"] = sorted(int(x) for x in u) if spec["type"] == "int" else sorted(u)
    return spec


def _infer_task(batch, plan, na_tokens):
    stats = {c: _new_stats() for c in batch}
    na_set = pa.array(na_tokens, pa.string())
    for f, cols in plan:
        tbl = pq.ParquetFile(f).read(columns=cols)
        for c in cols:
            st, arr = stats[c], tbl.column(c)
            st["n_files"] += 1
            st["n_rows"] += len(arr)
            st["n_null"] += arr.null_count
            st["arrow_types"].add(str(arr.type))
            kind = _kind(arr.type)
            if kind == "dict":
                arr = pa.chunked_array(
                    [ch.dictionary_decode() for ch in arr.chunks],
                    type=arr.type.value_type,
                )
                kind = _kind(arr.type)
            if kind == "other":
                try:
                    arr, kind = arr.cast(pa.string()), "string"
                except (pa.ArrowInvalid, pa.ArrowNotImplementedError):
                    kind = "nested"
            st["kinds"].add(kind)
            if kind in ("null", "nested"):
                continue
            _update_candidates(_unique_strings(arr, kind, st, na_set), st)
    return {c: _finalize(c, stats[c]) for c in batch}


def _date_blockers(el):
    for fmt, rx in DATE_FMTS.items():
        ex = el.get(f"date|{fmt}") or []
        if any(re.fullmatch(rx, str(x)) for x in ex):
            return ex
    return el.get("date|%Y-%m-%d") or el.get("date|%Y%m%d")


def _flat(v):
    return "" if v is None else str(v)


def infer(file_name, n_jobs=10, cols_per_task=16, limit_files=None, sample_files=None):
    t0 = time.time()
    files = list_files(file_name)
    assert files, f"没有找到 {file_name} 的 parquet 文件"
    files = _subset(files, limit_files, sample_files)
    col_order, file_cols = scan_columns(files, n_jobs)
    plans = _plans(col_order, files, file_cols, cols_per_task)
    print(
        f'[{file_name}] {len(files)} files ({files[0].split("/")[-1]} ~ {files[-1].split("/")[-1]}), '
        f"{len(col_order)} columns, {len(plans)} tasks"
    )

    results = Parallel(n_jobs=n_jobs)(
        delayed(_infer_task)(b, p, NA_TOKENS) for b, p in tqdm(plans)
    )
    specs = {}
    for r in results:
        specs.update(r)
    schema = dict(
        file_name=file_name,
        created_at=pd.Timestamp.now().isoformat(timespec="seconds"),
        n_files=len(files),
        first_file=files[0],
        last_file=files[-1],
        na_tokens=NA_TOKENS,
        columns={c: specs[c] for c in col_order},
    )
    save_json(schema, schema_path(file_name, "inferred"))

    rows = []
    for c, s in schema["columns"].items():
        el = s.get("eliminated", {})
        rows.append(
            dict(
                col=c,
                type=s["type"],
                fmt=_flat(s.get("fmt")),
                decided_by=s["decided_by"],
                note=s["note"],
                null_ratio=s["null_ratio"],
                n_na_token=s["n_na_token"],
                n_num_uniq=s.get("n_num_uniq", ""),
                nonnum_values=_flat(s.get("nonnum_values")),
                na_token_counts=_flat(s.get("na_token_counts") or ""),
                n_files=s["n_files"],
                n_unique=s["n_unique"],
                min=_flat(s.get("min")),
                max=_flat(s.get("max")),
                arrow_types="|".join(s["arrow_types"]),
                enum_preview=_flat((s.get("enum") or [])[:20]) if s.get("enum") else "",
                why_not_int=_flat(el.get("int")),
                why_not_float=_flat(el.get("float")),
                why_not_date=_flat(_date_blockers(el)),
            )
        )
    rep = pd.DataFrame(rows)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    rep_path = REPORT_DIR / f"{file_name}_infer.csv"
    rep.to_csv(rep_path, index=False, encoding="utf-8-sig")
    print(rep["type"].value_counts().to_string())
    flagged = rep[rep["note"] != ""]
    if len(flagged):
        print(f"需要人工确认的列 {len(flagged)} 个：")
        print(flagged[["col", "type", "note"]].to_string(index=False))
    print(
        f'schema -> {schema_path(file_name, "inferred")}\nreport -> {rep_path}\n用时 {time.time() - t0:.0f}s'
    )
    return schema


# ============================== 全量校验 ==============================
def _verify_task(batch, plan, specs, na_tokens):
    res = {
        c: dict(n_bad=0, bad_examples=[], warn=Counter()) for c in batch if c in specs
    }
    for f, cols in plan:
        cols = [c for c in cols if c in specs]
        if not cols:
            continue
        df = read_parquet(f, columns=cols)
        for c in cols:
            out, bad, oob = cast_series(df[c], specs[c], na_tokens)
            r = res[c]
            if oob.any():
                r["warn"]["date_out_of_range_to_NaT"] += int(oob.sum())
            if bad.any():
                r["n_bad"] += int(bad.sum())
                if len(r["bad_examples"]) < EXAMPLES_MAX:
                    r["bad_examples"] += [
                        str(x) for x in df[c][bad].unique()[:EXAMPLES_MAX]
                    ]
            for w in check_values(out, specs[c], c):
                r["warn"][w["kind"]] += w["n"]
    return res


def verify(
    file_name,
    n_jobs=10,
    cols_per_task=16,
    limit_files=None,
    sample_files=None,
    start_date=None,
):
    t0 = time.time()
    schema = load_schema(file_name)
    specs, na_tokens = schema["columns"], schema["na_tokens"]
    files = list_files(file_name)

    if start_date:
        files = _filter_by_start(files, schema, start_date)
        if not files:
            print(f"[{file_name}] 没有包含 {start_date} 之后数据的文件，无需校验。")
            return

    files = _subset(files, limit_files, sample_files)
    col_order, file_cols = scan_columns(files, n_jobs)
    not_in_schema = [c for c in col_order if c not in specs]
    plans = _plans(col_order, files, file_cols, cols_per_task)
    results = Parallel(n_jobs=n_jobs)(
        delayed(_verify_task)(b, p, specs, na_tokens) for b, p in tqdm(plans)
    )

    rows = []
    for r in results:
        for c, x in r.items():
            rows.append(
                dict(
                    col=c,
                    type=specs[c]["type"],
                    fmt=_flat(specs[c].get("fmt")),
                    decided_by=specs[c].get("decided_by"),
                    n_bad=x["n_bad"],
                    bad_examples=_flat(x["bad_examples"][:EXAMPLES_MAX]),
                    warnings=_flat(dict(x["warn"])) if x["warn"] else "",
                )
            )
    rows += [dict(col=c, type="NOT_IN_SCHEMA") for c in not_in_schema]
    rep = pd.DataFrame(rows)
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    rep_path = REPORT_DIR / f"{file_name}_verify.csv"
    rep.to_csv(rep_path, index=False, encoding="utf-8-sig")

    bad = rep[(rep["n_bad"].fillna(0) > 0) | (rep["type"] == "NOT_IN_SCHEMA")]
    if len(bad):
        print(f"[{file_name}] 校验未通过 {len(bad)} 列：")
        print(bad[["col", "type", "n_bad", "bad_examples"]].to_string(index=False))
    else:
        print(
            f"[{file_name}] 全部 {len(rep)} 列在 {len(files)} 个文件上均可按 schema 转换"
        )
    print(f"report -> {rep_path}\n用时 {time.time() - t0:.0f}s")
    return rep


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["infer", "verify"])
    ap.add_argument("file_name", help="数据集名，或 'all'")
    ap.add_argument("--n-jobs", type=int, default=10)
    ap.add_argument("--cols-per-task", type=int, default=16)
    ap.add_argument(
        "--limit-files", type=int, default=None, help="只用最早的 N 个文件（试跑用）"
    )
    ap.add_argument(
        "--sample-files",
        type=int,
        default=None,
        help="在整段历史上等间隔抽 N 个文件（试跑用）",
    )
    ap.add_argument(
        "--start", help="verify 用：只校验 DL_SNAPSHOT_DATE >= start 的数据"
    )
    a = ap.parse_args()
    names = list(DATASETS) if a.file_name == "all" else [a.file_name]

    for name in names:
        if a.cmd == "infer":
            infer(
                name,
                n_jobs=a.n_jobs,
                cols_per_task=a.cols_per_task,
                limit_files=a.limit_files,
                sample_files=a.sample_files,
            )
        else:
            verify(
                name,
                n_jobs=a.n_jobs,
                cols_per_task=a.cols_per_task,
                limit_files=a.limit_files,
                sample_files=a.sample_files,
                start_date=a.start,
            )
