"""
BBG 预处理公共模块：配置 + 类型系统。

bbg_schema.py（推断/校验字段类型）和 bbg_clean.py（清洗入库）都从这里取：
  - 候选类型的判定规则（推断时用）
  - cast_series：按 schema 强制转换（推断后的校验、入库时用）
  - check_values：新数据取值是否落在历史范围内
  - to_storage：落 xdata 前的格式约定
同一套规则，保证"推断时认为能转"和"入库时真的这么转"是一回事。
"""
import json
import logging
import re
import sys
import time
from concurrent.futures import FIRST_COMPLETED, wait
from datetime import date, datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ============================== 路径 ==============================
PATH_DATA_ROOT = Path('/mnt/win-g12x/Research/BBG')          # Bloomberg 原始 parquet（只读）
XDATA_ROOT = 'stock_1d/xdata_test/vendor_data'                # 本仓库（vendor_data）在 xdata 的根目录，与仓库同名
PATH_XDATA = f'{XDATA_ROOT}/bbg'                                  # Bloomberg 入库到 {PATH_XDATA}/{数据集名}
_HERE = Path(__file__).resolve().parent
SCHEMA_DIR = _HERE / 'schema'          # schema/inferred/*.json（脚本生成） + schema/overrides/*.json（人工修正）
REPORT_DIR = _HERE / 'reports'         # 推断/校验/重复诊断的 csv 报告
LOG_DIR = _HERE / 'logs'               # 入库日志 jsonl

# ============================== 数据集 ==============================
# key: 一行数据的业务主键（第一个必须是 DL_SNAPSHOT_DATE，入库后改名为 date）
# dup_priority: 主键冲突时选哪一行进主表，按顺序比较、越大越优先（见 bbg_clean.SCORERS）
_KEY_ID = ['DL_SNAPSHOT_DATE', 'ID_BB_UNIQUE']
_KEY_CA = _KEY_ID + ['ACTION_ID', 'CP_AMENDMENT_DATE']
DATASETS = {
    'equityNamrV2': dict(key=_KEY_ID, dup_priority=['n_nonnull', 'row_order']),
    'equityNamrCins': dict(key=_KEY_ID, dup_priority=['n_nonnull', 'row_order']),
    'equityNamrPricing': dict(key=_KEY_ID + ['EXCH_CODE'],
                              dup_priority=['has_px_last', 'n_nonnull', 'row_order']),
    'equityNamrCorporateActionsV2': dict(key=_KEY_CA, dup_priority=['comp_match', 'n_nonnull', 'row_order']),
    'effDtEquityNamrCorporateActionsCorporateActions': dict(
        key=_KEY_CA, dup_priority=['comp_match', 'n_nonnull', 'row_order']),
    'premarketEquityNamrCorporateActionsV2CorporateActions': dict(
        key=_KEY_CA, dup_priority=['comp_match', 'n_nonnull', 'row_order']),
}
DATE_COL = 'DL_SNAPSHOT_DATE'

# 判断"完全重复行"时忽略的列。
# COLUMN0 不是行号：实测（2026-11-11）取值是 'AAPL    US Equity' 这类 Bloomberg 请求标识，属于原始信息，不忽略。
# 同一证券被不同请求代码取到时，这些行会作为主键冲突进入 __dup 表，主表选一行。
META_COLS = []

# 视为缺失的字符串（所有列通用）。2026-11-11 用前 30 个文件实测：只出现了 'N.S.'（4 列 SEDOL 相关，整列）和 'None'。
# 'None' 只出现在 DVD_FREQ，很可能是真实取值（不派息），所以不当缺失；'nan'/'NULL' 等转换残留没有出现，不列入。
# 若全量数据里还有其它占位符，会在 bbg_report.py strings 的 [A] 组里显示出来，确认后再加。
NA_TOKENS = ['', 'N.A.', 'N.S.']
# 另外：只由空白字符组成的字符串（如 SEDOL1_COUNTRY_ISO 里的 '  '，固定宽度补齐留下的）一律视为缺失

EXACT_INT_MAX = 2 ** 53   # int 落库为 float64，超过这个绝对值会丢精度

ENUM_MAX = 500        # 历史上不同取值 <= 这个数的 string/int 字段，记录完整取值集合，新数据出现新值会报警
EXAMPLES_MAX = 10     # 报告里每类问题最多列多少个样例值

# ============================== 类型规则 ==============================
BOOL_MAP = {'Y': True, 'N': False, 'TRUE': True, 'FALSE': False}
INT_RE = r'[+-]?\d{1,18}'                 # 18 位以内保证不溢出 int64
LEAD0_RE = r'[+-]?0\d'                    # 前导零（CUSIP 之类的代码），不能转数字，否则丢 0
LONG_DIGITS_RE = r'[+-]?\d{16,}'          # 超过 15 位有效数字，float64 会丢精度
DATE_FMTS = {
    '%Y%m%d': r'\d{8}',
    '%Y-%m-%d': r'\d{4}-\d{2}-\d{2}',
    '%m/%d/%Y': r'\d{1,2}/\d{1,2}/\d{4}',
    '%Y/%m/%d': r'\d{4}/\d{1,2}/\d{1,2}',
}
ISO_DT_RE = r'\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d{1,9})?)?(Z|[+-]\d{2}:?\d{2})?'
TZ_SUFFIX_RE = r'.*(Z|[+-]\d{2}:?\d{2})'
DATE_NAME_RE = re.compile(r'(^|_)(DT|DATE)(_|$)')   # 整数和 %Y%m%d 都说得通时，字段名像日期才判为日期

PANDAS_GE_2 = int(pd.__version__.split('.')[0]) >= 2   # format='ISO8601' 需要 pandas>=2.0
# 推断时的优先级：越靠前越"严格"，所有值都满足的最严格类型胜出；都不满足就是 string
CANDIDATE_ORDER = (['bool', 'int', 'float']
                   + [f'date|{f}' for f in DATE_FMTS]
                   + (['datetime|ISO8601'] if PANDAS_GE_2 else []))

TYPES = ('bool', 'int', 'float', 'date', 'datetime', 'string', 'json', 'empty')


_cli = None


def get_cli():
    global _cli
    if _cli is None:
        from xdata.client import get_client
        _cli = get_client()
    return _cli


# ============================== 读文件 ==============================
def list_files(file_name):
    return sorted(str(p) for p in (PATH_DATA_ROOT / file_name).glob('*.parquet'))


def _types_mapper(t):
    # 整数/布尔列用 pandas 可空类型读，避免有缺失时被变成 float64（大整数会丢精度）
    if pa.types.is_integer(t):
        return pd.Int64Dtype()
    if pa.types.is_boolean(t):
        return pd.BooleanDtype()
    return None


def read_parquet(f, columns=None):
    return pq.ParquetFile(f).read(columns=columns).to_pandas(types_mapper=_types_mapper)


# ============================== schema 读写 ==============================
def schema_path(file_name, kind='inferred'):
    return SCHEMA_DIR / kind / f'{file_name}.json'


def save_json(obj, path):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as fh:
        json.dump(obj, fh, ensure_ascii=False, indent=1, default=str)


def load_schema(file_name):
    """推断结果 + 人工 overrides 合并。overrides 格式: {"columns": {"COL": {"type": "string"}, ...}}"""
    with open(schema_path(file_name, 'inferred'), encoding='utf-8') as fh:
        schema = json.load(fh)
    ov_path = schema_path(file_name, 'overrides')
    if ov_path.exists():
        with open(ov_path, encoding='utf-8') as fh:
            overrides = json.load(fh).get('columns', {})
        for col, ov in overrides.items():
            spec = schema['columns'].setdefault(col, {})
            if 'type' in ov and ov['type'] != spec.get('type'):
                # 类型被改了，旧类型下的取值范围/格式作废（除非 override 里一起给了）
                for k in ('fmt', 'enum', 'min', 'max'):
                    spec.pop(k, None)
            spec.update(ov)
            spec['decided_by'] = 'override'
            assert spec.get('type') in TYPES, f'{col}: override type 必须是 {TYPES} 之一'
    return schema


# ============================== 转换 ==============================
def _is_text(s):
    return s.dtype == object or isinstance(s.dtype, pd.StringDtype) or pd.api.types.is_string_dtype(s)


def _to_str(s):
    """非空值转 str，缺失统一为 np.nan，返回 object 列。"""
    out = pd.Series(np.nan, index=s.index, dtype=object)
    nn = s.notna()
    if nn.any():
        out[nn] = s[nn].astype(str).astype(object)
    return out


def normalize_na(s, na_tokens):
    if isinstance(s.dtype, pd.CategoricalDtype):
        s = s.astype(object)
    if _is_text(s):
        s = s.astype(object)
        na = s.isin(na_tokens)
        try:
            na |= s.str.fullmatch(r'\s*', na=False).astype(bool)   # 纯空白
        except AttributeError:      # 整列没有字符串（如 date32 读成的 datetime.date 对象）
            pass
        s = s.mask(na)
    return s


def _py(v):
    if isinstance(v, np.ndarray):
        return [_py(x) for x in v]
    if isinstance(v, dict):
        return {str(k): _py(x) for k, x in v.items()}
    if isinstance(v, (list, tuple)):
        return [_py(x) for x in v]
    if isinstance(v, np.generic):
        return v.item()
    if isinstance(v, (pd.Timestamp, datetime, date)):
        return v.isoformat()
    return v


def _json_cell(v):
    if v is None or (isinstance(v, float) and np.isnan(v)):
        return np.nan
    return json.dumps(_py(v), ensure_ascii=False, sort_keys=True, default=str)


def _fullmatch(txt, pattern):
    return txt.str.fullmatch(pattern, na=False).astype(bool)


PD_YEAR_MIN, PD_YEAR_MAX = 1678, 2261   # pandas datetime64[ns] 能表示 1677-09-21 ~ 2262-04-11，取内侧整年


def to_ns_range(parsed):
    """pandas>=3 能解析超出 ns 范围的日期（用 s/us 精度），pandas 2 直接给 NaT；这里统一成 NaT，两版本行为一致。"""
    if parsed.notna().any():
        parsed = parsed.mask((parsed < pd.Timestamp.min) | (parsed > pd.Timestamp.max))
    return parsed


def date_oob(txt, fmt, parsed):
    """解析失败、但格式正确且年份超出 pandas 范围的值（如 '2313-12-23'）。这类值按 NaT 处理并记录，不算转换失败。"""
    if not txt.notna().any():
        return pd.Series(False, index=txt.index)
    txt = txt.astype(object)
    if fmt == 'ISO8601':
        rx, year = ISO_DT_RE, txt.str[:4]
    else:
        rx = DATE_FMTS[fmt]
        year = txt.str[-4:] if fmt == '%m/%d/%Y' else txt.str[:4]
    y = pd.to_numeric(year, errors='coerce')
    return (parsed.isna() & txt.notna() & txt.str.fullmatch(rx, na=False)
            & ((y < PD_YEAR_MIN) | (y > PD_YEAR_MAX))).astype(bool)


def cast_series(s, spec, na_tokens):
    """
    按 spec 强制转换一列。不做 errors='coerce' 式的静默丢弃：
    返回 (转换结果, bad, oob)
      bad: 原来非空、但按该类型转不过去的位置 -> 调用方报错
      oob: 日期格式正确但超出 pandas 范围、被置为 NaT 的位置 -> 调用方记录
    """
    t = spec['type']
    if t == 'json':
        out = s.astype(object).map(_json_cell).astype('string')
        none = pd.Series(False, index=s.index)
        return out, none, none

    s = normalize_na(s, na_tokens)
    nn = s.notna()
    no_bad = pd.Series(False, index=s.index)

    if t in ('string', 'empty'):
        return _to_str(s).astype('string'), no_bad, no_bad

    if t == 'bool':
        if pd.api.types.is_bool_dtype(s):
            return s.astype('boolean'), no_bad, no_bad
        mapped = _to_str(s).str.upper().map(BOOL_MAP)
        bad = nn & mapped.isna()
        return mapped.astype('boolean'), bad, no_bad

    if t == 'int':
        if pd.api.types.is_integer_dtype(s):
            return s.astype('Int64'), no_bad, no_bad
        if pd.api.types.is_float_dtype(s):
            bad = nn & (s % 1 != 0)
            return s.where(~bad).astype('Int64'), bad, no_bad
        txt = _to_str(s)
        ok = _fullmatch(txt, INT_RE)
        bad = nn & ~ok
        arr = pa.array(txt.where(ok).to_numpy(dtype=object), type=pa.string(), from_pandas=True)
        vals = arr.cast(pa.int64()).to_pandas(types_mapper=_types_mapper)
        return pd.Series(vals.to_numpy(), index=s.index, dtype='Int64'), bad, no_bad

    if t == 'float':
        if pd.api.types.is_numeric_dtype(s) and not pd.api.types.is_bool_dtype(s):
            return s.astype('float64'), no_bad, no_bad
        num = pd.to_numeric(_to_str(s), errors='coerce').astype('float64')
        return num, nn & num.isna(), no_bad

    if t in ('date', 'datetime'):
        if pd.api.types.is_datetime64_any_dtype(s):
            if getattr(s.dt, 'tz', None) is not None:
                s = s.dt.tz_convert('UTC').dt.tz_localize(None)
            return s.astype('datetime64[ns]'), no_bad, no_bad
        txt = _to_str(s)
        fmt = spec['fmt']
        if fmt == 'ISO8601':
            # utc=True：带时区的统一转 UTC，不带时区的原样视为 UTC，最后去掉时区 -> naive UTC（与原 cols_utc 一致）
            out = to_ns_range(pd.to_datetime(txt, format='ISO8601', errors='coerce', utc=True).dt.tz_localize(None))
        else:
            ok = _fullmatch(txt, DATE_FMTS[fmt])
            out = to_ns_range(pd.to_datetime(txt.where(ok), format=fmt, errors='coerce'))
        out = out.astype('datetime64[ns]')
        oob = date_oob(txt, fmt, out)
        return out, nn & out.isna() & ~oob, oob

    raise ValueError(f'unknown type {t}')


# ============================== 取值校验 ==============================
def _examples(values):
    return [str(v) for v in list(values)[:EXAMPLES_MAX]]


def check_values(out, spec, col):
    """转换后的列与历史 schema 对比，返回 warning 列表（不改数据）。"""
    warns = []
    nn = out.notna()
    if not nn.any():
        return warns
    t = spec['type']
    vals = out[nn]

    if t == 'empty':
        warns.append(dict(col=col, kind='value_in_empty_col', n=int(nn.sum()), examples=_examples(vals.unique())))
        return warns

    enum = spec.get('enum')
    if enum is not None and t in ('int', 'string'):
        v = vals.astype('int64') if t == 'int' else vals
        unseen = pd.Index(v.unique()).difference(pd.Index(enum, dtype=v.dtype if t == 'int' else object))
        if len(unseen):
            warns.append(dict(col=col, kind='unseen_value', n=int(v.isin(unseen).sum()), examples=_examples(unseen)))

    lo, hi = spec.get('min'), spec.get('max')
    if lo is not None and hi is not None and t in ('int', 'float', 'date', 'datetime'):
        if t in ('date', 'datetime'):
            lo, hi, v = pd.Timestamp(lo), pd.Timestamp(hi), vals
        else:
            v = vals.astype('float64')
        for kind, m in (('below_hist_min', v < lo), ('above_hist_max', v > hi)):
            if m.any():
                warns.append(dict(col=col, kind=kind, n=int(m.sum()), hist=str(lo if kind.startswith('below') else hi),
                                  examples=_examples(v[m].unique())))
    return warns


# ============================== 落库格式 ==============================
def to_storage(out, spec, col=''):
    """
    内存类型 -> xdata 能接受的类型（2026-11-11 实测：xdata 不接受 Int64 / boolean / pandas string，
    也不接受"object 里混 int 和 NaN"；接受 float64 / datetime64[ns] / 纯字符串+NaN 的 object / 无缺失的 int64、bool）。
      int    -> float64（|值| <= 2^53 时无损；超出直接报错，不静默丢精度）
      bool   -> float64（1.0 / 0.0 / NaN）
      string / json / empty -> object（str + np.nan）
    读回时用 from_storage（或 bbg_read.read_bbg）按 schema 还原成 Int64 / boolean / string。
    """
    t = spec['type']
    if t == 'int':
        x = out.astype('Int64')
        if x.notna().any() and int(x.abs().max()) > EXACT_INT_MAX:
            raise ValueError(f'{col}: 整数超过 2^53，存成 float64 会丢精度，请在 overrides 里把该列改为 string')
        return x.astype('float64')
    if t == 'bool':
        return out.astype('boolean').astype('Float64').astype('float64')
    if t == 'float':
        return out.astype('float64')
    if t in ('date', 'datetime'):
        return out.astype('datetime64[ns]')
    return _to_str(out)


def from_storage(x, spec):
    """xdata 读回的一列 -> 明确的内存类型（与 cast_series 的输出一致）。"""
    t = spec['type']
    if t == 'int':
        return x.astype('Int64')          # 非整数值会直接报错
    if t == 'bool':
        return x.astype('boolean')
    if t == 'float':
        return x.astype('float64')
    if t in ('date', 'datetime'):
        return x.astype('datetime64[ns]')
    return _to_str(x).astype('string')


# 入库时附加的列（不在原始数据里）
EXTRA_SPECS = {'DUP_N': dict(type='int'), 'DUP_SEQ': dict(type='int'), 'DUP_CHOSEN': dict(type='bool'),
               'NOKEY_SEQ': dict(type='int')}


# ============================== 日志与并行（README 要求 14，2026-11-11） ==============================
def setup_logger(file_name, cmd):
    """
    同时输出到终端和 logs/{file_name}_{cmd}_{时间}.log，每行带时间戳。
    返回 (logger, 时间戳字符串)；同一次运行的 jsonl 日志用同一个时间戳，方便对照。
    只在主进程里用；worker 不写日志，把用时/内存等统计返回给主进程记录。
    """
    ts = f'{pd.Timestamp.now():%Y%m%d_%H%M%S}'
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    log = logging.getLogger(f'bbg.{file_name}.{cmd}.{ts}')
    log.setLevel(logging.INFO)
    log.propagate = False
    fmt = logging.Formatter('%(asctime)s %(levelname)s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
    path = LOG_DIR / f'{file_name}_{cmd}_{ts}.log'
    for h in (logging.StreamHandler(sys.stdout), logging.FileHandler(path, encoding='utf-8')):
        h.setFormatter(fmt)
        log.addHandler(h)
    log.info(f'日志文件 {path}')
    return log, ts


def _proc_status_gb(key):
    """读 /proc/self/status 里的 VmHWM（进程内存峰值）/ VmRSS（当前内存），单位 GB；读不到返回 nan。"""
    try:
        with open('/proc/self/status') as fh:
            for line in fh:
                if line.startswith(key + ':'):
                    return int(line.split()[1]) / 2 ** 20
    except OSError:
        pass
    return float('nan')


def rss_gb():
    return _proc_status_gb('VmRSS')


def _reset_peak():
    """把本进程的内存峰值（VmHWM）清零，这样任务结束时读到的就是"这个任务"的峰值。
    Linux 4.0+ 支持向 /proc/self/clear_refs 写 5；不支持时返回 False，此时读到的是 worker 进程启动以来的峰值。"""
    try:
        with open('/proc/self/clear_refs', 'w') as fh:
            fh.write('5')
        return True
    except OSError:
        return False


def _timed_call(fn, args):
    """在 worker 里执行一个任务，附带用时和内存峰值。"""
    scope = 'task' if _reset_peak() else 'process'
    t0 = time.time()
    res = fn(*args)
    return res, dict(sec=time.time() - t0, peak_gb=_proc_status_gb('VmHWM'), peak_scope=scope)


def _fmt_sec(s):
    s = int(s)
    return f'{s // 3600}h{s % 3600 // 60:02d}m' if s >= 3600 else f'{s // 60}m{s % 60:02d}s'


def run_parallel(fn, arg_list, n_jobs, log, desc, ordered=False, max_buffer=10, describe=None):
    """
    并行执行 fn(*args)（args 取自 arg_list），用 joblib 自带的 loky 进程池（与之前 joblib.Parallel 同一后端）。
    每完成一个任务记一行日志：进度按"已完成"计，附该任务用时、内存峰值、预计剩余时间。
      ordered=False：按完成顺序 yield (i, 结果)
      ordered=True ：按 arg_list 顺序 yield（写库要按日期顺序）。哪组先算完先收下，轮到它再交出去；
                     "正在算的 + 已算完还没轮到的"合计达到 n_jobs + max_buffer 个就暂停派发新任务，控制主进程内存
                     （某一组特别慢时，其它组算完只能在主进程里排队等它）。
                     （原来的做法是每批 --batch 组一起跑、等整批最慢的那组算完再开下一批，并发最多只有 batch 个。）
    describe(结果) -> str：可选，日志里对该任务结果的一句话描述（文件名、行数等）。
    出错时取消剩下的任务、关掉 worker，再把异常抛出来。
    """
    from joblib.externals.loky import get_reusable_executor
    n = len(arg_list)
    log.info(f'[{desc}] 开始：共 {n} 个任务，并发 {n_jobs}' + (f'，按顺序交付，正在算+待交付合计最多 {n_jobs + max_buffer} 组' if ordered else ''))
    ex = get_reusable_executor(max_workers=n_jobs)
    t0, pending, buf = time.time(), {}, {}
    nxt_submit = nxt_yield = n_done = 0
    try:
        while (nxt_yield if ordered else n_done) < n:
            while (nxt_submit < n and len(pending) < n_jobs
                   and (not ordered or len(pending) + len(buf) < n_jobs + max_buffer)):
                pending[ex.submit(_timed_call, fn, arg_list[nxt_submit])] = nxt_submit
                nxt_submit += 1
            done, _ = wait(list(pending), return_when=FIRST_COMPLETED)
            for fut in done:
                i = pending.pop(fut)
                try:
                    res, st = fut.result()
                except BaseException:
                    log.exception(f'[{desc}] 任务 #{i} 出错')
                    raise
                n_done += 1
                if ordered:
                    buf[i] = res
                el = time.time() - t0
                info = describe(res) if describe else ''
                log.info(f'[{desc}] 完成 {n_done}/{n}（#{i}）{info} | 用时 {st["sec"]:.0f}s，'
                         f'内存峰值 {st["peak_gb"]:.1f} GB({st["peak_scope"]}) | '
                         f'已用 {_fmt_sec(el)}，预计剩余 {_fmt_sec(el / n_done * (n - n_done))}'
                         + (f' | 已算完待写入 {len(buf)}' if ordered else ''))
                if not ordered:
                    yield i, res
            if ordered:
                while nxt_yield in buf:
                    yield nxt_yield, buf.pop(nxt_yield)
                    nxt_yield += 1
    except BaseException:
        for f in pending:
            f.cancel()
        ex.shutdown(wait=False, kill_workers=True)
        raise
    log.info(f'[{desc}] 全部完成，用时 {_fmt_sec(time.time() - t0)}')
