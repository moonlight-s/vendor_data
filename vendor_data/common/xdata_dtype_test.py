"""
xdata dtype 测试 第 2 轮：全空列 & append 时类型是否必须一致。
第 1 轮结论（2026-11-11）：
  可以：float64 / datetime64[ns] / object(纯 str + NaN) / 无缺失的 int64、bool
  不行：Int64 / boolean / pandas string / object(int + NaN)
只写测试路径，路径名与第 1 轮不重复。跑完把打印的表发回即可。
"""
import numpy as np
import pandas as pd
from xdata.client import get_client

cli = get_client()
# 注：2026-11-11 两轮实测是在旧路径 stock_1d/xdata_test/bbg/_dtype_test 下跑的；以后再测用新路径
BASE = 'stock_1d/xdata_test/vendor_data/_dtype_test'
D1, D2 = pd.to_datetime(['2024-01-02', '2024-01-03'])


def frame(date, values):
    idx = pd.MultiIndex.from_product([[date], ['EQ001', 'EQ002']], names=['date', 'ID_BB_UNIQUE'])
    values = np.asarray(values)
    if values.dtype.kind == 'M':
        values = values.astype('datetime64[ns]')
    return pd.DataFrame({'v': pd.Series(values, index=idx, dtype=values.dtype)}, index=idx)


STR = np.array(['a', 'b'], dtype=object)
NA_OBJ = np.array([np.nan, np.nan], dtype=object)
# 每个用例：先 write 第一块，再 append 第二块（第二块为 None 表示只 write）
CASES = {
    'r2_object_all_na':        (NA_OBJ, None),                                     # 全空字符串列
    'r2_str_then_all_na':      (STR, NA_OBJ),                                      # 某天该字段全空
    'r2_all_na_then_str':      (NA_OBJ, STR),                                      # 字段早期全空、后来有值
    'r2_int_as_float64':       (np.array([2.0 ** 53, -3.0]), None),                # int 存 float64 是否精确
    'r2_float_then_all_nan':   (np.array([1.5, 2.5]), np.array([np.nan, np.nan])),
    'r2_dt_then_all_nat':      (pd.to_datetime(['2020-01-01', '2021-01-01']).values,
                                np.array(['NaT', 'NaT'], dtype='datetime64[ns]')),
}


def _norm(s):
    return [None if pd.isna(v) else v for v in s.tolist()]


rows = []
for name, (a, b) in CASES.items():
    path = f'{BASE}/{name}'
    d1 = frame(D1, a)
    expected = d1 if b is None else pd.concat([d1, frame(D2, b)])
    row = dict(case=name, chunk1=str(d1['v'].dtype), chunk2='' if b is None else str(frame(D2, b)['v'].dtype))
    try:
        if not cli.exists(path):
            cli.register(path, 'daily', 'xx', is_public=False, desc='dtype test r2')
            cli.write(path, d1)
            if b is not None:
                cli.append(path, frame(D2, b))
        back = cli.read(path).sort_index()
        row['read_back'] = str(back['v'].dtype)
        row['same_values'] = _norm(back['v']) == _norm(expected['v'])
        row['back_values'] = _norm(back['v'])
    except Exception as e:
        row['error'] = f'{type(e).__name__}: {e}'[:300]
    rows.append(row)

pd.set_option('display.width', 250)
pd.set_option('display.max_colwidth', 120)
print(pd.DataFrame(rows).to_string(index=False))
