"""
下游读取入口：从 xdata 读 BBG 表，并按 schema 还原成明确的 dtype
（float64 / Int64 / boolean / datetime64[ns] / string），不出现 object。

    from bbg_read import read_bbg
    d   = read_bbg('equityNamrV2', start='2024-01-01', end='2024-06-30')
    dup = read_bbg('equityNamrV2', table='__dup')
其余参数原样传给 cli.read。
"""
import pandas as pd

from bbg_common import DATE_COL, EXTRA_SPECS, PATH_XDATA, from_storage, get_cli, load_schema


def restore_dtypes(d, schema):
    specs = {**EXTRA_SPECS, **schema['columns']}
    d = d.copy()
    for c in d.columns:
        if c in specs:
            d[c] = from_storage(d[c], specs[c])
    levels = []
    for i, name in enumerate(d.index.names):
        lv = d.index.get_level_values(i)
        spec = specs.get(DATE_COL if name == 'date' else name)
        levels.append(pd.Index(from_storage(lv.to_series(index=range(len(lv))), spec), name=name) if spec else lv)
    d.index = pd.MultiIndex.from_arrays(levels) if len(levels) > 1 else levels[0]
    return d


def read_bbg(file_name, table='', **kw):
    schema = load_schema(file_name)
    return restore_dtypes(get_cli().read(f'{PATH_XDATA}/{file_name}{table}', **kw), schema)
