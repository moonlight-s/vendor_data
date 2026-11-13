"""
下游读取入口：从 xdata 读 BBG 表，并按 schema 还原成明确的 dtype
（float64 / Int64 / boolean / datetime64[ns] / string），不出现 object。

    from bbg_read import read_bbg
    d   = read_bbg('equityNamrV2', start='2024-01-01', end='2024-06-30')
    dup = read_bbg('equityNamrV2', table='__dup')
其余参数原样传给 cli.read。
"""

import pandas as pd

from bbg_common import (
    DATE_COL,
    EXTRA_SPECS,
    PATH_XDATA,
    from_storage,
    get_cli,
    load_schema,
)


def restore_dtypes(d, schema):
    specs = {**EXTRA_SPECS, **schema["columns"]}
    d = d.copy()
    for c in d.columns:
        if c not in specs:
            raise KeyError(
                f"[bbg_read] 读取数据时发现 schema 中不存在的列: {c}，拒绝读取。"
            )
        d[c] = from_storage(d[c], specs[c])

    levels = []
    for i, name in enumerate(d.index.names):
        spec_name = DATE_COL if name == "date" else name
        if spec_name not in specs:
            raise KeyError(
                f"[bbg_read] 读取数据时发现 schema 中不存在的索引: {name}，拒绝读取。"
            )

        lv = d.index.get_level_values(i)
        levels.append(
            pd.Index(
                from_storage(lv.to_series(index=range(len(lv))), specs[spec_name]),
                name=name,
            )
        )
    d.index = pd.MultiIndex.from_arrays(levels) if len(levels) > 1 else levels[0]
    return d


def read_bbg(file_name, table="", **kw):
    schema = load_schema(file_name)
    return restore_dtypes(
        get_cli().read(f"{PATH_XDATA}/{file_name}{table}", **kw), schema
    )
