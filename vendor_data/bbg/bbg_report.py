"""
把 reports/{name}_infer.csv 里需要人工看的部分压缩打印，方便截图。每次只打印一段：

  python bbg_report.py equityNamrV2 types      # 1. 各类型多少列 + 需要人工确认的列
  python bbg_report.py equityNamrV2 strings    # 2. 判成 string 的列，以及挡住它变成数字/日期的样例值
  python bbg_report.py equityNamrV2 na         # 3. 各占位符（N.A./N.S. 等）的分布
  python bbg_report.py equityNamrV2 typed      # 4. 判成 bool/int/float/日期 的列及取值范围
  python bbg_report.py equityNamrV2 col PX_LAST   # 单独看某一列的完整推断结果
"""
import ast
import json
import re
import sys

import pandas as pd

from bbg_common import REPORT_DIR, schema_path

pd.set_option('display.width', 200)
pd.set_option('display.max_colwidth', 60)
pd.set_option('display.max_rows', 200)


def _lst(x):
    try:
        return ast.literal_eval(x) if isinstance(x, str) and x else []
    except (ValueError, SyntaxError):
        return []


def _short(vals, n=4, w=14):
    return ', '.join(str(v)[:w] for v in vals[:n])


def types(rep):
    print(rep['type'].value_counts().to_string(), '\n')
    e = rep[rep['type'] == 'empty']
    if len(e):
        print(f'全空列 {len(e)} 个：' + ', '.join(e['col']) + '\n')
    f = rep[(rep['note'].fillna('') != '') & (rep['type'] != 'empty')]
    print(f'需要人工确认 {len(f)} 列：')
    if len(f):
        print(f[['col', 'type', 'fmt', 'note']].fillna('').to_string(index=False))


def strings(rep):
    s = rep[rep['type'] == 'string'].copy()
    s['n_num'] = pd.to_numeric(s['n_num_uniq'], errors='coerce').fillna(0).astype(int)
    s['nonnum'] = s['nonnum_values'].map(_lst)
    s['date_block'] = s['why_not_date'].map(_lst)
    print(f'string 列共 {len(s)} 个\n')
    # A：大部分不同取值是数字，只有少数几种非数字值 -> 多半是占位符，加进 NA_TOKENS 后能变成数字列
    a = s[(s['n_num'] > 0) & s['nonnum'].map(lambda v: 0 < len(v) <= 5)]
    print(f'[A] 几乎全是数字，只被少数非数字值挡住 {len(a)} 列：')
    if len(a):
        print(a.assign(nonnum=a['nonnum'].map(lambda v: _short([repr(x) for x in v], 5, 18)))
              [['col', 'null_ratio', 'n_num', 'nonnum']].to_string(index=False))
    # B：没有占位符问题，但挡住的值本身像数字/日期 -> 格式问题（千分位、其它日期格式、超长数字、前导零代码）
    rest = s.drop(a.index)
    b = rest[rest['why_not_float'].map(_lst).map(
        lambda v: bool(v) and all(re.fullmatch(r'[\d.+\-eE ,/:]+', str(x)) and re.search(r'\d', str(x)) for x in v))]
    print(f'\n[B] 挡住的样例长得像数字/日期（格式问题，或前导零/超长代码）{len(b)} 列：')
    if len(b):
        print(b.assign(float_blk=b['why_not_float'].map(_lst).map(_short), date_blk=b['date_block'].map(_short))
              [['col', 'n_unique', 'float_blk', 'date_blk']].to_string(index=False))
    c = rest.drop(b.index)
    print(f'\n[C] 其余（大概率本来就是文本）{len(c)} 列，列名(不同取值数)：')
    print(', '.join(f'{x.col}({x.n_unique})' for x in c.itertuples()))


def na(rep):
    d = rep[['col', 'type', 'na_token_counts']].copy()
    d['cnt'] = d['na_token_counts'].map(lambda x: _lst(x) if isinstance(x, str) and x.startswith('{') else {})
    d = d[d['cnt'].map(bool)]
    if d.empty:
        print('没有任何列出现占位符')
        return
    long = pd.DataFrame([dict(col=r.col, token=k, n=v) for r in d.itertuples() for k, v in r.cnt.items()])
    print('各占位符：出现在多少列、总次数')
    long['token'] = long['token'].map(repr)
    print(long.groupby('token')['n'].agg(n_cols='size', total='sum').to_string(), '\n')
    rows = rep.set_index('col')
    d['null_ratio'] = d['col'].map(rows['null_ratio'])
    d['tokens'] = d['cnt'].map(lambda c: ' '.join(f'{k!r}:{v}' for k, v in sorted(c.items(), key=lambda kv: -kv[1])))
    d = d.sort_values('null_ratio', ascending=False)
    print(f'出现占位符的列 {len(d)} 个（按缺失率从高到低，最多 60 行）：')
    print(d[['col', 'type', 'null_ratio', 'tokens']].head(60).to_string(index=False))


def typed(rep):
    t = rep[~rep['type'].isin(['string', 'empty'])].copy()
    t['range'] = t['min'].str[:19] + ' ~ ' + t['max'].str[:19]
    t['range'] = t['range'].where(t['min'] != '', t['enum_preview'].str[:50])
    for ty in ['bool', 'int', 'float', 'date', 'datetime', 'json']:
        x = t[t['type'] == ty]
        if len(x):
            print(f'[{ty}] {len(x)} 列')
            print(x[['col', 'fmt', 'null_ratio', 'n_unique', 'range']].to_string(index=False), '\n')


def col(rep, name):
    r = rep[rep['col'] == name]
    if not len(r):
        print(f'没有列 {name}')
        return
    print(r.drop(columns=['why_not_int', 'why_not_float', 'why_not_date']).T.to_string())
    # 从 schema JSON 读完整的淘汰原因：每个候选类型/格式分别被哪些值挡住
    spec = json.load(open(schema_path(sys.argv[1], 'inferred'), encoding='utf-8'))['columns'][name]
    print('\n各候选类型被挡住的样例值：')
    for cand, ex in spec.get('eliminated', {}).items():
        print(f'  {cand:20s} {_short([repr(x) for x in ex], 6, 24)}')


if __name__ == '__main__':
    name, what = sys.argv[1], sys.argv[2]
    rep = pd.read_csv(REPORT_DIR / f'{name}_infer.csv', dtype=str, keep_default_na=False)
    rep['null_ratio'] = pd.to_numeric(rep['null_ratio']).round(4)
    {'types': types, 'strings': strings, 'na': na, 'typed': typed}.get(what, lambda r: col(r, sys.argv[3]))(rep)
